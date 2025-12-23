"""
Turbo Scanner v3.0
Ultra-fast arbitrage scanner with:
- WebSocket real-time prices (no REST delay)
- Parallel DEX requests
- Smart caching
- Instant signal detection
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Literal, Optional, Dict, List
from collections import defaultdict

from config import (
    MIN_SPREAD_PERCENT, MAX_SPREAD_PERCENT, MIN_LIQUIDITY_USD, 
    MIN_VOLUME_24H_USD, MIN_FDV_USD, DEXSCREENER_BATCH_SIZE, 
    TOKEN_BLACKLIST, TOTAL_FEES_PERCENT, MAJOR_TOKENS, MIN_TXNS_24H
)
from mexc_client import MEXCClient
from mexc_ws import get_ws_client, MEXCWebSocket
from dexscreener_client import DexScreenerClient, get_chain_display_name
from database import save_signal, check_signal_exists, save_price_history
from pair_manager import PairManager
from token_validator import get_validator

logger = logging.getLogger(__name__)


@dataclass
class ArbitrageSignal:
    token: str
    direction: Literal["LONG", "SHORT"]
    spread_percent: float
    net_profit: float
    mexc_price: float
    dex_price: float
    dex_url: str
    chain: str
    liquidity_usd: float = 0
    volume_24h: float = 0
    order_book_depth: float = 0
    detected_at: float = field(default_factory=time.time)


class TurboScanner:
    """
    Ultra-fast scanner using WebSocket + parallel requests.
    Detects opportunities in milliseconds, not seconds.
    """
    
    def __init__(self, mexc_client: MEXCClient, dexscreener_client: DexScreenerClient):
        self.mexc = mexc_client
        self.dexscreener = dexscreener_client
        self.pair_manager = PairManager(dexscreener_client)
        self.validator = get_validator()
        self.ws = get_ws_client()
        
        # Caching for speed
        self._dex_cache: Dict[str, dict] = {}  # {pair_addr: pair_data}
        self._cache_time: Dict[str, float] = {}
        self._cache_ttl = 5  # 5 second cache
        
        # Signal cooldowns (prevent spam)
        self._signal_cooldowns: Dict[str, float] = {}
        self._cooldown_sec = 300  # 5 min cooldown per token+direction
        
        self._discovery_task = None
        self._ws_started = False
    
    async def start_ws(self):
        """Initialize WebSocket connection"""
        if not self._ws_started:
            await self.ws.start()
            self._ws_started = True
            # Wait for initial prices
            await asyncio.sleep(2)
            logger.info(f"📊 WebSocket loaded {len(self.ws.prices)} prices")
    
    async def start_discovery(self, tickers: dict[str, float]):
        """Start background pair discovery"""
        if self._discovery_task is None or self._discovery_task.done():
            self._discovery_task = asyncio.create_task(
                self.pair_manager.discover_pairs(tickers)
            )
    
    def _is_on_cooldown(self, token: str, direction: str) -> bool:
        """Check if we recently sent this signal"""
        key = f"{token}_{direction}"
        last_time = self._signal_cooldowns.get(key, 0)
        return (time.time() - last_time) < self._cooldown_sec
    
    def _set_cooldown(self, token: str, direction: str):
        """Set cooldown for this signal"""
        key = f"{token}_{direction}"
        self._signal_cooldowns[key] = time.time()
    
    async def scan(self) -> List[ArbitrageSignal]:
        """
        TURBO SCAN - Ultra fast scanning
        Uses WebSocket prices + parallel DEX requests
        """
        signals = []
        start_time = time.time()
        
        # Ensure WebSocket is running
        await self.start_ws()
        
        # Get real-time prices from WebSocket (instant!)
        mexc_prices = self.ws.prices
        
        if not mexc_prices:
            # Fallback to REST if WS not ready
            tickers = await self.mexc.get_futures_tickers()
            mexc_prices = {t[0]: t[1] for t in tickers}
        
        if not mexc_prices:
            return []
        
        # Background discovery
        await self.start_discovery(mexc_prices)
        
        # Get batch candidates
        batches = self.pair_manager.get_batch_candidates()
        if not batches:
            return []
        
        # PARALLEL processing - scan all chains at once
        tasks = []
        for chain, addresses in batches.items():
            for i in range(0, len(addresses), DEXSCREENER_BATCH_SIZE):
                chunk = addresses[i:i + DEXSCREENER_BATCH_SIZE]
                tasks.append(self._scan_batch(chain, chunk, mexc_prices))
        
        # Execute all batches in parallel
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, list):
                    signals.extend(result)
        
        scan_time = (time.time() - start_time) * 1000
        if signals:
            logger.info(f"⚡ Scan completed in {scan_time:.0f}ms - Found {len(signals)} signals")
        
        return signals
    
    async def _scan_batch(
        self, 
        chain: str, 
        addresses: List[str],
        mexc_prices: Dict[str, float]
    ) -> List[ArbitrageSignal]:
        """Scan a single batch of addresses"""
        signals = []
        
        try:
            pairs = await self.dexscreener.get_pairs_by_addresses(chain, addresses)
            
            for pair in pairs:
                signal = await self._process_pair(pair, chain, mexc_prices)
                if signal:
                    signals.append(signal)
                    
        except Exception as e:
            logger.error(f"Batch scan error ({chain}): {e}")
        
        return signals
    
    async def _process_pair(
        self,
        pair: dict,
        chain: str,
        mexc_prices: Dict[str, float]
    ) -> Optional[ArbitrageSignal]:
        """Process single pair - optimized for speed"""
        symbol = pair.get("symbol", "")
        dex_price = pair.get("price_usd", 0)
        
        # Quick filters first (fast rejection)
        if not symbol or symbol in TOKEN_BLACKLIST:
            return None
        if symbol not in mexc_prices:
            return None
        
        mexc_price = mexc_prices[symbol]
        if mexc_price <= 0 or dex_price <= 0:
            return None
        
        # Calculate spread
        spread = ((dex_price - mexc_price) / mexc_price) * 100
        abs_spread = abs(spread)
        direction = "LONG" if spread > 0 else "SHORT"
        
        # Quick spread filter
        if abs_spread < MIN_SPREAD_PERCENT or abs_spread > MAX_SPREAD_PERCENT:
            return None
        
        # Cooldown check (prevent spam)
        if self._is_on_cooldown(symbol, direction):
            return None
        
        # SAVE PRICE HISTORY (throttled) - restoring charts
        # Save history for valid tokens even if spread is small, to build chart data
        now = time.time()
        last_save = self._cache_time.get(f"history_{symbol}", 0)
        if now - last_save > 60:  # Save once per minute
            await save_price_history(
                token=symbol,
                chain=chain,
                cex_price=mexc_price,
                dex_price=dex_price,
                spread_percent=spread
            )
            self._cache_time[f"history_{symbol}"] = now
        
        # Token validation
        is_valid, reason = self.validator.validate_token(
            symbol=symbol,
            chain=chain,
            dex_price=dex_price,
            mexc_price=mexc_price,
            spread_percent=abs_spread
        )
        if not is_valid:
            return None
        
        # Liquidity & Volume filters
        liquidity = pair.get("liquidity_usd", 0)
        volume_24h = pair.get("volume_24h", 0)
        
        if liquidity < MIN_LIQUIDITY_USD:
            return None
        if volume_24h < MIN_VOLUME_24H_USD:
            return None
        
        # Volume ratio filter
        if liquidity > 0 and (volume_24h / liquidity) < 0.05:
            return None
        
        # Net profit check
        net_profit = self.validator.calculate_net_profit(abs_spread)
        if net_profit < 3.0:  # Minimum 3% after fees
            return None
        
        # FDV check
        fdv = float(pair.get("fdv", 0) or 0)
        if fdv > 0 and fdv < MIN_FDV_USD:
            return None

        # Min Transactions Check (Anti-Bot/Wash Trading)
        txns = pair.get("txns", {}).get("h24", {})
        total_txns = txns.get("buys", 0) + txns.get("sells", 0)
        if total_txns < MIN_TXNS_24H:
            return None

        # Name/Symbol Spam Filter
        name_lower = pair.get("baseToken", {}).get("name", "").lower()
        if "test" in name_lower or "harry" in name_lower or "potter" in name_lower: # Common scam patterns
            return None
        
        # Wrapped Token Filter (only native allowed)
        if "wrapped" in name_lower and symbol not in MAJOR_TOKENS:
             if "sol" not in name_lower and "eth" not in name_lower: # Allow WBTC/WETH/WSOL context but filter rare wrapped
                 return None
        
        # Database duplicate check
        if await check_signal_exists(symbol, direction):
            return None
        
        # Get order book depth (async, non-blocking)
        order_book_depth = 0
        try:
            ob = await asyncio.wait_for(
                self.mexc.get_order_book_depth(symbol, 5000),
                timeout=1.0  # 1 second timeout
            )
            if ob:
                order_book_depth = ob.get("depth_usd", 0)
                if order_book_depth < 10_000:
                    return None
        except asyncio.TimeoutError:
            pass  # Skip order book check if too slow
        
        # Create signal
        signal = ArbitrageSignal(
            token=symbol,
            direction=direction,
            spread_percent=abs_spread,
            net_profit=net_profit,
            mexc_price=mexc_price,
            dex_price=dex_price,
            dex_url=pair.get("url", ""),
            chain=chain,
            liquidity_usd=liquidity,
            volume_24h=volume_24h,
            order_book_depth=order_book_depth
        )
        
        # Save to DB and set cooldown
        await self._save_signal(signal, pair)
        self._set_cooldown(symbol, direction)
        
        logger.info(
            f"🚀 SIGNAL: {direction} ${symbol} | "
            f"Net: +{net_profit:.1f}% | "
            f"Spread: {abs_spread:.1f}%"
        )
        
        return signal
    
    async def _save_signal(self, signal: ArbitrageSignal, pair: dict):
        """Save signal to database"""
        dep_status = self.mexc.get_cached_deposit_status(signal.token)
        
        await save_signal(
            token=signal.token,
            chain=signal.chain,
            direction=signal.direction,
            spread_percent=signal.spread_percent,
            dex_price=signal.dex_price,
            mexc_price=signal.mexc_price,
            dex_source="DexScreener",
            liquidity_usd=signal.liquidity_usd,
            volume_24h_usd=signal.volume_24h,
            deposit_enabled=dep_status.get("deposit_enabled", False),
            withdraw_enabled=dep_status.get("withdraw_enabled", False)
        )
        
        # Save price (optional for stats, but not needed for chart anymore)
        await save_price_history(
            token=signal.token,
            chain=signal.chain,
            cex_price=signal.mexc_price,
            dex_price=signal.dex_price,
            spread_percent=signal.spread_percent
        )


def format_turbo_signal(signal: ArbitrageSignal, token_stats: dict = None) -> str:
    """Format signal - MINIMALIST MODE"""
    chain = get_chain_display_name(signal.chain)
    
    if signal.direction == "LONG":
        header = f"🟢 <b>LONG #{signal.token}</b>"
        action = "SPOT Buy -> FUTURES Short"
    else:
        header = f"🔴 <b>SHORT #{signal.token}</b>"
        action = "SPOT Sell -> FUTURES Long"

    return (
        f"{header}\n"
        f"Strategy: {action}\n\n"
        f"💵 <b>Profit: +{signal.net_profit:.1f}%</b> (Gap: {signal.spread_percent:.1f}%)\n"
        f"📉 MEXC: ${signal.mexc_price}\n"
        f"📈 DEX: ${signal.dex_price}\n\n"
        f"💧 Liq: ${signal.liquidity_usd:,.0f}\n"
        f"🔗 {chain} | <a href='{signal.dex_url}'>DexScreener</a> | <a href='https://futures.mexc.com/exchange/{signal.token}_USDT'>MEXC</a>"
    )


# Alias for backward compatibility
ArbitrageScanner = TurboScanner
format_signal_message = format_turbo_signal
