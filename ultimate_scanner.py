"""
Ultimate Scanner v5.0 - The TOP MEXC-DEX Spread Hunter
Combines:
- New listings detector (50%+ spreads on fresh pairs)
- Pump/dump detector (catch rapid movements)
- Smart DEX price fetching
- Origin detection (where movement started)
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Literal, Optional, Dict, List
from collections import defaultdict

from config import (
    MIN_SPREAD_PERCENT, MAX_SPREAD_PERCENT, 
    MIN_LIQUIDITY_USD, MIN_VOLUME_24H_USD,
    TOKEN_BLACKLIST, TOTAL_FEES_PERCENT,
    TOKEN_BLACKLIST, TOTAL_FEES_PERCENT,
    MAJOR_TOKENS, MIN_TXNS_24H, DEXSCREENER_BATCH_SIZE
)
from mexc_client import MEXCClient
from mexc_ws import get_ws_client
from dexscreener_client import DexScreenerClient, get_chain_display_name
from database import save_signal, check_signal_exists, save_price_history
from listing_detector import get_listing_detector
from pump_detector import get_pump_detector
from pair_manager import PairManager
from token_validator import get_validator

logger = logging.getLogger(__name__)


@dataclass
class UltimateSignal:
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
    # Intelligence data
    quality_score: float = 5.0
    funding_cost: float = 0
    momentum_strength: float = 0
    entry_quality: float = 5.0
    convergence_time_est: float = 0
    # Origin detection
    origin: str = ""  # "CEX_PUMP", "DEX_DUMP", etc.
    mexc_change: float = 0
    dex_change: float = 0
    # Is new listing?
    is_new_listing: bool = False
    # Database ID
    id: Optional[int] = None


class UltimateScanner:
    """
    The ultimate MEXC-DEX spread hunter.
    Finds real arbitrage opportunities.
    """
    
    def __init__(self, mexc_client: MEXCClient, dexscreener_client: DexScreenerClient):
        self.mexc = mexc_client
        self.dexscreener = dexscreener_client
        self.ws = get_ws_client()
        self.pair_manager = PairManager(dexscreener_client)
        
        # Detectors
        self.listing_detector = get_listing_detector()
        self.pump_detector = get_pump_detector()
        self.validator = get_validator()
        
        # Background tasks
        self._discovery_task = None
        
        # Price tracking for origin detection
        self._mexc_history: Dict[str, List[tuple]] = defaultdict(list)  # {symbol: [(time, price), ...]}
        self._dex_history: Dict[str, List[tuple]] = defaultdict(list)
        
        # Signal cooldowns
        self._signal_cooldowns: Dict[str, float] = {}
        self._cooldown_sec = 300  # 5 min cooldown per token+direction
        
        self._ws_started = False
        self._scan_count = 0
    
    async def start_ws(self):
        """Initialize WebSocket connection"""
        if not self._ws_started:
            await self.ws.start()
            self._ws_started = True
            await asyncio.sleep(2)
            logger.info(f"📊 WebSocket loaded {len(self.ws.prices)} prices")
    
    def _is_on_cooldown(self, token: str, direction: str) -> bool:
        key = f"{token}_{direction}"
        last_time = self._signal_cooldowns.get(key, 0)
        return (time.time() - last_time) < self._cooldown_sec
    
    def _set_cooldown(self, token: str, direction: str):
        key = f"{token}_{direction}"
        self._signal_cooldowns[key] = time.time()
    
    def _record_prices(self, symbol: str, mexc_price: float, dex_price: float):
        """Record prices for origin detection"""
        now = time.time()
        
        self._mexc_history[symbol].append((now, mexc_price))
        self._dex_history[symbol].append((now, dex_price))
        
        # Keep only last 5 minutes
        cutoff = now - 300
        self._mexc_history[symbol] = [(t, p) for t, p in self._mexc_history[symbol] if t > cutoff]
        self._dex_history[symbol] = [(t, p) for t, p in self._dex_history[symbol] if t > cutoff]
    
    def _detect_origin(self, symbol: str, direction: str) -> tuple[str, float, float]:
        """
        Detect where the price movement originated.
        Returns (origin_str, mexc_change%, dex_change%)
        """
        mexc_hist = self._mexc_history.get(symbol, [])
        dex_hist = self._dex_history.get(symbol, [])
        
        if len(mexc_hist) < 2 or len(dex_hist) < 2:
            return "", 0, 0
        
        # Calculate changes over last 5 min
        mexc_old = mexc_hist[0][1]
        mexc_new = mexc_hist[-1][1]
        dex_old = dex_hist[0][1]
        dex_new = dex_hist[-1][1]
        
        if mexc_old <= 0 or dex_old <= 0:
            return "", 0, 0
        
        mexc_change = ((mexc_new - mexc_old) / mexc_old) * 100
        dex_change = ((dex_new - dex_old) / dex_old) * 100
        
        # Determine origin
        if direction == "SHORT":
            # MEXC higher than DEX
            if mexc_change > dex_change + 2:
                origin = "CEX (PUMP)"
            elif dex_change < mexc_change - 2:
                origin = "DEX (DUMP)"
            else:
                origin = "MIXED"
        else:  # LONG
            # DEX higher than MEXC
            if dex_change > mexc_change + 2:
                origin = "DEX (PUMP)"
            elif mexc_change < dex_change - 2:
                origin = "CEX (DUMP)"
            else:
                origin = "MIXED"
        
        return origin, mexc_change, dex_change
    
    async def scan(self) -> List[UltimateSignal]:
        """
        ULTIMATE SCAN - Find all spread opportunities
        """
        signals = []
        start_time = time.time()
        
        # Ensure WebSocket is running
        await self.start_ws()
        
        # Get real-time prices
        mexc_prices = self.ws.prices
        if not mexc_prices:
            tickers = await self.mexc.get_futures_tickers()
            mexc_prices = {t[0]: t[1] for t in tickers}
        
        if not mexc_prices:
            return []
        
        self._scan_count += 1
        
        # Step 1: Check for new listings
        new_listings = self.listing_detector.detect_new_listings(mexc_prices)
        if new_listings:
            logger.info(f"🆕 {len(new_listings)} new listings detected!")
        
        # Step 2: Record prices and detect pumps
        self.pump_detector.record_prices(mexc_prices)
        pump_events = self.pump_detector.detect_pumps(mexc_prices)
        if pump_events:
            logger.info(f"💥 {len(pump_events)} pump/dump events detected!")
        
        # Step 3: Start background discovery
        if self._discovery_task is None or self._discovery_task.done():
            self._discovery_task = asyncio.create_task(
                self.pair_manager.discover_pairs(mexc_prices)
            )
        
        # Step 4: Scan Priority Tokens (New Listings + Pumps)
        # These are scanned individually to ensure we get the absolute latest data
        priority_tokens = set()
        for listing in new_listings:
            priority_tokens.add(listing.symbol)
        for event in self.pump_detector.get_recent_events(max_age_sec=120):
            priority_tokens.add(event.symbol)
            
        for symbol in priority_tokens:
            if symbol in mexc_prices:
                try:
                    signal = await self._check_spread(
                        symbol, 
                        mexc_prices[symbol],
                        is_new_listing=(symbol in [l.symbol for l in new_listings])
                    )
                    if signal:
                        signals.append(signal)
                except Exception as e:
                    logger.debug(f"Error checking priority {symbol}: {e}")

        # Step 5: Batch Scan (The "Optimization")
        # Scan known pairs in batches to maximize throughput
        batches = self.pair_manager.get_batch_candidates()
        tasks = []
        
        for chain, addresses in batches.items():
            # Process in chunks
            for i in range(0, len(addresses), DEXSCREENER_BATCH_SIZE):
                chunk = addresses[i:i + DEXSCREENER_BATCH_SIZE]
                tasks.append(self._scan_batch(chain, chunk, mexc_prices))
        
        # Execute batch scans
        if tasks:
            # Limit concurrency to avoid overwhelming CPU/Network
            # Split tasks into chunks of 10 concurrent requests
            chunk_size = 10
            for i in range(0, len(tasks), chunk_size):
                batch_tasks = tasks[i:i + chunk_size]
                results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, list):
                        signals.extend(result)
        
        # Log periodically
        if self._scan_count % 30 == 0:
            logger.info(
                f"📊 Scan #{self._scan_count}: "
                f"checked {len(priority_tokens)} priority + {len(tasks)} batches, "
                f"found {len(signals)} signals"
            )
        
        scan_time = (time.time() - start_time) * 1000
        if signals:
            logger.info(f"⚡ Scan completed in {scan_time:.0f}ms - Found {len(signals)} signals")
        
        return signals

    async def _scan_batch(
        self, 
        chain: str, 
        addresses: List[str],
        mexc_prices: Dict[str, float]
    ) -> List[UltimateSignal]:
        """Scan a batch of addresses"""
        signals = []
        try:
            pairs = await self.dexscreener.get_pairs_by_addresses(chain, addresses)
            for pair in pairs:
                # We need to reconstruct the symbol check since we're coming from address
                symbol = pair.get("symbol")
                if not symbol or symbol not in mexc_prices:
                    continue
                    
                # Reuse _check_spread logic but with pre-fetched pair
                # We need to refactor _check_spread to accept an optional pair
                # Or just duplicate the logic here for speed (preferred for batching)
                mexc_price = mexc_prices[symbol]
                
                # Fast check spread
                dex_price = pair.get("price_usd", 0)
                if dex_price <= 0 or mexc_price <= 0:
                    continue
                    
                spread = ((dex_price - mexc_price) / mexc_price) * 100
                abs_spread = abs(spread)
                
                if abs_spread < MIN_SPREAD_PERCENT or abs_spread > MAX_SPREAD_PERCENT:
                    continue
                
                # If spread looks good, do full validation
                signal = await self._validate_and_create_signal(symbol, pair, mexc_price)
                if signal:
                    signals.append(signal)
                    
        except Exception as e:
            logger.debug(f"Batch scan error {chain}: {e}")
        return signals
    
    async def _check_spread(
        self, 
        symbol: str, 
        mexc_price: float,
        is_new_listing: bool = False
    ) -> Optional[UltimateSignal]:
        """Check single token for spread opportunity (fetches pair)"""
        
        # Get DEX data with reference price
        pair = await self.dexscreener.get_best_dex_price(
            symbol,
            min_liquidity=MIN_LIQUIDITY_USD,
            min_volume=MIN_VOLUME_24H_USD,
            reference_price=mexc_price
        )
        
        if not pair:
            return None
            
        return await self._validate_and_create_signal(
            symbol, pair, mexc_price, is_new_listing
        )

    async def _validate_and_create_signal(
        self, 
        symbol: str, 
        pair: dict,
        mexc_price: float,
        is_new_listing: bool = False
    ) -> Optional[UltimateSignal]:
        """Validate pair and create signal (shared logic)"""
        
        # Check Volume/Liquidity Ratio (Dead/Fake token check)
        # Real tokens usually have Volume > 10% of Liquidity.
        # Honeypots often have high liquidity but 0 volume.
        liq = pair.get("liquidity_usd", 0)
        vol = pair.get("volume_24h", 0)
        if liq > 0 and (vol / liq) < 0.02: # volume must be at least 2% of liquidity
             return None
        
        # Check transaction count (Activity Filter)
        txns = pair.get("txns", {}).get("h24", {})
        total_txns = txns.get("buys", 0) + txns.get("sells", 0)
        if total_txns < MIN_TXNS_24H:
            # logger.debug(f"Skip {symbol}: Low activity ({total_txns} txns < {MIN_TXNS_24H})")
            return None
        
        if not pair:
            return None
        
        dex_price = pair.get("price_usd", 0)
        if dex_price <= 0 or mexc_price <= 0:
            return None
        
        # Record for origin detection
        self._record_prices(symbol, mexc_price, dex_price)
        
        # Calculate spread
        spread = ((dex_price - mexc_price) / mexc_price) * 100
        abs_spread = abs(spread)
        direction = "LONG" if spread > 0 else "SHORT"
        
        # Check spread threshold (lower for new listings)
        min_spread = MIN_SPREAD_PERCENT / 2 if is_new_listing else MIN_SPREAD_PERCENT
        if abs_spread < min_spread:
            return None
        if abs_spread > MAX_SPREAD_PERCENT:
            return None
        
        # VALIDATE TOKEN (Anti-Scam/Fake Check)
        is_valid, reason = self.validator.validate_token(
            symbol, 
            chain, 
            dex_price, 
            mexc_price, 
            abs_spread,
            pair.get("token_address")
        )
        
        if not is_valid:
            logger.debug(f"Skip {symbol}: {reason}")
            return None
        
        # Check cooldown
        if self._is_on_cooldown(symbol, direction):
            return None
        
        # Check database
        if await check_signal_exists(symbol, direction):
            return None
        
        # Get additional data
        liquidity = pair.get("liquidity_usd", 0)
        volume_24h = pair.get("volume_24h", 0)
        chain = pair.get("chain", "unknown")
        dex_url = pair.get("url", "")
        
        # Calculate net profit
        net_profit = abs_spread - TOTAL_FEES_PERCENT
        if net_profit < 0.5:  # At least 0.5% profit
            return None
        
        # Detect origin
        origin, mexc_change, dex_change = self._detect_origin(symbol, direction)
        
        # Calculate quality score
        quality_score = min(10, abs_spread / 3)  # Higher spread = higher score
        
        # Create signal
        signal = UltimateSignal(
            token=symbol,
            direction=direction,
            spread_percent=abs_spread,
            net_profit=net_profit,
            mexc_price=mexc_price,
            dex_price=dex_price,
            dex_url=dex_url,
            chain=chain,
            liquidity_usd=liquidity,
            volume_24h=volume_24h,
            quality_score=round(quality_score, 1),
            origin=origin,
            mexc_change=mexc_change,
            dex_change=dex_change,
            is_new_listing=is_new_listing
        )
        
        # Save to database
        signal_id = await save_signal(
            token=symbol,
            chain=chain,
            direction=direction,
            spread_percent=abs_spread,
            dex_price=dex_price,
            mexc_price=mexc_price,
            dex_source="DexScreener",
            liquidity_usd=liquidity,
            volume_24h_usd=volume_24h,
            deposit_enabled=True,
            withdraw_enabled=True
        )
        
        signal.id = signal_id
        
        # Save price history
        await save_price_history(
            token=symbol,
            chain=chain,
            cex_price=mexc_price,
            dex_price=dex_price,
            spread_percent=spread
        )
        
        # Set cooldown
        self._set_cooldown(symbol, direction)
        
        listing_tag = " [NEW LISTING]" if is_new_listing else ""
        logger.info(
            f"🚀 SIGNAL: {direction} ${symbol}{listing_tag} | "
            f"Spread: {abs_spread:.1f}% | Net: +{net_profit:.1f}% | "
            f"Origin: {origin}"
        )
        
        return signal


def format_ultimate_signal(signal: UltimateSignal, token_stats: dict = None) -> str:
    """Format signal using the existing MMRdex style"""
    chain = get_chain_display_name(signal.chain)
    
    if signal.direction == "LONG":
        header = f"🟢 <b>LONG #{signal.token}</b> | +{signal.net_profit:.1f}%"
        desc = "MEXC Futures Opportunity"
    else:
        header = f"🔴 <b>SHORT #{signal.token}</b> | +{signal.net_profit:.1f}%"
        desc = "MEXC Futures Opportunity"
    
    # New listing badge
    if signal.is_new_listing:
        header = f"🆕 {header}"
    
    # Quality text
    if signal.quality_score >= 8:
        quality_str = "🔥 Excellent"
    elif signal.quality_score >= 6:
        quality_str = "✅ Good"
    elif signal.quality_score >= 4:
        quality_str = "⚠️ Moderate"
    else:
        quality_str = "❓ Low"
    
    # Origin info
    origin_info = ""
    if signal.origin:
        origin_info = f"\n💥 Origin: {signal.origin} [M: {signal.mexc_change:+.1f}% vs D: {signal.dex_change:+.1f}%]"
    
    # Convergence time estimation
    time_info = ""
    if signal.convergence_time_est > 0:
        if signal.convergence_time_est >= 3600:
            time_str = f"{signal.convergence_time_est / 3600:.1f}h"
        elif signal.convergence_time_est >= 60:
            time_str = f"{signal.convergence_time_est / 60:.0f}m"
        else:
            time_str = f"{signal.convergence_time_est:.0f}s"
        time_info = f" • ⏱️ ~{time_str}"
    
    # Funding info
    funding_info = ""
    if abs(signal.funding_cost) > 0.05:
        if signal.funding_cost > 0:
            funding_info = f" • 💸 -{signal.funding_cost:.2f}%"
        else:
            funding_info = f" • 💰 +{abs(signal.funding_cost):.2f}%"
    
    # Compact liquidity
    liq_str = f"${signal.liquidity_usd/1000:.0f}k" if signal.liquidity_usd > 1000 else f"${signal.liquidity_usd:.0f}"
    
    return (
        f"{header}\n"
        f"{desc}{origin_info}\n\n"
        f"<b>Gap: {signal.spread_percent:.1f}%</b>\n"
        f"MEXC: ${signal.mexc_price}\n"
        f"DEX: ${signal.dex_price}\n\n"
        f"Score: <b>{signal.quality_score}/10</b> ({quality_str})\n"
        f"💧 Liq: {liq_str}{time_info}{funding_info}\n"
        f"{chain} • <a href='{signal.dex_url}'>DexScreener</a> • <a href='https://futures.mexc.com/exchange/{signal.token}_USDT'>MEXC</a>"
    )


# Backward compatibility aliases
ArbitrageSignal = UltimateSignal
ArbitrageScanner = UltimateScanner
TurboScanner = UltimateScanner
format_turbo_signal = format_ultimate_signal
format_signal_message = format_ultimate_signal
