"""
Fresh Scanner - Finds high spread opportunities by searching directly
Instead of using cached pairs, searches DexScreener dynamically
"""
import asyncio
import logging
import time
from typing import Dict, List, Optional
from dataclasses import dataclass

from config import (
    MIN_SPREAD_PERCENT, MAX_SPREAD_PERCENT, MIN_LIQUIDITY_USD,
    MIN_VOLUME_24H_USD, TOKEN_BLACKLIST, TOTAL_FEES_PERCENT
)
from mexc_client import MEXCClient
from mexc_ws import get_ws_client
from dexscreener_client import DexScreenerClient, get_chain_display_name
from database import save_signal, check_signal_exists, save_price_history

logger = logging.getLogger(__name__)


@dataclass
class FreshSignal:
    token: str
    direction: str  # "LONG" or "SHORT"
    spread_percent: float
    net_profit: float
    mexc_price: float
    dex_price: float
    dex_url: str
    chain: str
    liquidity_usd: float
    volume_24h: float
    funding_rate: float = 0
    market_cap: float = 0


class FreshScanner:
    """
    Scans for high-spread opportunities by searching DexScreener directly.
    Does NOT rely on cached pairs - finds fresh opportunities.
    """
    
    def __init__(self, mexc_client: MEXCClient, dexscreener_client: DexScreenerClient):
        self.mexc = mexc_client
        self.dexscreener = dexscreener_client
        self.ws = get_ws_client()
        
        # Signal cooldowns - prevent spam
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
    
    async def scan(self) -> List[FreshSignal]:
        """
        FRESH SCAN - Search DexScreener directly for each MEXC token
        This finds opportunities that cached pairs miss
        """
        signals = []
        start_time = time.time()
        
        # Ensure WebSocket is running
        await self.start_ws()
        
        # Get real-time prices from WebSocket
        mexc_prices = self.ws.prices
        if not mexc_prices:
            tickers = await self.mexc.get_futures_tickers()
            mexc_prices = {t[0]: t[1] for t in tickers}
        
        if not mexc_prices:
            return []
        
        self._scan_count += 1
        
        # Scan all tokens for spreads
        tokens_to_check = [t for t in mexc_prices.keys() if t not in TOKEN_BLACKLIST]
        
        # Log every 10 scans
        if self._scan_count % 10 == 0:
            logger.info(f"🔍 Fresh scan #{self._scan_count}: checking {len(tokens_to_check)} tokens...")
        
        for symbol in tokens_to_check:
            try:
                signal = await self._check_token(symbol, mexc_prices[symbol])
                if signal:
                    signals.append(signal)
            except Exception as e:
                logger.debug(f"Error checking {symbol}: {e}")
        
        scan_time = (time.time() - start_time) * 1000
        if signals:
            logger.info(f"⚡ Fresh scan completed in {scan_time:.0f}ms - Found {len(signals)} signals")
        
        return signals
    
    async def _check_token(self, symbol: str, mexc_price: float) -> Optional[FreshSignal]:
        """Check single token for spread opportunity"""
        
        # Skip if on cooldown for both directions
        if self._is_on_cooldown(symbol, "LONG") and self._is_on_cooldown(symbol, "SHORT"):
            return None
        
        # Search DexScreener for this token
        pair = await self.dexscreener.get_best_dex_price(
            symbol,
            min_liquidity=MIN_LIQUIDITY_USD,
            min_volume=MIN_VOLUME_24H_USD
        )
        
        if not pair:
            return None
        
        dex_price = pair.get("price_usd", 0)
        if dex_price <= 0 or mexc_price <= 0:
            return None
        
        # Calculate spread
        spread = ((dex_price - mexc_price) / mexc_price) * 100
        abs_spread = abs(spread)
        direction = "LONG" if spread > 0 else "SHORT"
        
        # Check spread threshold
        if abs_spread < MIN_SPREAD_PERCENT or abs_spread > MAX_SPREAD_PERCENT:
            return None
        
        # Check cooldown for this direction
        if self._is_on_cooldown(symbol, direction):
            return None
        
        # Get additional data
        liquidity = pair.get("liquidity_usd", 0)
        volume_24h = pair.get("volume_24h", 0)
        chain = pair.get("chain", "unknown")
        dex_url = pair.get("url", "")
        market_cap = pair.get("fdv", 0)
        
        # Calculate net profit
        net_profit = abs_spread - TOTAL_FEES_PERCENT
        
        if net_profit < 1.0:  # At least 1% net profit
            return None
        
        # Check if already in database
        if await check_signal_exists(symbol, direction):
            return None
        
        # Create and save signal
        signal = FreshSignal(
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
            market_cap=market_cap
        )
        
        # Save to DB
        await save_signal(
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
        
        # Set cooldown
        self._set_cooldown(symbol, direction)
        
        logger.info(
            f"🚀 FRESH SIGNAL: {direction} ${symbol} | "
            f"Spread: {abs_spread:.1f}% | Net: +{net_profit:.1f}%"
        )
        
        return signal


def format_fresh_signal(signal: FreshSignal) -> str:
    """Format signal for Telegram"""
    chain = get_chain_display_name(signal.chain)
    
    if signal.direction == "LONG":
        header = f"🟢🟢 <b>LONG? #{signal.token}</b> Spread +{signal.spread_percent:.2f}%"
        desc = "MEXC cheaper than DEX - expected to rise"
    else:
        header = f"🔴🔴 <b>SHORT? #{signal.token}</b> Spread -{signal.spread_percent:.2f}%"
        desc = "MEXC more expensive than DEX - expected to fall"
    
    # Format prices
    mexc_str = f"${signal.mexc_price:.6f}" if signal.mexc_price < 0.01 else f"${signal.mexc_price:.4f}"
    dex_str = f"${signal.dex_price:.6f}" if signal.dex_price < 0.01 else f"${signal.dex_price:.4f}"
    
    # Liquidity
    if signal.liquidity_usd >= 1_000_000:
        liq_str = f"${signal.liquidity_usd/1_000_000:.2f}M"
    elif signal.liquidity_usd >= 1000:
        liq_str = f"${signal.liquidity_usd/1000:.1f}K"
    else:
        liq_str = f"${signal.liquidity_usd:.0f}"
    
    # Volume
    if signal.volume_24h >= 1_000_000:
        vol_str = f"${signal.volume_24h/1_000_000:.2f}M"
    elif signal.volume_24h >= 1000:
        vol_str = f"${signal.volume_24h/1000:.1f}K"
    else:
        vol_str = f"${signal.volume_24h:.0f}"
    
    return (
        f"{header}\n"
        f"{desc}\n\n"
        f"🌐 Price DEX: {dex_str}\n"
        f"🎰 Price MEXC: {mexc_str}\n\n"
        f"💰 Liquidity: {liq_str}\n"
        f"💸 Volume 24h: {vol_str}\n\n"
        f"⛓️ #{chain}\n"
        f"🌐 <a href='{signal.dex_url}'>DEX</a> • "
        f"🎰 <a href='https://futures.mexc.com/exchange/{signal.token}_USDT'>MEXC</a>"
    )
