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
    MAJOR_TOKENS
)
from mexc_client import MEXCClient
from mexc_ws import get_ws_client
from dexscreener_client import DexScreenerClient, get_chain_display_name
from database import save_signal, check_signal_exists, save_price_history
from listing_detector import get_listing_detector
from pump_detector import get_pump_detector

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


class UltimateScanner:
    """
    The ultimate MEXC-DEX spread hunter.
    Finds real arbitrage opportunities.
    """
    
    def __init__(self, mexc_client: MEXCClient, dexscreener_client: DexScreenerClient):
        self.mexc = mexc_client
        self.dexscreener = dexscreener_client
        self.ws = get_ws_client()
        
        # Detectors
        self.listing_detector = get_listing_detector()
        self.pump_detector = get_pump_detector()
        
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
        
        # Step 3: Priority tokens (new listings + recent pumps + random sample)
        priority_tokens = set()
        
        # Add new listings (HIGHEST PRIORITY)
        for listing in new_listings:
            priority_tokens.add(listing.symbol)
        
        # Add recent pump/dump tokens
        for event in self.pump_detector.get_recent_events(max_age_sec=120):
            priority_tokens.add(event.symbol)
        
        # Add sample of other tokens (rotate through all)
        all_tokens = [t for t in mexc_prices.keys() if t not in TOKEN_BLACKLIST]
        sample_start = (self._scan_count * 50) % len(all_tokens)
        sample_end = min(sample_start + 50, len(all_tokens))
        for token in all_tokens[sample_start:sample_end]:
            priority_tokens.add(token)
        
        # Step 4: Check spreads for priority tokens
        for symbol in priority_tokens:
            if symbol not in mexc_prices:
                continue
            
            try:
                signal = await self._check_spread(
                    symbol, 
                    mexc_prices[symbol],
                    is_new_listing=(symbol in [l.symbol for l in new_listings])
                )
                if signal:
                    signals.append(signal)
            except Exception as e:
                logger.debug(f"Error checking {symbol}: {e}")
        
        # Log periodically
        if self._scan_count % 30 == 0:
            logger.info(
                f"📊 Scan #{self._scan_count}: "
                f"checked {len(priority_tokens)} tokens, "
                f"found {len(signals)} signals"
            )
        
        scan_time = (time.time() - start_time) * 1000
        if signals:
            logger.info(f"⚡ Scan completed in {scan_time:.0f}ms - Found {len(signals)} signals")
        
        return signals
    
    async def _check_spread(
        self, 
        symbol: str, 
        mexc_price: float,
        is_new_listing: bool = False
    ) -> Optional[UltimateSignal]:
        """Check single token for spread opportunity"""
        
        # Get DEX price
        pair = await self.dexscreener.get_best_dex_price(
            symbol,
            min_liquidity=MIN_LIQUIDITY_USD,
            min_volume=0  # Don't filter by volume - new tokens may have low volume
        )
        
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
        
        # PRICE RATIO FILTER - the KEY to avoiding fake tokens
        # Real arbitrage: prices differ but are in same ballpark
        # Fake tokens: completely different price (e.g. $0.05 vs $5.00)
        price_ratio = dex_price / mexc_price if mexc_price > 0 else 0
        
        if symbol in MAJOR_TOKENS:
            # Major tokens: DEX price must be 0.8x-1.25x of MEXC (max 20% spread)
            if price_ratio < 0.8 or price_ratio > 1.25:
                logger.debug(f"Skip {symbol}: Major token price ratio {price_ratio:.2f} (fake)")
                return None
        else:
            # Altcoins: DEX price must be 0.6x-1.5x of MEXC (max 50% spread realistic)
            if price_ratio < 0.6 or price_ratio > 1.5:
                logger.debug(f"Skip {symbol}: Price ratio {price_ratio:.2f} outside realistic range (fake)")
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
