"""
Arbitrage Scanner v2.0
Scans for REAL arbitrage opportunities between DEX and MEXC Futures.
Features:
- Token validation to filter fakes
- Order book depth for real execution prices
- Net profit calculation after fees
- Enhanced filtering for major tokens
"""
import asyncio
import logging
from dataclasses import dataclass
from typing import Literal, Optional

from config import (
    MIN_LIQUIDITY_USD, MIN_VOLUME_24H_USD, MIN_FDV_USD, DEXSCREENER_BATCH_SIZE, 
    TOKEN_BLACKLIST, TOTAL_FEES_PERCENT, MIN_TXNS_24H
)
from mexc_client import MEXCClient
from dexscreener_client import DexScreenerClient, get_chain_display_name
from database import save_signal, check_signal_exists, save_price_history
from pair_manager import PairManager
from token_validator import get_validator, TokenValidator

logger = logging.getLogger(__name__)


@dataclass
class ArbitrageSignal:
    token: str
    direction: Literal["LONG", "SHORT"]
    spread_percent: float
    net_profit: float  # After fees
    mexc_price: float
    dex_price: float
    dex_url: str
    chain: str
    liquidity_usd: float
    volume_24h: float
    volume_24h: float
    order_book_depth: float = 0  # MEXC order book depth
    id: Optional[int] = None


class ArbitrageScanner:
    def __init__(self, mexc_client: MEXCClient, dexscreener_client: DexScreenerClient):
        self.mexc = mexc_client
        self.dexscreener = dexscreener_client
        self.pair_manager = PairManager(dexscreener_client)
        self.validator = get_validator()
        self._discovery_task = None
    
    async def start_discovery(self, tickers: dict[str, float]):
        """Start background discovery of new pairs"""
        if self._discovery_task is None or self._discovery_task.done():
            self._discovery_task = asyncio.create_task(
                self.pair_manager.discover_pairs(tickers)
            )
            
    async def scan(self) -> list[ArbitrageSignal]:
        """
        Scan for REAL arbitrage opportunities.
        Now with token validation, order book checks, and net profit calculation.
        """
        signals = []
        
        # 1. Get current MEXC futures prices
        tickers_list = await self.mexc.get_futures_tickers()
        if not tickers_list:
            logger.warning("No futures tickers received")
            return []
            
        # Create dict for fast lookup: {symbol: (price, volume)}
        mexc_prices = {t[0]: t[1] for t in tickers_list}
        
        # 2. Start discovery for unknown tokens (in background)
        await self.start_discovery(mexc_prices)
        
        # 3. Get batch candidates from PairManager
        batches = self.pair_manager.get_batch_candidates()
        
        if not batches:
            logger.info("No known pairs to scan yet. Waiting for discovery...")
            return []
            
        # 4. Process batches
        for chain, addresses in batches.items():
            for i in range(0, len(addresses), DEXSCREENER_BATCH_SIZE):
                chunk = addresses[i:i + DEXSCREENER_BATCH_SIZE]
                
                try:
                    pairs = await self.dexscreener.get_pairs_by_addresses(chain, chunk)
                    
                    for pair in pairs:
                        signal = await self._process_pair(pair, chain, mexc_prices)
                        if signal:
                            signals.append(signal)
                        
                except Exception as e:
                    logger.error(f"Error scanning batch {chain}: {e}")
                    
                await asyncio.sleep(0.1)
                
        return signals
    
    async def _process_pair(
        self, 
        pair: dict, 
        chain: str, 
        mexc_prices: dict[str, float]
    ) -> Optional[ArbitrageSignal]:
        """Process a single pair and return signal if valid"""
        symbol = pair["symbol"]
        dex_price = pair["price_usd"]
        
        # Basic checks
        if symbol not in mexc_prices:
            return None
        if symbol in TOKEN_BLACKLIST:
            return None
            
        mexc_price = mexc_prices[symbol]
        if mexc_price <= 0 or dex_price <= 0:
            return None
        
        # Calculate raw spread
        spread = ((dex_price - mexc_price) / mexc_price) * 100
        abs_spread = abs(spread)
        
        # Save price history for charts (all valid pairs)
        await save_price_history(
            token=symbol,
            chain=pair["chain"],
            cex_price=mexc_price,
            dex_price=dex_price,
            spread_percent=spread
        )
        
        # ========== TOKEN VALIDATION ==========
        is_valid, reason = self.validator.validate_token(
            symbol=symbol,
            chain=chain,
            dex_price=dex_price,
            mexc_price=mexc_price,
            spread_percent=abs_spread,
            contract_address=pair.get("pair_address")
        )
        
        if not is_valid:
            logger.debug(f"Token validation failed: {symbol} - {reason}")
            return None
        
        # ========== SPREAD FILTERS ==========
        if abs_spread < MIN_SPREAD_PERCENT:
            return None
        if abs_spread > MAX_SPREAD_PERCENT:
            return None
        
        # Net profit after fees
        net_profit = self.validator.calculate_net_profit(abs_spread)
        
        # Must be profitable after fees (minimum 3% net)
        if not self.validator.is_profitable(abs_spread, min_profit=3.0):
            logger.debug(f"Not profitable after fees: {symbol} spread={abs_spread:.1f}% net={net_profit:.1f}%")
            return None
        
        # ========== LIQUIDITY & VOLUME FILTERS ==========
        liquidity = pair.get("liquidity_usd", 0)
        volume_24h = pair.get("volume_24h", 0)
        
        if liquidity < MIN_LIQUIDITY_USD:
            return None
        if volume_24h < MIN_VOLUME_24H_USD:
            return None
        
        # Check transaction count (Activity Filter)
        txns = pair.get("txns", {}).get("h24", {})
        total_txns = txns.get("buys", 0) + txns.get("sells", 0)
        if total_txns < MIN_TXNS_24H:
            return None
        
        # Volume/Liquidity ratio - filter dead pools
        if liquidity > 0:
            volume_ratio = volume_24h / liquidity
            # Minimum 5% daily turnover (more strict)
            if volume_ratio < 0.05:
                return None
        
        # For altcoins, require higher volume
        if not self.validator.is_major_token(symbol):
            if volume_24h < MIN_VOLUME_24H_USD:  # Use config value
                return None
        
        # FDV check
        pair_fdv = float(pair.get("fdv", 0) or 0)
        if pair_fdv > 0 and pair_fdv < MIN_FDV_USD:
            return None
        
        # ========== PRICE CORRELATION CHECK ==========
        dex_change_24h = pair.get("price_change_24h", 0)
        mexc_change_24h = await self.mexc.get_price_change_24h(symbol)
        
        if mexc_change_24h is not None:
            # If price movements are opposite by >20%, likely different tokens
            if (dex_change_24h > 20 and mexc_change_24h < -20) or \
               (dex_change_24h < -20 and mexc_change_24h > 20):
                logger.debug(f"Price correlation failed: {symbol} DEX={dex_change_24h:+.1f}% MEXC={mexc_change_24h:+.1f}%")
                return None
        
        # ========== ORDER BOOK CHECK ==========
        order_book = await self.mexc.get_order_book_depth(symbol, amount_usd=5000)
        order_book_depth = 0
        
        if order_book:
            order_book_depth = order_book.get("depth_usd", 0)
            # Require at least $10k depth for execution
            if order_book_depth < 10_000:
                logger.debug(f"Insufficient order book depth: {symbol} depth=${order_book_depth:.0f}")
                return None
        
        # ========== DUPLICATE CHECK ==========
        direction = "LONG" if spread > 0 else "SHORT"
        
        if await check_signal_exists(symbol, direction):
            return None
        
        # ========== CREATE SIGNAL ==========
        signal = ArbitrageSignal(
            token=symbol,
            direction=direction,
            spread_percent=abs_spread,
            net_profit=net_profit,
            mexc_price=mexc_price,
            dex_price=dex_price,
            dex_url=pair["url"],
            chain=pair["chain"],
            liquidity_usd=liquidity,
            volume_24h=volume_24h,
            order_book_depth=order_book_depth
        )
        
        # Get deposit status
        dep_status = self.mexc.get_cached_deposit_status(symbol)
        
        # Save to DB
        signal_id = await save_signal(
            token=symbol,
            chain=pair["chain"],
            direction=direction,
            spread_percent=abs_spread,
            dex_price=dex_price,
            mexc_price=mexc_price,
            dex_source="DexScreener",
            liquidity_usd=liquidity,
            volume_24h_usd=volume_24h,
            deposit_enabled=dep_status.get("deposit_enabled", False),
            withdraw_enabled=dep_status.get("withdraw_enabled", False)
        )
        
        signal.id = signal_id
        
        logger.info(
            f"✅ NEW SIGNAL: {direction} {symbol} | "
            f"Spread: {abs_spread:.1f}% | Net: {net_profit:.1f}% | "
            f"Liq: ${liquidity/1000:.0f}K | Depth: ${order_book_depth/1000:.0f}K"
        )
        
        return signal


def format_signal_message(signal: ArbitrageSignal, token_stats: dict = None) -> str:
    """Format signal message for Telegram with net profit"""
    direction_emoji = "🟢" if signal.direction == "LONG" else "🔴"
    chain_display = get_chain_display_name(signal.chain)
    
    # Generate URLs
    mexc_url = f"https://futures.mexc.com/exchange/{signal.token}_USDT"
    
    # Format prices nicely
    def format_price(p: float) -> str:
        if p >= 1:
            return f"${p:.2f}"
        elif p >= 0.01:
            return f"${p:.4f}"
        else:
            return f"${p:.6f}"
    
    # Build message with NET PROFIT prominently displayed
    lines = [
        f"{direction_emoji} <b>{signal.direction}</b> #{signal.token} | {chain_display}",
        f"💰 <b>Net Profit: {signal.net_profit:+.1f}%</b> (Spread: {signal.spread_percent:.1f}%)",
        f"📊 DEX {format_price(signal.dex_price)} → MEXC {format_price(signal.mexc_price)}",
        f"💧 Liq: ${signal.liquidity_usd/1000:.0f}K | Vol: ${signal.volume_24h/1000:.0f}K",
    ]
    
    # Order book depth if available
    if signal.order_book_depth > 0:
        lines.append(f"📈 MEXC Depth: ${signal.order_book_depth/1000:.0f}K")
    
    # Add historical stats if available
    if token_stats and token_stats.get("total", 0) > 0:
        lines.append(
            f"📈 History: W/D/L {token_stats['wins']}/{token_stats['draws']}/{token_stats['loses']} "
            f"| Avg: {token_stats['avg_pnl']:+.1f}%"
        )
    
    # Links
    lines.append(f"<a href='{signal.dex_url}'>DEX</a> • <a href='{mexc_url}'>MEXC Futures</a>")
    
    return "\n".join(lines)
