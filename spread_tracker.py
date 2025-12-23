"""
Spread Tracker
Monitors active signals and detects when spreads close
"""
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from config import SPREAD_CLOSURE_THRESHOLD
from mexc_client import MEXCClient
from dexscreener_client import DexScreenerClient
from database import get_active_signals, close_signal

logger = logging.getLogger(__name__)


@dataclass
class ClosedSignal:
    """Represents a closed arbitrage signal"""
    signal_id: int
    token: str
    direction: str
    chain: str
    initial_spread: float
    final_spread: float
    price_change_percent: float
    outcome: str  # win, draw, lose
    align_seconds: int  # Time until spread closed


class SpreadTracker:
    def __init__(self, mexc: MEXCClient, dexscreener: DexScreenerClient):
        self.mexc = mexc
        self.dexscreener = dexscreener
    
    async def check_closures(self) -> list[ClosedSignal]:
        """
        Check all active signals for spread closure
        Returns list of closed signals
        """
        closed = []
        
        # Get active signals from database
        active_signals = await get_active_signals()
        if not active_signals:
            return []
        
        # Get current MEXC prices
        tickers_list = await self.mexc.get_futures_tickers()
        tickers = {t[0]: t[1] for t in tickers_list}
        
        for signal in active_signals:
            try:
                token = signal["token"]
                
                # Get current MEXC price
                current_mexc_price = tickers.get(token)
                if not current_mexc_price:
                    continue
                
                # Get current DEX price
                dex_pair = await self.dexscreener.get_best_dex_price(token)
                if not dex_pair:
                    continue
                
                current_dex_price = dex_pair["price_usd"]
                
                # Calculate current spread
                current_spread = abs((current_dex_price - current_mexc_price) / current_mexc_price * 100)
                
                # Check if spread has closed
                if current_spread < SPREAD_CLOSURE_THRESHOLD:
                    # Calculate price change from signal
                    original_mexc_price = signal["mexc_price"]
                    price_change = ((current_mexc_price - original_mexc_price) / original_mexc_price) * 100
                    
                    # Adjust for direction (LONG expects price increase, SHORT expects decrease)
                    if signal["direction"] == "SHORT":
                        price_change = -price_change
                    
                    # Close the signal
                    outcome = await close_signal(
                        signal_id=signal["id"],
                        final_spread=current_spread,
                        price_change_percent=price_change
                    )
                    
                    # Calculate align time
                    created_at = signal.get("created_at", "")
                    if created_at:
                        try:
                            created_time = datetime.fromisoformat(created_at)
                            align_seconds = int((datetime.utcnow() - created_time).total_seconds())
                        except:
                            align_seconds = 0
                    else:
                        align_seconds = 0
                    
                    closed_signal = ClosedSignal(
                        signal_id=signal["id"],
                        token=token,
                        direction=signal["direction"],
                        chain=signal.get("chain", "unknown"),
                        initial_spread=signal["spread_percent"],
                        final_spread=current_spread,
                        price_change_percent=price_change,
                        outcome=outcome,
                        align_seconds=align_seconds
                    )
                    closed.append(closed_signal)
                    
                    logger.info(
                        f"Spread closed: {token} | {outcome.upper()} | "
                        f"PnL: {price_change:+.1f}% | Aligned in {align_seconds}s"
                    )
                    
            except Exception as e:
                logger.error(f"Error checking signal {signal.get('id')}: {e}")
                continue
        
        return closed


def format_closure_message(closed: ClosedSignal) -> str:
    """Format spread closure notification for Telegram (minimal @mexcdao style)"""
    # Format align time
    if closed.align_seconds >= 3600:
        align_str = f"{closed.align_seconds // 3600}h {(closed.align_seconds % 3600) // 60}m"
    elif closed.align_seconds >= 60:
        align_str = f"{closed.align_seconds // 60}m {closed.align_seconds % 60}s"
    else:
        align_str = f"{closed.align_seconds}s"
    
    # Outcome emoji
    if closed.outcome == "win":
        emoji = "✅"
        pnl_emoji = "🟢"
    elif closed.outcome == "lose":
        emoji = "❌"
        pnl_emoji = "🔴"
    else:
        emoji = "➖"
        pnl_emoji = "🟠"
    
    # Minimal format like @mexcdao
    return (
        f"{emoji} #{closed.token} #{closed.chain.upper()} Aligned in {align_str}\n"
        f"📊 Spread: {closed.final_spread:.1f}%\n"
        f"{pnl_emoji} PnL: {closed.price_change_percent:+.1f}%"
    )

