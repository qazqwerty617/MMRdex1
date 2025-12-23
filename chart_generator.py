"""
Chart Generator
Creates Price Charts: MEXC Trends vs DEX Level
"""
import io
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)

# Dark theme colors
CHART_BG_COLOR = '#0d1117'
CEX_COLOR = '#4ade80'  # Green
DEX_COLOR = '#f87171'  # Red
GRID_COLOR = '#30363d'
TEXT_COLOR = '#e6edf3'


def generate_spread_chart(
    token: str,
    klines: Dict[str, list],
    dex_price: float,
    direction: str = "LONG"
) -> Optional[bytes]:
    """
    Generate chart from MEXC Klines + static DEX price level.
    
    Args:
        token: Token symbol
        klines: Dict with keys 'time' (timestamps) and 'close' (prices)
        dex_price: Current DEX price
        direction: Signal direction
    """
    if not klines or not klines.get("time") or not klines.get("close"):
        return None
    
    try:
        # Parse data
        timestamps = [datetime.fromtimestamp(t) for t in klines["time"]]
        mexc_prices = [float(p) for p in klines["close"]]
        
        # Create figure
        fig, ax = plt.subplots(figsize=(8, 4), facecolor=CHART_BG_COLOR)
        ax.set_facecolor(CHART_BG_COLOR)
        
        # Plot MEXC Price Trend
        ax.plot(timestamps, mexc_prices, color=CEX_COLOR, linewidth=2, label='MEXC Futures')
        
        # Plot DEX Price Level (Horizontal Line)
        ax.axhline(y=dex_price, color=DEX_COLOR, linestyle='--', linewidth=2, label=f'DEX Spot (${dex_price:.4f})')
        
        # Fill the gap (arb opportunity)
        # We fill between the MEXC line and the DEX level
        ax.fill_between(timestamps, mexc_prices, dex_price, color=CEX_COLOR, alpha=0.1)
        
        # Title & Style
        spread = ((dex_price - mexc_prices[-1]) / mexc_prices[-1]) * 100
        title = f"{direction} ${token} | Gap: {abs(spread):.1f}%"
        ax.set_title(title, color=TEXT_COLOR, fontsize=12, fontweight='bold')
        
        # Grid and Labels
        ax.grid(True, color=GRID_COLOR, linestyle=':', alpha=0.6)
        
        # Legend
        legend = ax.legend(loc='best', facecolor=CHART_BG_COLOR, edgecolor=GRID_COLOR)
        for text in legend.get_texts():
            text.set_color(TEXT_COLOR)
            
        # Axes
        ax.tick_params(colors=TEXT_COLOR)
        for spine in ax.spines.values():
            spine.set_color(GRID_COLOR)
            
        # Date format
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        
        # Export
        buf = io.BytesIO()
        plt.savefig(buf, format='png', facecolor=CHART_BG_COLOR, dpi=100)
        buf.seek(0)
        plt.close(fig)
        
        return buf.getvalue()
        
    except Exception as e:
        logger.error(f"Chart generation error for {token}: {e}")
        return None
