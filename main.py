"""
MMRdex Bot v3.0 - TURBO Edition
Ultra-fast arbitrage scanner with:
- WebSocket real-time MEXC prices
- Parallel DEX scanning
- Token validation & fake detection
- Net profit after fees
"""
import asyncio
import logging
import sys

import sys
import platform

from config import (
    SCAN_INTERVAL_SEC, 
    SPREAD_CHECK_INTERVAL_SEC,
    SIGNAL_COOLDOWN_SEC,
    LOG_LEVEL
)
# ... imports ...

# Fix for Windows Event Loop Issue (WinError 10022)
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
from database import init_db, get_token_stats, get_price_history
from mexc_client import MEXCClient
from mexc_ws import get_ws_client
from dexscreener_client import DexScreenerClient
from turbo_scanner import TurboScanner, format_turbo_signal
from spread_tracker import SpreadTracker, format_closure_message
from bot import TelegramBot

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)


class MMRdexBot:
    """Main bot orchestrator - TURBO edition"""
    
    def __init__(self):
        self.mexc = MEXCClient()
        self.dexscreener = DexScreenerClient()
        self.scanner = TurboScanner(self.mexc, self.dexscreener)
        self.tracker = SpreadTracker(self.mexc, self.dexscreener)
        self.telegram = TelegramBot()
        self.ws = get_ws_client()
        self._running = False
    
    async def start(self):
        """Start all bot components"""
        logger.info("=" * 50)
        logger.info("🚀 MMRdex TURBO v3.0 Starting...")
        logger.info("=" * 50)
        
        # Initialize database
        await init_db()
        logger.info("✅ Database initialized")
        
        # Start WebSocket for real-time prices
        await self.ws.start()
        logger.info("✅ WebSocket connected")
        
        # Wait for prices to load
        await asyncio.sleep(3)
        logger.info(f"✅ Loaded {len(self.ws.prices)} MEXC prices")
        
        self._running = True
        
        # Start all tasks
        await asyncio.gather(
            self._run_scanner(),
            self._run_tracker(),
            self._run_telegram(),
        )
    
    async def stop(self):
        """Stop all components"""
        self._running = False
        await self.ws.close()
        await self.mexc.close()
        await self.dexscreener.close()
        await self.telegram.stop()
        logger.info("Bot stopped")
    
    async def _run_scanner(self):
        """Main scanner loop - TURBO speed"""
        from chart_generator import generate_spread_chart
        
        logger.info(f"⚡ TURBO Scanner started (interval: {SCAN_INTERVAL_SEC}s)")
        
        scan_count = 0
        signal_count = 0
        
        while self._running:
            try:
                scan_count += 1
                signals = await self.scanner.scan()
                
                # Send notifications for new signals
                for signal in signals:
                    signal_count += 1
                    
                    # Get token statistics
                    token_stats = await get_token_stats(signal.token)
                    message = format_turbo_signal(signal, token_stats)
                    
                    # Generate chart
                    chart_image = None
                    try:
                        # Fetch real klines from MEXC (Real-time chart)
                        # limit=48 -> 12 hours of 15m candles
                        klines = await self.mexc.get_kline_data(signal.token, "Min15", limit=48)
                        
                        if klines:
                            chart_image = generate_spread_chart(
                                signal.token, 
                                klines, 
                                signal.dex_price,
                                signal.direction
                            )
                    except Exception as e:
                        logger.error(f"Chart error: {e}")
                    
                    await self.telegram.send_signal(message, chart_image)
                    logger.info(f"📤 Signal sent: {signal.direction} {signal.token} +{signal.net_profit:.1f}%")
                    
                    await asyncio.sleep(SIGNAL_COOLDOWN_SEC)
                
                # Periodic stats
                if scan_count % 100 == 0:
                    logger.info(f"📊 Stats: {scan_count} scans, {signal_count} signals, {len(self.ws.prices)} prices")
                
            except Exception as e:
                logger.error(f"Scanner error: {e}")
            
            await asyncio.sleep(SCAN_INTERVAL_SEC)
    
    async def _run_tracker(self):
        """Spread tracker loop"""
        logger.info(f"📊 Tracker started (interval: {SPREAD_CHECK_INTERVAL_SEC}s)")
        
        while self._running:
            try:
                closed = await self.tracker.check_closures()
                
                for closure in closed:
                    message = format_closure_message(closure)
                    await self.telegram.send_closure(message)
                    logger.info(f"✅ Closed: {closure.token} - {closure.outcome}")
                
            except Exception as e:
                logger.error(f"Tracker error: {e}")
            
            await asyncio.sleep(SPREAD_CHECK_INTERVAL_SEC)
    
    async def _run_telegram(self):
        """Run Telegram bot"""
        try:
            await self.telegram.start()
        except Exception as e:
            logger.error(f"Telegram error: {e}")


async def main():
    bot = MMRdexBot()
    
    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await bot.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bye!")
        sys.exit(0)
