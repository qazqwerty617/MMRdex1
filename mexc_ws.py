"""
MEXC WebSocket Client
Real-time price feeds for maximum speed
"""
import asyncio
import json
import logging
from typing import Callable, Optional
import aiohttp

logger = logging.getLogger(__name__)

MEXC_WS_URL = "wss://contract.mexc.com/edge"


class MEXCWebSocket:
    """
    WebSocket client for real-time MEXC futures prices.
    Much faster than REST API polling.
    """
    
    def __init__(self):
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._prices: dict[str, float] = {}  # {symbol: last_price}
        self._callbacks: list[Callable] = []
        self._reconnect_delay = 1
        self._subscribed_symbols: set[str] = set()
    
    @property
    def prices(self) -> dict[str, float]:
        """Get current price snapshot"""
        return self._prices.copy()
    
    def get_price(self, symbol: str) -> Optional[float]:
        """Get price for specific symbol"""
        return self._prices.get(symbol)
    
    async def connect(self):
        """Connect to WebSocket"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        
        try:
            self._ws = await self._session.ws_connect(
                MEXC_WS_URL,
                heartbeat=30,
                receive_timeout=60
            )
            self._running = True
            self._reconnect_delay = 1
            logger.info("✅ MEXC WebSocket connected")
            return True
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            return False
    
    async def subscribe_tickers(self, symbols: list[str] = None):
        """
        Subscribe to ticker updates.
        If symbols is None, subscribes to ALL tickers.
        """
        if not self._ws:
            return
        
        # Subscribe to all tickers at once (more efficient)
        sub_msg = {
            "method": "sub.tickers",
            "param": {}
        }
        
        await self._ws.send_json(sub_msg)
        logger.info("📡 Subscribed to all MEXC tickers")
    
    async def _handle_message(self, data: dict):
        """Process incoming WebSocket message"""
        channel = data.get("channel", "")
        
        if channel == "push.tickers":
            # Batch ticker update
            tickers = data.get("data", [])
            for ticker in tickers:
                symbol_raw = ticker.get("symbol", "")  # e.g., "BTC_USDT"
                if "_USDT" in symbol_raw:
                    symbol = symbol_raw.replace("_USDT", "")
                    price = float(ticker.get("lastPrice", 0))
                    if price > 0:
                        self._prices[symbol] = price
        
        elif channel == "push.ticker":
            # Single ticker update
            ticker = data.get("data", {})
            symbol_raw = ticker.get("symbol", "")
            if "_USDT" in symbol_raw:
                symbol = symbol_raw.replace("_USDT", "")
                price = float(ticker.get("lastPrice", 0))
                if price > 0:
                    self._prices[symbol] = price
    
    async def listen(self):
        """Main listening loop"""
        while self._running:
            try:
                if not self._ws or self._ws.closed:
                    success = await self.connect()
                    if not success:
                        await asyncio.sleep(self._reconnect_delay)
                        self._reconnect_delay = min(self._reconnect_delay * 2, 30)
                        continue
                    await self.subscribe_tickers()
                
                # Main read loop
                async for msg in self._ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            # Parse JSON efficiently
                            data = json.loads(msg.data)
                            
                            # Handle different message types
                            channel = data.get("channel", "")
                            
                            if channel == "push.tickers":
                                # Ticker list update
                                for ticker in data.get("data", []):
                                    sym = ticker.get("symbol", "")
                                    if sym.endswith("_USDT"):
                                        price = float(ticker.get("lastPrice", 0))
                                        if price > 0:
                                            self._prices[sym[:-5]] = price
                                            
                            elif channel == "push.ticker":
                                # Single ticker update
                                ticker = data.get("data", {})
                                sym = ticker.get("symbol", "")
                                if sym.endswith("_USDT"):
                                    price = float(ticker.get("lastPrice", 0))
                                    if price > 0:
                                       self._prices[sym[:-5]] = price

                        except json.JSONDecodeError:
                            continue
                            
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error(f"WebSocket error frame: {msg.data}")
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        logger.warning("WebSocket closed by server")
                        break
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"WebSocket listener error: {e}")
                
            # Reconnection delay if loop breaks
            if self._running:
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, 60)
    
    async def start(self):
        """Start WebSocket connection and listening"""
        await self.connect()
        if self._ws:
            await self.subscribe_tickers()
            # Start listening in background
            asyncio.create_task(self.listen())
    
    async def close(self):
        """Close WebSocket connection"""
        self._running = False
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("WebSocket closed")


# Global instance
_ws_client: Optional[MEXCWebSocket] = None


def get_ws_client() -> MEXCWebSocket:
    """Get singleton WebSocket client"""
    global _ws_client
    if _ws_client is None:
        _ws_client = MEXCWebSocket()
    return _ws_client
