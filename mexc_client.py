"""
MEXC API Client
Handles futures contracts, prices, and deposit/withdrawal status
"""
import aiohttp
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# API Endpoints
MEXC_CONTRACT_BASE = "https://contract.mexc.com"
MEXC_SPOT_BASE = "https://api.mexc.com"


class MEXCClient:
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._futures_contracts: dict = {}  # Cache for contracts
        self._deposit_status: dict = {}  # Cache for deposit/withdrawal status
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def get_futures_contracts(self) -> list[dict]:
        """
        Get all futures contracts from MEXC
        Returns list of active contracts with their details
        """
        session = await self._get_session()
        try:
            async with session.get(
                f"{MEXC_CONTRACT_BASE}/api/v1/contract/detail"
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success"):
                        contracts = []
                        for item in data.get("data", []):
                            # state 0 = active
                            if item.get("state") == 0:
                                contracts.append({
                                    "symbol": item.get("symbol"),  # e.g. "BTC_USDT"
                                    "base_coin": item.get("baseCoin"),  # e.g. "BTC"
                                    "quote_coin": item.get("quoteCoin"),  # e.g. "USDT"
                                    "display_name": item.get("displayNameEn"),
                                })
                        self._futures_contracts = {c["base_coin"]: c for c in contracts}
                        return contracts
                logger.error(f"Failed to get futures contracts: {resp.status}")
                return []
        except Exception as e:
            logger.error(f"Error fetching futures contracts: {e}")
            return []
    
    async def get_futures_tickers(self) -> list[tuple[str, float, float]]:
        """
        Get current prices for all futures contracts
        Returns list of (symbol, last_price, volume_24h) sorted by volume DESC
        """
        session = await self._get_session()
        try:
            async with session.get(
                f"{MEXC_CONTRACT_BASE}/api/v1/contract/ticker"
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success"):
                        tickers = []
                        for item in data.get("data", []):
                            symbol = item.get("symbol")  # e.g. "BTC_USDT"
                            last_price = float(item.get("lastPrice", 0))
                            volume_24h = float(item.get("volume24", 0))  # 24h volume
                            if symbol and last_price > 0:
                                # Extract base coin from symbol (BTC_USDT -> BTC)
                                base_coin = symbol.split("_")[0]
                                tickers.append((base_coin, last_price, volume_24h))
                        # Sort by volume descending
                        tickers.sort(key=lambda x: x[2], reverse=True)
                        return tickers
                logger.error(f"Failed to get futures tickers: {resp.status}")
                return []
        except Exception as e:
            logger.error(f"Error fetching futures tickers: {e}")
            return []
    
    async def get_order_book_depth(self, symbol: str, amount_usd: float = 10000) -> dict:
        """
        Get order book and calculate executable price for given USD amount.
        
        Args:
            symbol: Token symbol (e.g., "BTC")
            amount_usd: Trade size in USD to calculate slippage
            
        Returns:
            {
                "bid_price": float,  # Price to sell (best bid after slippage)
                "ask_price": float,  # Price to buy (best ask after slippage)
                "spread_percent": float,  # Bid-ask spread
                "depth_usd": float,  # Total depth within 1% of mid price
            }
        """
        session = await self._get_session()
        try:
            async with session.get(
                f"{MEXC_CONTRACT_BASE}/api/v1/contract/depth/{symbol}_USDT",
                params={"limit": 20}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success") and data.get("data"):
                        book = data["data"]
                        bids = book.get("bids", [])  # [[price, qty], ...]
                        asks = book.get("asks", [])  # [[price, qty], ...]
                        
                        if not bids or not asks:
                            return None
                        
                        # Calculate weighted average price for given amount
                        def calc_executable_price(orders: list, amount_usd: float) -> float:
                            """Calculate average execution price for given USD amount"""
                            remaining = amount_usd
                            total_qty = 0
                            total_value = 0
                            
                            for order in orders:
                                price = float(order[0])
                                qty = float(order[1])
                                order_value = price * qty
                                
                                if remaining <= 0:
                                    break
                                    
                                if order_value <= remaining:
                                    total_qty += qty
                                    total_value += order_value
                                    remaining -= order_value
                                else:
                                    # Partial fill
                                    partial_qty = remaining / price
                                    total_qty += partial_qty
                                    total_value += remaining
                                    remaining = 0
                            
                            return total_value / total_qty if total_qty > 0 else 0
                        
                        # Best bid/ask (top of book)
                        best_bid = float(bids[0][0])
                        best_ask = float(asks[0][0])
                        mid_price = (best_bid + best_ask) / 2
                        
                        # Executable prices with slippage
                        exec_bid = calc_executable_price(bids, amount_usd)
                        exec_ask = calc_executable_price(asks, amount_usd)
                        
                        # Calculate depth within 1% of mid
                        depth_usd = 0
                        for bid in bids:
                            price = float(bid[0])
                            if price >= mid_price * 0.99:
                                depth_usd += price * float(bid[1])
                        for ask in asks:
                            price = float(ask[0])
                            if price <= mid_price * 1.01:
                                depth_usd += price * float(ask[1])
                        
                        spread_pct = ((best_ask - best_bid) / mid_price) * 100
                        
                        return {
                            "bid_price": exec_bid,
                            "ask_price": exec_ask,
                            "mid_price": mid_price,
                            "spread_percent": spread_pct,
                            "depth_usd": depth_usd,
                            "slippage_buy": ((exec_ask - best_ask) / best_ask) * 100 if best_ask > 0 else 0,
                            "slippage_sell": ((best_bid - exec_bid) / best_bid) * 100 if best_bid > 0 else 0,
                        }
                return None
        except Exception as e:
            logger.error(f"Error fetching order book for {symbol}: {e}")
            return None
    
    async def get_deposit_withdraw_status(self) -> dict[str, dict]:
        """
        Get deposit/withdrawal status for all coins
        Returns dict: {coin: {deposit_enabled, withdraw_enabled, networks}}
        """
        session = await self._get_session()
        try:
            async with session.get(
                f"{MEXC_SPOT_BASE}/api/v3/capital/config/getall"
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    status = {}
                    for item in data:
                        coin = item.get("coin", "").upper()
                        networks = item.get("networkList", [])
                        
                        # Check if any network has deposit/withdraw enabled
                        deposit_enabled = any(
                            n.get("depositEnable", False) for n in networks
                        )
                        withdraw_enabled = any(
                            n.get("withdrawEnable", False) for n in networks
                        )
                        
                        # Get network names with enabled deposits
                        enabled_networks = [
                            n.get("network", "") for n in networks
                            if n.get("depositEnable") or n.get("withdrawEnable")
                        ]
                        
                        status[coin] = {
                            "deposit_enabled": deposit_enabled,
                            "withdraw_enabled": withdraw_enabled,
                            "networks": enabled_networks
                        }
                    self._deposit_status = status
                    return status
                logger.error(f"Failed to get deposit status: {resp.status}")
                return {}
        except Exception as e:
            logger.error(f"Error fetching deposit status: {e}")
            return {}
    
    def get_cached_deposit_status(self, coin: str) -> dict:
        """Get cached deposit/withdrawal status for a coin"""
        return self._deposit_status.get(coin.upper(), {
            "deposit_enabled": False,
            "withdraw_enabled": False,
            "networks": []
        })
    
    def is_futures_coin(self, coin: str) -> bool:
        """Check if a coin has futures on MEXC"""
        return coin.upper() in self._futures_contracts
    
    async def get_price_change_24h(self, symbol: str) -> Optional[float]:
        """
        Get 24h price change percentage from MEXC klines.
        Returns percentage change (e.g., +10.5 or -5.2) or None on error.
        """
        session = await self._get_session()
        try:
            # Use futures ticker which includes 24h change
            async with session.get(
                f"{MEXC_CONTRACT_BASE}/api/v1/contract/ticker",
                params={"symbol": f"{symbol}_USDT"}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success") and data.get("data"):
                        # riseFallRate is the 24h price change rate
                        rise_fall = data["data"].get("riseFallRate", 0)
                        return float(rise_fall) * 100  # Convert to percentage
                return None
        except Exception as e:
            logger.error(f"Error fetching MEXC price change for {symbol}: {e}")
            return None

    async def get_kline_data(self, symbol: str, interval: str = "Min15", limit: int = 10) -> list:
        """
        Get kline (candle) data from MEXC Futures
        """
        session = await self._get_session()
        try:
            # MEXC Futures Kline Endpoint
            async with session.get(
                f"{MEXC_CONTRACT_BASE}/api/v1/contract/kline/{symbol}_USDT",
                params={"interval": interval, "limit": limit}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success") and data.get("data"):
                        # Response format: {"data": {"time": [...], "close": [...], ...}}
                        return data["data"]
        except Exception as e:
            logger.error(f"Error fetching klines for {symbol}: {e}")
        return None
