"""
DexScreener API Client
Fetches token prices, liquidity, and volume from DEX
"""
import aiohttp
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

DEXSCREENER_BASE = "https://api.dexscreener.com"
CACHE_TTL_SECONDS = 60  # Cache results for 60 seconds (reduces API calls)


class DexScreenerClient:
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._cache: dict[str, tuple[float, list]] = {}  # {symbol: (timestamp, pairs)}
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
    
    def _get_cached(self, symbol: str) -> Optional[list]:
        """Get cached result if not expired"""
        if symbol in self._cache:
            timestamp, pairs = self._cache[symbol]
            if time.time() - timestamp < CACHE_TTL_SECONDS:
                return pairs
            else:
                del self._cache[symbol]  # Expired
        return None
    
    def _set_cache(self, symbol: str, pairs: list):
        """Cache search results"""
        self._cache[symbol] = (time.time(), pairs)
    
    async def search_token(self, symbol: str) -> list[dict]:
        """
        Search for token pairs by symbol
        Returns list of pairs with price, liquidity, volume
        """
        # Check cache first
        cached = self._get_cached(symbol)
        if cached is not None:
            return cached
        
        session = await self._get_session()
        try:
            async with session.get(
                f"{DEXSCREENER_BASE}/latest/dex/search",
                params={"q": symbol}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = []
                    for pair in data.get("pairs", []) or []:
                        # Get base token info
                        base_token = pair.get("baseToken", {})
                        token_symbol = base_token.get("symbol", "").upper()
                        
                        # Only include if symbol matches
                        if token_symbol != symbol.upper():
                            continue
                        
                        price_usd = float(pair.get("priceUsd", 0) or 0)
                        liquidity = pair.get("liquidity", {})
                        liquidity_usd = float(liquidity.get("usd", 0) or 0)
                        volume = pair.get("volume", {})
                        volume_24h = float(volume.get("h24", 0) or 0)
                        
                        if price_usd > 0:
                            # Get FDV and market cap for scam filtering
                            fdv = float(pair.get("fdv", 0) or 0)
                            market_cap = float(pair.get("marketCap", 0) or 0)
                            
                            pairs.append({
                                "symbol": token_symbol,
                                "chain": pair.get("chainId", "unknown"),
                                "dex": pair.get("dexId", "unknown"),
                                "pair_address": pair.get("pairAddress", ""),
                                "price_usd": price_usd,
                                "liquidity_usd": liquidity_usd,
                                "volume_24h": volume_24h,
                                "fdv": fdv,
                                "market_cap": market_cap,
                                "price_change_24h": float(pair.get("priceChange", {}).get("h24", 0) or 0),
                                "url": pair.get("url", "")
                            })
                    
                    # Sort by liquidity (highest first)
                    pairs.sort(key=lambda x: x["liquidity_usd"], reverse=True)
                    
                    # Cache the result
                    self._set_cache(symbol, pairs)
                    return pairs
                
                # 400 = token not found, silently return empty
                return []
        except Exception as e:
            logger.error(f"Error searching token {symbol}: {e}")
            return []
    
    async def get_pairs_by_addresses(self, chain_id: str, pair_addresses: list[str]) -> list[dict]:
        """
        Get multiple pairs by address (batch request)
        Max 30 addresses per request
        """
        if not pair_addresses:
            return []
            
        # Join addresses with comma
        addresses_str = ",".join(pair_addresses)
        
        session = await self._get_session()
        try:
            async with session.get(
                f"{DEXSCREENER_BASE}/latest/dex/pairs/{chain_id}/{addresses_str}"
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = []
                    # Response format: {"schemaVersion": "1.0.0", "pairs": [...]} or just list for some endpoints
                    # But /pairs endpoint usually returns {"pairs": [...]}
                    raw_pairs = data.get("pairs", []) if isinstance(data, dict) else data
                    
                    if not raw_pairs:
                        return []

                    for pair in raw_pairs:
                        price_usd = float(pair.get("priceUsd", 0) or 0)
                        liquidity = pair.get("liquidity", {})
                        liquidity_usd = float(liquidity.get("usd", 0) or 0)
                        volume = pair.get("volume", {})
                        volume_24h = float(volume.get("h24", 0) or 0)
                        
                        base_token = pair.get("baseToken", {})
                        token_symbol = base_token.get("symbol", "").upper()
                        
                        if price_usd > 0:
                            # Get FDV and market cap
                            fdv = float(pair.get("fdv", 0) or 0)
                            market_cap = float(pair.get("marketCap", 0) or 0)
                            
                            pairs.append({
                                "symbol": token_symbol,
                                "chain": pair.get("chainId", "unknown"),
                                "dex": pair.get("dexId", "unknown"),
                                "pair_address": pair.get("pairAddress", ""),
                                "price_usd": price_usd,
                                "liquidity_usd": liquidity_usd,
                                "volume_24h": volume_24h,
                                "fdv": fdv,
                                "market_cap": market_cap,
                                "price_change_24h": float(pair.get("priceChange", {}).get("h24", 0) or 0),
                                "url": pair.get("url", "")
                            })
                    return pairs
                
                # Silently return empty for non-200
                return []
        except Exception as e:
            logger.error(f"Error discrete batch fetching: {e}")
            return []
            
    async def get_best_dex_price(
        self, 
        symbol: str, 
        min_liquidity: float = 0,
        min_volume: float = 0,
        reference_price: float = None
    ) -> Optional[dict]:
        """
        Get the best DEX pair for a token (highest liquidity that meets criteria)
        
        Args:
            reference_price: MEXC price to validate it's the same token.
                            If DEX price differs by more than 2x, it's probably a fake token.
        
        Returns pair info or None if not found
        """
        pairs = await self.search_token(symbol)
        
        for pair in pairs:
            # Check liquidity and volume
            if pair["liquidity_usd"] < min_liquidity or pair["volume_24h"] < min_volume:
                continue
            
            # Validate it's the same token by checking price is reasonable
            if reference_price and reference_price > 0:
                dex_price = pair["price_usd"]
                # If price differs by more than 2x, it's probably a different token
                price_ratio = dex_price / reference_price if reference_price else 1
                if price_ratio > 2.0 or price_ratio < 0.5:
                    continue  # Skip this pair, likely wrong token
            
            return pair
        
        return None
    
    async def get_multiple_tokens(self, symbols: list[str]) -> dict[str, Optional[dict]]:
        """
        Get best DEX prices for multiple tokens
        Returns dict: {symbol: pair_info or None}
        """
        results = {}
        for symbol in symbols:
            results[symbol] = await self.get_best_dex_price(symbol)
        return results


# Chain name mappings for display
CHAIN_NAMES = {
    "solana": "Solana",
    "ethereum": "Ethereum",
    "bsc": "BSC",
    "arbitrum": "Arbitrum", 
    "base": "Base",
    "polygon": "Polygon",
    "avalanche": "Avalanche",
    "optimism": "Optimism",
    "fantom": "Fantom",
    "sui": "Sui",
    "ton": "TON",
    "tron": "Tron",
    "pulsechain": "PulseChain",
    "cronos": "Cronos",
    "mantle": "Mantle",
    "linea": "Linea",
    "blast": "Blast",
    "scroll": "Scroll",
    "zksync": "zkSync",
    "aptos": "Aptos",
}


def get_chain_display_name(chain_id: str) -> str:
    """Get human-readable chain name"""
    return CHAIN_NAMES.get(chain_id.lower(), chain_id.capitalize())
