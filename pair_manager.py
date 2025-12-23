"""
Pair Manager v2.0
Enhanced discovery with token validation and fake detection.
"""
import json
import os
import asyncio
import logging
import random
import time
from typing import Optional, Dict, List
from config import (
    PAIRS_CACHE_FILE, MIN_LIQUIDITY_USD, MIN_VOLUME_24H_USD, 
    TOKEN_BLACKLIST, MAJOR_TOKENS
)
from dexscreener_client import DexScreenerClient
from token_validator import get_validator

logger = logging.getLogger(__name__)


# Chains where specific tokens are NATIVE and should be found
NATIVE_CHAINS = {
    "SOL": "solana",
    "ETH": "ethereum",
    "BNB": "bsc",
    "MATIC": "polygon",
    "AVAX": "avalanche",
    "FTM": "fantom",
    "ARB": "arbitrum",
    "OP": "optimism",
    "BASE": "base",
}


class PairManager:
    """
    Enhanced pair discovery with validation.
    Caches verified pair addresses for fast batch scanning.
    """
    
    def __init__(self, dexscreener: DexScreenerClient):
        self.dexscreener = dexscreener
        self.validator = get_validator()
        self.known_pairs: Dict[str, dict] = {}  # {symbol: {chain, address, dex, verified, updated}}
        self.blacklisted_pairs: set = set()  # Pairs that failed validation
        self.load_cache()
    
    def load_cache(self):
        """Load known pairs from disk"""
        if os.path.exists(PAIRS_CACHE_FILE):
            try:
                with open(PAIRS_CACHE_FILE, 'r') as f:
                    data = json.load(f)
                    # Migration: ensure new fields exist
                    for symbol, pair_data in data.items():
                        if "verified" not in pair_data:
                            pair_data["verified"] = False
                        if "updated" not in pair_data:
                            pair_data["updated"] = 0
                    self.known_pairs = data
                logger.info(f"✅ Loaded {len(self.known_pairs)} known pairs from cache")
            except Exception as e:
                logger.error(f"Failed to load pairs cache: {e}")
    
    def save_cache(self):
        """Save known pairs to disk"""
        try:
            with open(PAIRS_CACHE_FILE, 'w') as f:
                json.dump(self.known_pairs, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save pairs cache: {e}")
    
    def _should_skip_token(self, symbol: str) -> bool:
        """Check if token should be skipped entirely"""
        return symbol in TOKEN_BLACKLIST
    
    def _validate_pair(self, symbol: str, pair: dict, mexc_price: float) -> bool:
        """
        Validate that a pair is legitimate.
        Returns True if pair passes all checks.
        """
        if not pair:
            return False
        
        chain = pair.get("chain", "").lower()
        dex_price = pair.get("price_usd", 0)
        liquidity = pair.get("liquidity_usd", 0)
        volume = pair.get("volume_24h", 0)
        
        # Basic checks
        if dex_price <= 0 or liquidity < MIN_LIQUIDITY_USD:
            return False
        
        # Use TokenValidator for comprehensive checks
        is_valid, reason = self.validator.validate_token(
            symbol=symbol,
            chain=chain,
            dex_price=dex_price,
            mexc_price=mexc_price,
            spread_percent=abs((dex_price - mexc_price) / mexc_price * 100) if mexc_price > 0 else 0
        )
        
        if not is_valid:
            logger.debug(f"Pair validation failed for {symbol}: {reason}")
            return False
        
        # For major tokens, be extra strict
        if symbol in MAJOR_TOKENS:
            # Check if chain matches expected native chain
            expected_chain = NATIVE_CHAINS.get(symbol)
            if expected_chain and chain != expected_chain:
                # Major token on wrong chain - likely fake
                logger.debug(f"Major token {symbol} on wrong chain: {chain} (expected {expected_chain})")
                return False
            
            # Require higher liquidity for major tokens
            if liquidity < 100_000:  # $100k minimum for major tokens
                return False
        
        # Volume/Liquidity ratio check
        if liquidity > 0:
            ratio = volume / liquidity
            if ratio < 0.02:  # Less than 2% daily turnover = dead pool
                return False
        
        return True
    
    async def discover_pairs(self, tokens: Dict[str, float]):
        """
        Background task to find and validate pairs for tokens.
        Enhanced with validation to prevent fake pairs.
        """
        unknown_tokens = [
            t for t in tokens.keys() 
            if t not in self.known_pairs 
            and t not in self.blacklisted_pairs
            and not self._should_skip_token(t)
        ]
        
        if not unknown_tokens:
            return
            
        logger.info(f"🔍 Discovering {len(unknown_tokens)} new tokens...")
        
        # Shuffle to distribute load
        random.shuffle(unknown_tokens)
        
        count = 0
        skipped = 0
        
        for symbol in unknown_tokens:
            try:
                mexc_price = tokens.get(symbol, 0)
                if mexc_price <= 0:
                    continue
                
                # Search for best pair
                pair = await self.dexscreener.get_best_dex_price(
                    symbol,
                    min_liquidity=MIN_LIQUIDITY_USD,
                    min_volume=MIN_VOLUME_24H_USD,
                    reference_price=mexc_price
                )
                
                if pair and self._validate_pair(symbol, pair, mexc_price):
                    # Verified pair - save to cache
                    self.known_pairs[symbol] = {
                        "chain": pair["chain"],
                        "address": pair["pair_address"],
                        "dex": pair.get("dex", "unknown"),
                        "verified": True,
                        "updated": int(time.time())
                    }
                    count += 1
                    
                    logger.info(f"✅ Found: {symbol} on {pair['chain']} ({pair.get('dex', 'unknown')})")
                    
                    if count % 5 == 0:
                        self.save_cache()
                else:
                    skipped += 1
                
                # Rate limiting
                await asyncio.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Error discovering {symbol}: {e}")
                await asyncio.sleep(1)
        
        self.save_cache()
        logger.info(f"🔍 Discovery complete: {count} new pairs, {skipped} skipped")
    
    def get_batch_candidates(self) -> Dict[str, List[str]]:
        """
        Return verified pair addresses grouped by chain.
        Only returns pairs that passed validation.
        """
        batches: Dict[str, List[str]] = {}
        
        for symbol, data in self.known_pairs.items():
            # Skip blacklisted symbols
            if self._should_skip_token(symbol):
                continue
            
            chain = data.get("chain", "")
            address = data.get("address", "")
            
            if not chain or not address:
                continue
            
            if chain not in batches:
                batches[chain] = []
            batches[chain].append(address)
        
        return batches
    
    def get_symbol_by_address(self, address: str) -> Optional[str]:
        """Reverse lookup: find symbol by pair address"""
        for symbol, data in self.known_pairs.items():
            if data.get("address") == address:
                return symbol
        return None
    
    def invalidate_pair(self, symbol: str):
        """Mark a pair as invalid (remove from cache)"""
        if symbol in self.known_pairs:
            del self.known_pairs[symbol]
            self.blacklisted_pairs.add(symbol)
            self.save_cache()
            logger.info(f"❌ Invalidated pair: {symbol}")
    
    def get_stats(self) -> dict:
        """Get discovery statistics"""
        chains = {}
        for data in self.known_pairs.values():
            chain = data.get("chain", "unknown")
            chains[chain] = chains.get(chain, 0) + 1
        
        return {
            "total_pairs": len(self.known_pairs),
            "blacklisted": len(self.blacklisted_pairs),
            "by_chain": chains
        }
