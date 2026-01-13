"""
Listing Detector - Detects NEW futures listings on MEXC
When a new pair appears, there's often 50%+ spread with DEX
"""
import asyncio
import logging
import time
import json
import os
from typing import Set, Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

KNOWN_PAIRS_FILE = "known_futures.json"


@dataclass
class NewListing:
    symbol: str
    price: float
    detected_at: float


class ListingDetector:
    """
    Monitors MEXC for new futures listings.
    New listings often have massive spreads with DEX.
    """
    
    def __init__(self):
        self.known_pairs: Set[str] = set()
        self.new_listings: List[NewListing] = []
        self._load_known_pairs()
    
    def _load_known_pairs(self):
        """Load known pairs from disk"""
        if os.path.exists(KNOWN_PAIRS_FILE):
            try:
                with open(KNOWN_PAIRS_FILE, 'r') as f:
                    data = json.load(f)
                    self.known_pairs = set(data.get("pairs", []))
                logger.info(f"📋 Loaded {len(self.known_pairs)} known futures pairs")
            except Exception as e:
                logger.error(f"Failed to load known pairs: {e}")
    
    def _save_known_pairs(self):
        """Save known pairs to disk"""
        try:
            with open(KNOWN_PAIRS_FILE, 'w') as f:
                json.dump({"pairs": list(self.known_pairs), "updated": time.time()}, f)
        except Exception as e:
            logger.error(f"Failed to save known pairs: {e}")
    
    def detect_new_listings(self, current_pairs: Dict[str, float]) -> List[NewListing]:
        """
        Compare current pairs with known pairs.
        Returns list of newly detected pairs.
        """
        current_symbols = set(current_pairs.keys())
        new_symbols = current_symbols - self.known_pairs
        
        new_listings = []
        
        for symbol in new_symbols:
            listing = NewListing(
                symbol=symbol,
                price=current_pairs[symbol],
                detected_at=time.time()
            )
            new_listings.append(listing)
            self.known_pairs.add(symbol)
            
            logger.info(f"🆕 NEW LISTING DETECTED: {symbol} @ ${current_pairs[symbol]}")
        
        if new_listings:
            self._save_known_pairs()
            self.new_listings.extend(new_listings)
        
        return new_listings
    
    def initialize(self, current_pairs: Dict[str, float]):
        """
        Initialize with current pairs (first run - no alerts).
        Call this on startup to populate known pairs.
        """
        if not self.known_pairs:
            self.known_pairs = set(current_pairs.keys())
            self._save_known_pairs()
            logger.info(f"📋 Initialized with {len(self.known_pairs)} futures pairs")
        else:
            # Check for new ones
            new = self.detect_new_listings(current_pairs)
            if new:
                logger.info(f"🆕 Found {len(new)} new listings on startup!")
    
    def get_recent_listings(self, max_age_sec: int = 300) -> List[NewListing]:
        """Get listings detected in the last N seconds"""
        cutoff = time.time() - max_age_sec
        return [l for l in self.new_listings if l.detected_at > cutoff]
    
    def clear_old_listings(self, max_age_sec: int = 3600):
        """Remove listings older than N seconds from memory"""
        cutoff = time.time() - max_age_sec
        self.new_listings = [l for l in self.new_listings if l.detected_at > cutoff]


# Singleton
_detector: Optional[ListingDetector] = None

def get_listing_detector() -> ListingDetector:
    global _detector
    if _detector is None:
        _detector = ListingDetector()
    return _detector
