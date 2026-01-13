"""
Pump/Dump Detector - Detects rapid price movements on MEXC
When price moves fast, spread with DEX appears
"""
import logging
import time
from typing import Dict, List, Optional, Literal
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class PricePoint:
    price: float
    timestamp: float


@dataclass  
class PumpEvent:
    symbol: str
    direction: Literal["PUMP", "DUMP"]
    change_percent: float
    current_price: float
    start_price: float
    duration_sec: float
    detected_at: float = field(default_factory=time.time)


class PumpDetector:
    """
    Monitors MEXC prices for rapid movements (pumps/dumps).
    When detected, signals should be checked against DEX.
    """
    
    def __init__(
        self,
        pump_threshold: float = 5.0,  # 5% move = pump
        dump_threshold: float = -5.0,  # -5% move = dump
        lookback_sec: int = 300,  # Look at last 5 minutes
        min_duration_sec: int = 10  # Ignore if too fast (likely glitch)
    ):
        self.pump_threshold = pump_threshold
        self.dump_threshold = dump_threshold
        self.lookback_sec = lookback_sec
        self.min_duration_sec = min_duration_sec
        
        # Price history: {symbol: [PricePoint, ...]}
        self._price_history: Dict[str, List[PricePoint]] = defaultdict(list)
        
        # Recent events (to avoid duplicate alerts)
        self._recent_events: Dict[str, float] = {}  # {symbol_direction: timestamp}
        self._event_cooldown = 300  # 5 min cooldown per symbol+direction
        
        # Detected events
        self.events: List[PumpEvent] = []
    
    def record_price(self, symbol: str, price: float):
        """Record a price point for a symbol"""
        now = time.time()
        self._price_history[symbol].append(PricePoint(price, now))
        
        # Cleanup old data
        cutoff = now - self.lookback_sec * 2
        self._price_history[symbol] = [
            p for p in self._price_history[symbol] 
            if p.timestamp > cutoff
        ]
    
    def record_prices(self, prices: Dict[str, float]):
        """Record prices for multiple symbols"""
        for symbol, price in prices.items():
            self.record_price(symbol, price)
    
    def _is_on_cooldown(self, symbol: str, direction: str) -> bool:
        key = f"{symbol}_{direction}"
        last_time = self._recent_events.get(key, 0)
        return (time.time() - last_time) < self._event_cooldown
    
    def _set_cooldown(self, symbol: str, direction: str):
        key = f"{symbol}_{direction}"
        self._recent_events[key] = time.time()
    
    def detect_pumps(self, current_prices: Dict[str, float]) -> List[PumpEvent]:
        """
        Analyze price history to detect pumps/dumps.
        Returns list of new pump events.
        """
        events = []
        now = time.time()
        
        for symbol, price in current_prices.items():
            history = self._price_history.get(symbol, [])
            if len(history) < 2:
                continue
            
            # Find price from lookback period ago
            lookback_time = now - self.lookback_sec
            old_prices = [p for p in history if p.timestamp < lookback_time]
            
            if not old_prices:
                # Use oldest available
                old_price = history[0]
            else:
                old_price = old_prices[-1]  # Most recent "old" price
            
            # Calculate change
            if old_price.price <= 0:
                continue
                
            change = ((price - old_price.price) / old_price.price) * 100
            duration = now - old_price.timestamp
            
            # Check if significant
            if change >= self.pump_threshold:
                direction = "PUMP"
            elif change <= self.dump_threshold:
                direction = "DUMP"
            else:
                continue
            
            # Skip if duration too short (glitch)
            if duration < self.min_duration_sec:
                continue
            
            # Skip if on cooldown
            if self._is_on_cooldown(symbol, direction):
                continue
            
            # Create event
            event = PumpEvent(
                symbol=symbol,
                direction=direction,
                change_percent=change,
                current_price=price,
                start_price=old_price.price,
                duration_sec=duration
            )
            
            events.append(event)
            self.events.append(event)
            self._set_cooldown(symbol, direction)
            
            logger.info(
                f"💥 {direction} DETECTED: {symbol} "
                f"{change:+.1f}% in {duration:.0f}s "
                f"(${old_price.price:.6f} → ${price:.6f})"
            )
        
        return events
    
    def get_recent_events(self, max_age_sec: int = 60) -> List[PumpEvent]:
        """Get events from the last N seconds"""
        cutoff = time.time() - max_age_sec
        return [e for e in self.events if e.detected_at > cutoff]
    
    def cleanup_old_events(self, max_age_sec: int = 3600):
        """Remove old events from memory"""
        cutoff = time.time() - max_age_sec
        self.events = [e for e in self.events if e.detected_at > cutoff]


# Singleton
_detector: Optional[PumpDetector] = None

def get_pump_detector() -> PumpDetector:
    global _detector
    if _detector is None:
        _detector = PumpDetector()
    return _detector
