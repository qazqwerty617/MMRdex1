"""
MMRdex Bot Configuration v4.0
ULTRA INTELLIGENT Edition
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_USER_ID = int(os.getenv("TELEGRAM_USER_ID", "-1003582014728"))
TELEGRAM_TOPIC_ID = int(os.getenv("TELEGRAM_TOPIC_ID", "11")) or None

# ============================================================
# ARBITRAGE SETTINGS - HIGH SPREAD ONLY
# ============================================================
MIN_SPREAD_PERCENT = 10.0   # Minimum 2% spread for more signals
MAX_SPREAD_PERCENT = 1000.0 # Effectively unlimited (was 50.0)
MIN_LIQUIDITY_USD = 50_000      # $50k liquidity
MIN_VOLUME_24H_USD = 30_000     # $30k volume
MIN_FDV_USD = 50_000            # No micro-caps: $50k
MIN_TXNS_24H = 100              # Minimum activity: 100 trades

# ============================================================
# TRADING FEES (Futures Only Strategy)
# ============================================================
MEXC_TAKER_FEE = 0.0005  # 0.05% taker fee
SLIPPAGE_ESTIMATE = 0.005  # 0.5% estimated slippage
# DEX fees irrelevant for lead-lag (we don't swap there)
TOTAL_FEES_PERCENT = (MEXC_TAKER_FEE * 2 + SLIPPAGE_ESTIMATE) * 100

# ============================================================
# INTELLIGENCE THRESHOLDS - NEW v4.0
# ============================================================
# Minimum quality score to send signal (0-10 scale)
MIN_QUALITY_SCORE = 0.1  # ULTRA LOW - allow all signals

# Minimum token win rate to signal (0-1 scale)
MIN_WIN_RATE = 0.01  # ULTRA LOW - 1%

# Minimum entry timing quality (0-10 scale)
MIN_ENTRY_QUALITY = 0.1  # ULTRA LOW - allow all entries

# Max MEXC movement before entry is "too late" (%)
MAX_ENTRY_MOVEMENT = 3.0

# Minimum momentum strength to require alignment
MIN_MOMENTUM_STRENGTH = 0.5

# ============================================================
# CONVERGENCE SETTINGS
# ============================================================
# How to score convergence speed
EXCELLENT_CONVERGENCE_SEC = 300   # <5min = Excellent
POOR_CONVERGENCE_SEC = 3600       # >1h = Poor

# Minimum convergence rate to not blacklist token
MIN_CONVERGENCE_RATE = 0.20  # 20%

# ============================================================
# MAJOR TOKENS
# ============================================================
MAJOR_TOKENS = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE", "DOT", "MATIC", "SHIB",
    "AVAX", "LINK", "UNI", "ATOM", "LTC", "ETC", "XLM", "ALGO", "VET", "FIL",
    "NEAR", "APT", "OP", "ARB", "INJ", "SUI", "SEI", "TIA", "JUP", "WIF",
    "BONK", "PEPE", "FLOKI", "MEME", "ORDI", "STX", "IMX", "RUNE", "FTM",
    "DASH", "FTT", "YFI", "MKR", "AAVE", "COMP", "SNX", "BCH", "ETC", "LTC"
}

MAJOR_TOKEN_MAX_SPREAD = 50.0
MAJOR_TOKEN_PRICE_RATIO_MIN = 0.50
MAJOR_TOKEN_PRICE_RATIO_MAX = 2.00
ALTCOIN_PRICE_RATIO_MIN = 0.50
ALTCOIN_PRICE_RATIO_MAX = 2.00

# ============================================================
# TOKEN BLACKLIST
# ============================================================
TOKEN_BLACKLIST = {
    "SENTIS",
    # Wrapped versions
    "WETH", "WBTC", "WBNB", "WSOL", "WMATIC", "WAVAX",
    # Stablecoins
    "USDT", "USDC", "DAI", "BUSD", "TUSD", "USDP", "FRAX",
    # Common fakes
    "ETH2", "BTC2", "SOL2",
}

# ============================================================
# CACHE & FILES
# ============================================================
PAIRS_CACHE_FILE = "known_pairs.json"

# ============================================================
# TIMINGS
# ============================================================
SIGNAL_COOLDOWN_SEC = 3  # Delay between sending signals
SPREAD_CLOSURE_THRESHOLD = 2.0  # Spread closed when < 2%

# Statistics thresholds (PnL direction)
WIN_THRESHOLD = 3.5   # Win if price moved +0.5% in direction
LOSE_THRESHOLD = -3.5 # Lose if moved -0.5% against

# Scan Intervals - TURBO SPEED
SCAN_INTERVAL_SEC = 0.5  # 0.5 second scan interval (MAX SPEED)
SPREAD_CHECK_INTERVAL_SEC = 5  # NOW FASTER: Check every 5s instead of 20s!
DEPOSIT_STATUS_INTERVAL_SEC = 0  # Disabled

# Funding rate refresh (seconds)
FUNDING_REFRESH_SEC = 300  # Every 5 minutes

# ============================================================
# API LIMITS
# ============================================================
DEXSCREENER_BATCH_SIZE = 50  # Increased batch size for speed
DEXSCREENER_RATE_LIMIT = 300
DEXSCREENER_DELAY_MS = 100

# ============================================================
# DATABASE
# ============================================================
DATABASE_PATH = "mmrdex.db"

# ============================================================
# LOGGING
# ============================================================
LOG_LEVEL = "DEBUG"
