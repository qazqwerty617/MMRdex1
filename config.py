"""
MMRdex Bot Configuration
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_USER_ID = int(os.getenv("TELEGRAM_USER_ID", "0"))
TELEGRAM_TOPIC_ID = int(os.getenv("TELEGRAM_TOPIC_ID", "0")) or None  # ID топика (для форумов)

# Arbitrage Settings - ANTI-SCAM ELITE MODE
MIN_SPREAD_PERCENT = 4.0   # От 4%
MAX_SPREAD_PERCENT = 30.0  # Выше 30% = Скам/Глюк
MIN_LIQUIDITY_USD = 150_000  # Минимум $150k ликвидности
MIN_VOLUME_24H_USD = 150_000 # Минимум $150k объема
MIN_FDV_USD = 3_000_000      # Минимум $3M капитализации (убирает 99% мусора)
MIN_TXNS_24H = 300           # Минимум 300 сделок за сутки (защита от накрутки)

# Trading Fees (Futures Only Strategy)
MEXC_TAKER_FEE = 0.0005  # 0.05% taker fee
SLIPPAGE_ESTIMATE = 0.005  # 0.5% estimated slippage for entry/exit
# DEX fees are irrelevant for lead-lag trading (we don't swap there)
TOTAL_FEES_PERCENT = (MEXC_TAKER_FEE * 2 + SLIPPAGE_ESTIMATE) * 100  # Entry + Exit fees + Slippage

# Major tokens - require strict validation (fake versions exist on other chains)
MAJOR_TOKENS = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE", "DOT", "MATIC", "SHIB",
    "AVAX", "LINK", "UNI", "ATOM", "LTC", "ETC", "XLM", "ALGO", "VET", "FIL",
    "NEAR", "APT", "OP", "ARB", "INJ", "SUI", "SEI", "TIA", "JUP", "WIF",
    "BONK", "PEPE", "FLOKI", "MEME", "ORDI", "STX", "IMX", "RUNE", "FTM"
}

# For major tokens, max allowed spread (higher = definitely fake token)
MAJOR_TOKEN_MAX_SPREAD = 3.0  # Major tokens should NOT have >3% spreads on legit pools

# Price ratio limits for validation
MAJOR_TOKEN_PRICE_RATIO_MIN = 0.97  # Major token DEX price must be within 3% of MEXC
MAJOR_TOKEN_PRICE_RATIO_MAX = 1.03
ALTCOIN_PRICE_RATIO_MIN = 0.7  # Altcoins can have up to 30% difference
ALTCOIN_PRICE_RATIO_MAX = 1.3

# Token Blacklist (never signal these - known fakes/wrapped)
TOKEN_BLACKLIST = {
    "SENTIS",
    # Wrapped versions that cause false signals
    "WETH", "WBTC", "WBNB", "WSOL", "WMATIC", "WAVAX",
    # Stablecoins
    "USDT", "USDC", "DAI", "BUSD", "TUSD", "USDP", "FRAX",
    # Common fakes
    "ETH2", "BTC2", "SOL2",
}

# Cache Settings
PAIRS_CACHE_FILE = "known_pairs.json"  # Файл с кешем адресов пар

# Signal Cooldown (avoid spam)
SIGNAL_COOLDOWN_SEC = 3  # Задержка между отправкой сигналов

# Spread Closure Settings
SPREAD_CLOSURE_THRESHOLD = 2.0  # Спред считается закрытым при < 2%

# Statistics Thresholds
WIN_THRESHOLD = 3.5  # Win если цена пошла в нужную сторону на > +0.5%
LOSE_THRESHOLD = -3.5  # Lose если цена пошла против на > -0.5%

# Scan Intervals (seconds) - TURBO SPEED
SCAN_INTERVAL_SEC = 1  # TURBO: 1 second scan interval
SPREAD_CHECK_INTERVAL_SEC = 20  # Faster spread closure check
DEPOSIT_STATUS_INTERVAL_SEC = 0  # Disabled (API requires auth)

# API Rate Limits
DEXSCREENER_BATCH_SIZE = 30  # Макс. токенов за запрос к DexScreener
DEXSCREENER_RATE_LIMIT = 300  # Запросов в минуту
DEXSCREENER_DELAY_MS = 100  # Задержка между запросами к DexScreener (мс)

# Database
DATABASE_PATH = "mmrdex.db"

# Logging
LOG_LEVEL = "INFO"
