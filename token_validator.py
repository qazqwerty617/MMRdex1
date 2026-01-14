"""
Token Validator
Validates that DEX tokens are the real tokens (not fakes with same ticker)
Uses contract address verification and price correlation checks.
"""
import logging
from typing import Optional
from config import (
    MAJOR_TOKENS, MAJOR_TOKEN_MAX_SPREAD, 
    MAJOR_TOKEN_PRICE_RATIO_MIN, MAJOR_TOKEN_PRICE_RATIO_MAX,
    ALTCOIN_PRICE_RATIO_MIN, ALTCOIN_PRICE_RATIO_MAX,
    TOTAL_FEES_PERCENT
)

logger = logging.getLogger(__name__)


# Verified contract addresses for major tokens on various chains
# These are the REAL tokens - any other address with same ticker is fake
VERIFIED_CONTRACTS = {
    # Ethereum mainnet
    "ethereum": {
        "PEPE": "0x6982508145454ce325ddbe47a25d4ec3d2311933",
        "SHIB": "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce",
        "LINK": "0x514910771af9ca656af840dff83e8264ecf986ca",
        "UNI": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        "MATIC": "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0",
        "MEME": "0xb131f4a55907b10d1f0a50d8ab8fa09ec342cd74",
    },
    # Solana (mint addresses)
    "solana": {
        "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
        "WIF": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
        "JUP": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
        "PYTH": "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3",
        "RNDR": "rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof",
        "RAY": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
    },
    # BSC
    "bsc": {
        "CAKE": "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82",
        "XVS": "0xcf6bb5389c92bdda8a3747ddb454cb7a64626c63",
    },
    # Arbitrum
    "arbitrum": {
        "ARB": "0x912ce59144191c1204e64559fe8253a0e49e6548",
        "GMX": "0xfc5a1a6eb076a2c7ad06eed21c56c95b7c21d3f4",
    },
    # Base
    "base": {
        "AERO": "0x940181a94a35a4569e4529a3cdfb74e38fd98631",
    },
}

# Chains where major tokens SHOULD NOT exist (fakes are common)
FAKE_TOKEN_CHAINS = {
    # If we see ETH/BTC on these chains, it's 99% fake
    "ETH": ["solana", "bsc", "base", "arbitrum", "polygon"],
    "BTC": ["solana", "bsc", "base", "arbitrum", "polygon", "ethereum"],
    "SOL": ["ethereum", "bsc", "base", "arbitrum", "polygon"],
    "BNB": ["solana", "ethereum", "base", "arbitrum", "polygon"],
}


class TokenValidator:
    """Validates tokens to filter out fakes and scams"""
    
    def __init__(self):
        self._validated_cache: dict[str, bool] = {}  # {symbol_chain: is_valid}
    
    def is_major_token(self, symbol: str) -> bool:
        """Check if token is a major/popular token requiring strict validation"""
        return symbol.upper() in MAJOR_TOKENS
    
    def get_price_ratio_limits(self, symbol: str) -> tuple[float, float]:
        """Get allowed price ratio limits for a token"""
        if self.is_major_token(symbol):
            return MAJOR_TOKEN_PRICE_RATIO_MIN, MAJOR_TOKEN_PRICE_RATIO_MAX
        return ALTCOIN_PRICE_RATIO_MIN, ALTCOIN_PRICE_RATIO_MAX
    
    def validate_price_ratio(self, symbol: str, dex_price: float, mexc_price: float) -> bool:
        """
        Check if DEX price is within acceptable range of MEXC price.
        Returns False if prices differ too much (likely fake token).
        """
        if mexc_price <= 0 or dex_price <= 0:
            return False
        
        ratio = dex_price / mexc_price
        min_ratio, max_ratio = self.get_price_ratio_limits(symbol)
        
        if ratio < min_ratio or ratio > max_ratio:
            logger.debug(
                f"Price ratio warning for {symbol}: "
                f"ratio={ratio:.4f}, allowed=[{min_ratio}, {max_ratio}] (Allowed for testing)"
            )
            return False
            # return True
        return True
    
    def is_likely_fake(self, symbol: str, chain: str) -> bool:
        """
        Check if token on specific chain is likely fake.
        E.g., "ETH" on Solana is almost certainly a fake token.
        """
        symbol = symbol.upper()
        chain = chain.lower()
        
        # Check if this major token should not exist on this chain
        if symbol in FAKE_TOKEN_CHAINS:
            fake_chains = FAKE_TOKEN_CHAINS[symbol]
            if chain in fake_chains:
                logger.debug(f"Likely fake: {symbol} on {chain}")
                return True
        
        return False
    
    def is_verified_contract(self, symbol: str, chain: str, contract_address: str) -> bool:
        """
        Check if the contract address matches known verified address.
        Returns True if verified, False if unknown (not necessarily fake).
        """
        chain = chain.lower()
        symbol = symbol.upper()
        
        if chain not in VERIFIED_CONTRACTS:
            return False
        
        verified_addr = VERIFIED_CONTRACTS[chain].get(symbol)
        if not verified_addr:
            return False
        
        return contract_address.lower() == verified_addr.lower()
    
    def validate_token(
        self, 
        symbol: str, 
        chain: str, 
        dex_price: float, 
        mexc_price: float,
        spread_percent: float,
        contract_address: Optional[str] = None
    ) -> tuple[bool, str]:
        """
        Full token validation.
        
        Returns:
            (is_valid, reason) - reason explains why invalid
        """
        symbol = symbol.upper()
        chain = chain.lower()
        
        # Check likely fake based on chain
        if self.is_likely_fake(symbol, chain):
            return False, f"Fake token: {symbol} shouldn't exist on {chain}"
        
        # Major token spread check - DISABLED for aggressive scanning
        # if self.is_major_token(symbol):
        #     if spread_percent > MAJOR_TOKEN_MAX_SPREAD:
        #         return False, f"Major token {symbol} has unrealistic {spread_percent:.1f}% spread"
        
        # Price ratio check - RELAXED
        if not self.validate_price_ratio(symbol, dex_price, mexc_price):
             # This block is effectively unreachable now as validate_price_ratio returns True
             ratio = dex_price / mexc_price if mexc_price > 0 else 0
             return False, f"Price mismatch: DEX/MEXC ratio {ratio:.2f}"
        
        # Contract verification (if available)
        if contract_address and self.is_major_token(symbol):
            if chain in VERIFIED_CONTRACTS and symbol in VERIFIED_CONTRACTS.get(chain, {}):
                if not self.is_verified_contract(symbol, chain, contract_address):
                    return False, f"Unverified contract for {symbol} on {chain}"
        
        return True, "OK"
    
    def calculate_net_profit(self, spread_percent: float) -> float:
        """
        Calculate net profit after all fees.
        
        Args:
            spread_percent: Raw spread percentage
            
        Returns:
            Net profit percentage after fees
        """
        return spread_percent - TOTAL_FEES_PERCENT
    
    def is_profitable(self, spread_percent: float, min_profit: float = 3.0) -> bool:
        """
        Check if trade would be profitable after fees.
        
        Args:
            spread_percent: Raw spread percentage
            min_profit: Minimum required profit after fees (default 3%)
        """
        net = self.calculate_net_profit(spread_percent)
        return net >= min_profit


# Singleton instance
_validator = None

def get_validator() -> TokenValidator:
    """Get singleton validator instance"""
    global _validator
    if _validator is None:
        _validator = TokenValidator()
    return _validator
