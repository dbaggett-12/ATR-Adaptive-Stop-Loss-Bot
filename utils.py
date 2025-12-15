# utils.py
import logging

# --- Contract Metadata ---
# Per-contract point values: dollar value per 1.00 displayed price move
# For contracts quoted in cents (agricultural), this is the dollar value per 1 cent move
# This ensures accurate dollar risk calculations across all contract types
CONTRACT_POINT_VALUES = {
    # Standard Index Futures
    'ES': 50.0,      # E-mini S&P 500: $50 per point
    'NQ': 20.0,      # E-mini Nasdaq: $20 per point
    'YM': 5.0,       # E-mini Dow: $5 per point
    'RTY': 50.0,     # E-mini Russell 2000: $50 per point
    
    # Micro Index Futures
    'MES': 5.0,      # Micro E-mini S&P 500: $5 per point
    'MNQ': 2.0,      # Micro E-mini Nasdaq: $2 per point
    'MYM': 0.50,     # Micro E-mini Dow: $0.50 per point
    'M2K': 5.0,      # Micro E-mini Russell 2000: $5 per point
    
    # Treasury Futures
    'ZN': 1000.0,    # 10-Year T-Note: $1000 per point
    'ZB': 1000.0,    # 30-Year T-Bond: $1000 per point
    'ZF': 1000.0,    # 5-Year T-Note: $1000 per point
    'ZT': 2000.0,    # 2-Year T-Note: $2000 per point
    
    # Micro Treasury Futures
    'MZN': 100.0,    # Micro 10-Year Yield: $100 per point (10 × $10)
    '10Y': 1000.0,   # Micro 10-Year Yield: $1000 per 1.00 point move in yield
    '2YY': 2000.0,   # Micro 2-Year Yield: $2000 per 1.00 point move in yield
    '30Y': 1000.0,   # Micro 30-Year Yield: $1000 per 1.00 point move in yield
    
    # Agricultural Futures (prices displayed in cents, e.g., 450.25 = 450.25 cents/bushel)
    # Point value = contract size (bushels/lbs) × $0.01 per cent
    # A 1.00 displayed price move = 1 cent = point_value dollars
    'ZC': 50.0,      # Corn: 5000 bushels × $0.01 = $50 per 1 cent (1.00 displayed)
    'ZS': 50.0,      # Soybeans: 5000 bushels × $0.01 = $50 per 1 cent
    'ZW': 50.0,      # Wheat: 5000 bushels × $0.01 = $50 per 1 cent
    'ZM': 100.0,     # Soybean Meal: 100 tons × $1.00 = $100 per point (quoted in $/ton)
    'ZL': 600.0,     # Soybean Oil: 60000 lbs × $0.01 = $600 per 1 cent move
    'HE': 400.0,     # Lean Hogs: 40000 lbs × $0.01 = $400 per 1 cent move
    'LE': 400.0,     # Live Cattle: 40000 lbs × $0.01 = $400 per 1 cent move
    'GF': 500.0,     # Feeder Cattle: 50000 lbs × $0.01 = $500 per 1 cent move
    
    # Micro Agricultural Futures (1/10th of standard)
    'MZC': 5.0,      # Micro Corn: 500 bushels × $0.01 = $5 per 1 cent move
    'MZS': 10.0,     # Micro Soybeans: 1000 bushels × $0.01 = $10 per 1 cent move
    'MZW': 5.0,      # Micro Wheat: 500 bushels × $0.01 = $5 per 1 cent move
    'MZM': 10.0,     # Micro Soybean Meal: 10 tons × $1.00 = $10 per point
    'MZL': 60.0,     # Micro Soybean Oil: 6000 lbs × $0.01 = $60 per 1 cent move
    
    # Metals
    'GC': 100.0,     # Gold: 100 oz × $1.00 = $100 per point
    'SI': 5000.0,    # Silver: 5000 oz × $1.00 = $5000 per point
    'HG': 25000.0,   # Copper: 25000 lbs. Price is in cents, so a 1.00 move (1 cent) is 25000 * $0.01 = $250.
    'PL': 50.0,      # Platinum: 50 oz × $1.00 = $50 per point
    
    # Micro Metals
    'MGC': 10.0,     # Micro Gold: 10 oz × $1.00 = $10 per point
    'SIL': 1000.0,   # Micro Silver: 1000 oz × $1.00 = $1000 per point
    'MHG': 2500.0,   # Micro Copper: 2500 lbs. Price is in cents, so a 1.00 move (1 cent) is 2500 * $0.01 = $25.
    
    # Energy
    'CL': 1000.0,    # Crude Oil: 1000 barrels × $1.00 = $1000 per point
    'NG': 10000.0,   # Natural Gas: 10000 MMBtu × $1.00 = $10000 per point
    'RB': 420.0,     # RBOB Gasoline: 42000 gallons × $0.01 = $420 per 1 cent
    'HO': 420.0,     # Heating Oil: 42000 gallons × $0.01 = $420 per 1 cent
    
    # Micro Energy
    'MCL': 100.0,    # Micro Crude Oil: 100 barrels × $1.00 = $100 per point
    'MNG': 1000.0,   # Micro Natural Gas: 1000 MMBtu × $1.00 = $1000 per point
    
    # Currency Futures
    'EUR': 125000.0, # Euro FX: 125000 EUR × $1.00 = $125000 per point
    '6E': 125000.0,  # Euro FX (CME symbol): same as EUR
    '6J': 12500000.0,# Japanese Yen: 12500000 JPY × $0.000001 = $12.50 per tick
    '6B': 62500.0,   # British Pound: 62500 GBP × $1.00 = $62500 per point
    '6A': 100000.0,  # Australian Dollar: 100000 AUD × $1.00 = $100000 per point
    '6C': 100000.0,  # Canadian Dollar: 100000 CAD × $1.00 = $100000 per point
    
    # Micro Currency Futures
    'M6E': 12500.0,  # Micro Euro: 12500 EUR × $1.00 = $12500 per point
}


def get_point_value(symbol, contract_details, multiplier):
    """
    Get the dollar value per 1.00 price move for a contract.
    
    Uses explicit metadata first, then falls back to deriving from contract details.
    
    Args:
        symbol: The contract symbol (e.g., 'ES', 'MZN', 'ZC')
        contract_details: Dict with priceMagnifier, mdSizeMultiplier, etc.
        multiplier: The contract multiplier from IBKR
    
    Returns:
        float: Dollar value per 1.00 price move
    """
    # First, check the explicit metadata dictionary
    if symbol in CONTRACT_POINT_VALUES:
        return CONTRACT_POINT_VALUES[symbol]
    
    # Fallback logic remains the same
    price_magnifier = contract_details.get('priceMagnifier', 1)
    md_size_multiplier = contract_details.get('mdSizeMultiplier')
    
    if price_magnifier > 1 and md_size_multiplier is not None:
        point_value = float(md_size_multiplier) / price_magnifier
        logging.warning(f"Unknown contract {symbol}. Derived point value: ${point_value:.2f} (mdSizeMultiplier={md_size_multiplier}, priceMagnifier={price_magnifier}). Consider adding to CONTRACT_POINT_VALUES.")
        return point_value
    
    elif md_size_multiplier is not None and md_size_multiplier > 1:
        point_value = float(md_size_multiplier)
        logging.warning(f"Unknown contract {symbol}. Using mdSizeMultiplier as point value: ${point_value:.2f}. Consider adding to CONTRACT_POINT_VALUES.")
        return point_value
    
    else:
        point_value = multiplier if multiplier > 0 else 1.0
        logging.warning(f"Unknown contract {symbol}. Using multiplier as point value: ${point_value:.2f}. Consider adding to CONTRACT_POINT_VALUES.")
        return point_value