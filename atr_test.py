import re
import pandas as pd
from ib_insync import IB, Future
from decimal import Decimal

# Example function to parse a raw IBKR position line (works as you have it)
def parse_ibkr_position(raw_line):
    try:
        _, rest = raw_line.split(":", 1)
        symbol_match = re.match(r"\s*(\w+)\s*\|", rest)
        symbol = symbol_match.group(1) if symbol_match else "N/A"
        pos_match = re.search(r"\|\s*([\d\.]+)\s*\|", rest)
        positions_held = float(pos_match.group(1)) if pos_match else 0.0
        cost_match = re.search(r"Avg Cost:\s*([\d\.]+)", rest)
        avg_cost = float(cost_match.group(1)) if cost_match else 0.0
        return symbol, positions_held, avg_cost
    except Exception:
        return "N/A", 0.0, 0.0

# Function to calculate ATR from a pandas DataFrame of OHLC
def calculate_atr(df, period=14):
    df['H-L'] = df['high'] - df['low']
    df['H-C'] = abs(df['high'] - df['close'].shift(1))
    df['L-C'] = abs(df['low'] - df['close'].shift(1))
    df['TR'] = df[['H-L', 'H-C', 'L-C']].max(axis=1)
    df['ATR'] = df['TR'].rolling(period).mean()
    return df['ATR'].iloc[-1]  # Return the most recent ATR value

# Connect to IB
ib = IB()
ib.connect('127.0.0.1', 7496, clientId=1)  # Adjust port/clientId if needed

# Example: take raw portfolio lines
raw_positions = [
    "DUO883664: MES | 1.0 | Avg Cost: 34398.12",
    "XYZ123456: MZL | 2.0 | Avg Cost: 700.50"
]

for line in raw_positions:
    symbol, positions_held, avg_cost = parse_ibkr_position(line)
    if symbol == "N/A":
        continue

    # Define the futures contract (example: MES future)
    contract = Future(symbol=symbol, lastTradeDateOrContractMonth='20251219', exchange='CME', currency='USD')

    # Request historical data (1 month daily bars)
    bars = ib.reqHistoricalData(
        contract,
        endDateTime='',
        durationStr='1 M',
        barSizeSetting='1 day',
        whatToShow='TRADES',
        useRTH=True,
        formatDate=1
    )

    if not bars:
        print(f"No historical data for {symbol}")
        continue

    # Convert to DataFrame for ATR calculation
    df = pd.DataFrame([{
        'open': b.open,
        'high': b.high,
        'low': b.low,
        'close': b.close,
        'volume': b.volume
    } for b in bars])

    atr_value = calculate_atr(df)
    print(f"Symbol: {symbol}, ATR: {atr_value:.2f}")
