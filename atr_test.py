import re
import pandas as pd
from ib_insync import IB, Future

# --- Parse IBKR portfolio line ---
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

# --- Calculate TR and ATR using prior ATR ---
def calculate_tr_and_atr(df, prior_atr=7.25):
    if len(df) < 2:
        return None, None

    prev_close = df['close'].iloc[-2]
    current_high = df['high'].iloc[-1]
    current_low = df['low'].iloc[-1]

    # True Range
    tr1 = current_high - current_low
    tr2 = abs(current_high - prev_close)
    tr3 = abs(current_low - prev_close)
    current_tr = max(tr1, tr2, tr3)

    # ATR with prior ATR
    current_atr = (prior_atr * 13 + current_tr) / 14

    return current_tr, current_atr

# --- Connect to IBKR ---
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)

# --- Example portfolio lines ---
raw_positions = [
    "DUO883664: MES | 1.0 | Avg Cost: 34398.12",
    "XYZ123456: MZL | 2.0 | Avg Cost: 700.50"
]

for line in raw_positions:
    symbol, positions_held, avg_cost = parse_ibkr_position(line)
    if symbol == "N/A":
        continue

    # Define contract
    contract = Future(symbol=symbol, lastTradeDateOrContractMonth='20251219', exchange='CME', currency='USD')

    # Get historical bars (at least 2 days)
    bars = ib.reqHistoricalData(
        contract,
        endDateTime='',
        durationStr='3 D',  # enough for previous + current bar
        barSizeSetting='1 day',
        whatToShow='TRADES',
        useRTH=True,
        formatDate=1
    )

    if not bars or len(bars) < 2:
        print(f"Not enough historical data for {symbol}")
        continue

    df = pd.DataFrame([{
        'open': b.open,
        'high': b.high,
        'low': b.low,
        'close': b.close,
        'volume': b.volume
    } for b in bars])

    tr, atr = calculate_tr_and_atr(df, prior_atr=7.25)
    print(f"Symbol: {symbol}, Current TR: {tr:.2f}, ATR(14) with prior 7.25: {atr:.2f}")
