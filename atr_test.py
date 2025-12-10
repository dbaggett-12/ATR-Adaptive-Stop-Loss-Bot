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

# --- Calculate TR and ATR using stored prior ATR from previous interval ---
def calculate_tr_and_atr(df, prior_atr):
    if len(df) < 2:
        return None, None

    # Calculate current TR
    prev_close = df['close'].iloc[-2]
    current_high = df['high'].iloc[-1]
    current_low = df['low'].iloc[-1]

    # True Range (current)
    tr1 = current_high - current_low
    tr2 = abs(current_high - prev_close)
    tr3 = abs(current_low - prev_close)
    current_tr = max(tr1, tr2, tr3)

    # ATR using the calculated prior ATR from previous 15-minute interval
    # Formula: ATR = (Prior ATR × 13 + Current TR) / 14
    current_atr = (prior_atr * 13 + current_tr) / 14

    return current_tr, current_atr
