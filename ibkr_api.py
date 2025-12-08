from ib_insync import IB
from time import sleep
import re

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


def fetch_positions():
    """
    Connects to IBKR and returns a list of dicts:
    [
        {
            'position': 'MES',
            'symbol': 'MES',
            'positions_held': 1.0,
            'avg_cost': 34398.12,
            'current_price': 34400.0,
            'monthly_pl_percent': 0.06,
            'raw_line': '...'
        }, ...
    ]
    """
    ib = IB()
    results = []

    try:
        ib.connect('127.0.0.1', 7497, clientId=1)
        positions = ib.positions()
        for pos in positions:
            raw_line = f"{pos.account}: {pos.contract.symbol} | {pos.position} | Avg Cost: {pos.avgCost}"
            symbol, held, avg = parse_ibkr_position(raw_line)

            ticker = ib.reqMktData(pos.contract, "", False, False)
            sleep(0.1)
            current_price = ticker.last if ticker.last else 0.0
            monthly_pl_pct = ((current_price - avg) / avg) * 100 if avg != 0 else 0.0

            results.append({
                'position': pos.contract.symbol,
                'symbol': symbol,
                'positions_held': held,
                'avg_cost': avg,
                'current_price': current_price,
                'monthly_pl_percent': monthly_pl_pct,
                'raw_line': raw_line
            })
        ib.disconnect()
    except Exception as e:
        print(f"Error connecting to IBKR: {e}")

    return results