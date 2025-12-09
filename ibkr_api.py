from ib_insync import IB
from time import sleep
import re
import random

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
    Connects to IBKR and returns a tuple: (results_list, connection_success)
    results_list: list of dicts with position data
    connection_success: True if successfully connected and retrieved data, False otherwise
    """
    ib = IB()
    results = []
    connection_success = False
    
    # Try multiple clientIds in case one is still in use
    max_retries = 5
    for attempt in range(max_retries):
        client_id = random.randint(100, 999)
        
        try:
            print(f"Attempting to connect with clientId {client_id}...")
            ib.connect('127.0.0.1', 7497, clientId=client_id)
            print(f"Successfully connected with clientId {client_id}")
            
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
            connection_success = True
            break  # Success, exit the retry loop
            
        except Exception as e:
            error_msg = str(e)
            print(f"Attempt {attempt + 1} failed with clientId {client_id}: {error_msg}")
            
            # Check if it's a clientId error
            if "client id" in error_msg.lower() or "clientid" in error_msg.lower():
                if attempt < max_retries - 1:
                    print("ClientId conflict detected, retrying with a different clientId...")
                    sleep(0.5)  # Brief pause before retry
                    continue
                else:
                    print("Max retries reached. Unable to connect.")
            else:
                # For non-clientId errors, don't retry
                print(f"Error connecting to IBKR: {e}")
                break
                
        finally:
            # Always disconnect, even if there was an error
            if ib.isConnected():
                ib.disconnect()

    return results, connection_success
