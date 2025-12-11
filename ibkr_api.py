from ib_insync import IB, util, Contract, StopOrder
from time import sleep
import random
import math
from datetime import datetime
import logging
import pytz

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
            
            # Qualify contracts first to fill in missing details like exchange
            contracts = [pos.contract for pos in positions]
            ib.qualifyContracts(*contracts)
            
            # Request delayed market data (type 3) if live data is not available
            # Type 1 = Live, Type 2 = Frozen, Type 3 = Delayed, Type 4 = Delayed-Frozen
            ib.reqMarketDataType(3)
            
            # Request market data for all positions
            tickers = {}
            for pos in positions:
                try:
                    ticker = ib.reqMktData(pos.contract, "", False, False)
                    tickers[pos.contract.symbol] = (pos, ticker)
                except Exception as e:
                    print(f"Warning: Could not request market data for {pos.contract.symbol}: {e}")
                    tickers[pos.contract.symbol] = (pos, None)
            
            # Wait for market data to populate. A simple, longer sleep is more reliable
            # than a complex loop, as it gives all tickers a chance to update without
            # exiting prematurely.
            print("Waiting for market data to populate...")
            ib.sleep(5) # Process events for 5 seconds to allow all data to arrive.
            print("Finished waiting for market data.")
            
            for symbol, (pos, ticker) in tickers.items():
                # Get values directly from IBKR position object
                positions_held = float(pos.position)
                raw_avg_cost = float(pos.avgCost)
                
                # Get security type and multiplier
                sec_type = pos.contract.secType if hasattr(pos.contract, 'secType') else 'STK'
                
                # Get contract multiplier (for futures, this is the contract size)
                multiplier = 1.0
                if hasattr(pos.contract, 'multiplier') and pos.contract.multiplier:
                    try:
                        multiplier = float(pos.contract.multiplier)
                    except (ValueError, TypeError):
                        multiplier = 1.0
                
                # Handle avgCost based on security type
                # For FUTURES (FUT): IBKR's avgCost includes the multiplier, so divide to get entry price
                # For STOCKS (STK) and ETFs: avgCost IS the actual entry price, don't divide
                if sec_type == 'FUT' and multiplier > 1:
                    avg_cost = raw_avg_cost / multiplier
                else:
                    avg_cost = raw_avg_cost
                
                print(f"DEBUG {symbol}: secType={sec_type}, raw_avgCost={raw_avg_cost}, multiplier={multiplier}, avg_cost={avg_cost}")

                # Try to get the best available price
                current_price = 0.0
                
                if ticker is not None:
                    # Check last price first
                    if ticker.last and not math.isnan(ticker.last) and ticker.last > 0:
                        current_price = ticker.last
                    # Fall back to close price
                    elif ticker.close and not math.isnan(ticker.close) and ticker.close > 0:
                        current_price = ticker.close
                    # Fall back to bid/ask midpoint
                    elif ticker.bid and ticker.ask and not math.isnan(ticker.bid) and not math.isnan(ticker.ask):
                        current_price = (ticker.bid + ticker.ask) / 2
                    # Last resort: use market price from ticker
                    elif ticker.marketPrice() and not math.isnan(ticker.marketPrice()):
                        current_price = ticker.marketPrice()

                # Calculate market values and P/L
                # For futures: cost_basis and market_value should include multiplier
                cost_basis = avg_cost * multiplier * abs(positions_held)
                market_value = current_price * multiplier * abs(positions_held)
                
                # P/L calculation (handles both long and short positions)
                if positions_held > 0:  # Long position
                    unrealized_pl = market_value - cost_basis
                else:  # Short position
                    unrealized_pl = cost_basis - market_value
                
                # P/L percentage based on cost basis
                pl_percent = (unrealized_pl / cost_basis) * 100 if cost_basis != 0 else 0.0

                # Store contract details for ATR calculations
                contract_details = {
                    'secType': sec_type,
                    'exchange': pos.contract.exchange if hasattr(pos.contract, 'exchange') else '',
                    'currency': pos.contract.currency if hasattr(pos.contract, 'currency') else 'USD',
                    'lastTradeDateOrContractMonth': pos.contract.lastTradeDateOrContractMonth if hasattr(pos.contract, 'lastTradeDateOrContractMonth') else '',
                    'conId': pos.contract.conId if hasattr(pos.contract, 'conId') else 0,
                }
                
                results.append({
                    'position': pos.contract.symbol,
                    'symbol': symbol,
                    'positions_held': positions_held,
                    'avg_cost': avg_cost,  # This is now the actual entry price
                    'current_price': current_price,
                    'multiplier': multiplier,
                    'cost_basis': cost_basis,
                    'market_value': market_value,
                    'unrealized_pl': unrealized_pl,
                    'pl_percent': pl_percent,
                    'contract_details': contract_details
                })
            
            # Cancel market data subscriptions
            for symbol, (pos, ticker) in tickers.items():
                if ticker is not None:
                    try:
                        ib.cancelMktData(pos.contract)
                    except Exception:
                        pass
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


def submit_stop_loss_orders(stop_loss_data):
    """
    Submit stop loss orders for each symbol in stop_loss_data.
    Cancels any existing orders for each symbol before submitting new stop loss.
    
    Args:
        stop_loss_data: dict mapping symbol to dict with:
            - 'stop_price': the stop loss price
            - 'quantity': number of contracts/shares (positive for long, negative for short)
            - 'contract_details': dict with conId, secType, exchange, currency, lastTradeDateOrContractMonth
    
    Returns:
        tuple: (results_list, success)
        results_list: list of dicts with order submission results
        success: True if all orders submitted successfully
    """
    ib = IB()
    results = []
    success = False
    
    if not stop_loss_data:
        print("No stop loss data provided")
        return results, False
    
    max_retries = 5
    for attempt in range(max_retries):
        client_id = random.randint(100, 999)
        
        try:
            print(f"Attempting to connect with clientId {client_id} for stop loss orders...")
            ib.connect('127.0.0.1', 7497, clientId=client_id)
            print(f"Successfully connected with clientId {client_id}")
            
            # Get all open trades and create a map of symbol to existing stop order
            open_trades = ib.openTrades()
            existing_stop_orders = {}
            for trade in open_trades:
                # Ensure it's a Stop order
                if trade.order.orderType == 'STP':
                    existing_stop_orders[trade.contract.symbol] = trade
            print(f"Found {len(existing_stop_orders)} existing stop orders.")

            for symbol, data in stop_loss_data.items():
                try:
                    stop_price = data.get('stop_price', 0)
                    quantity = data.get('quantity', 0)
                    contract_details = data.get('contract_details', {})
                    
                    if stop_price <= 0:
                        print(f"Skipping {symbol}: Invalid stop price {stop_price}")
                        results.append({
                            'symbol': symbol,
                            'status': 'skipped',
                            'message': f'Invalid stop price: {stop_price}'
                        })
                        continue
                    
                    if quantity == 0:
                        print(f"Skipping {symbol}: No position quantity")
                        results.append({
                            'symbol': symbol,
                            'status': 'skipped',
                            'message': 'No position quantity'
                        })
                        continue
                    
                    # Create contract from details
                    con_id = contract_details.get('conId', 0)
                    if con_id:
                        contract = Contract(conId=con_id)
                        ib.qualifyContracts(contract)
                        print(f"Using conId {con_id} for {symbol}")
                    else:
                        print(f"Skipping {symbol}: No conId available")
                        results.append({
                            'symbol': symbol,
                            'status': 'skipped',
                            'message': 'No contract ID available'
                        })
                        continue
                    
                    # Determine order action based on position
                    # For long positions (quantity > 0), we SELL to close
                    # For short positions (quantity < 0), we BUY to close
                    if quantity > 0:
                        action = 'SELL'
                        order_quantity = abs(quantity)
                    else:
                        action = 'BUY'
                        order_quantity = abs(quantity)

                    # Check if an order already exists for this symbol
                    existing_trade = existing_stop_orders.get(symbol)

                    if existing_trade:
                        # An order exists, check if modification is needed
                        existing_order = existing_trade.order
                        if existing_order.stopPrice == round(stop_price, 2):
                            print(f"No change needed for {symbol}: Stop price is already {stop_price:.2f}")
                            results.append({'symbol': symbol, 'status': 'unchanged', 'message': 'Stop price is already correct.'})
                            continue
                        else:
                            # Modify the existing order
                            print(f"Modifying {symbol} stop order from {existing_order.stopPrice} to {stop_price:.2f}")
                            stop_order = existing_order
                            stop_order.stopPrice = round(stop_price, 2)
                    else:
                        # No order exists, create a new one
                        print(f"Creating new {action} STOP order for {symbol}: {order_quantity} @ {stop_price:.2f}")
                        stop_order = StopOrder(
                            action=action,
                            totalQuantity=order_quantity,
                            stopPrice=round(stop_price, 2),
                            tif='GTC'  # Good Till Cancelled
                        )

                    # Place the new or modified order
                    trade = ib.placeOrder(contract, stop_order)
                    
                    # --- Wait for order status to confirm submission ---
                    # This logic remains the same for both new and modified orders.
                    # It ensures we wait for IBKR to acknowledge the request.
                    
                    # Wait for order to reach a stable state (submitted or rejected)
                    # Valid statuses: PendingSubmit, PreSubmitted, Submitted, Filled
                    valid_statuses = ['PendingSubmit', 'PreSubmitted', 'Submitted', 'Filled', 'ApiPending']
                    max_wait = 10  # Maximum wait time in seconds
                    wait_interval = 0.5
                    waited = 0
                    
                    while waited < max_wait:
                        ib.sleep(wait_interval)
                        waited += wait_interval
                        order_status = trade.orderStatus.status if trade.orderStatus else ''
                        print(f"  {symbol} order status: {order_status} (waited {waited:.1f}s)")
                        
                        if order_status in valid_statuses:
                            print(f"  Order for {symbol} reached {order_status} - pushing to IBKR")
                            break
                        elif order_status in ['Cancelled', 'ApiCancelled', 'Inactive']:
                            print(f"  Order for {symbol} was rejected: {order_status}")
                            break
                    
                    # Final status check
                    final_status = trade.orderStatus.status if trade.orderStatus else 'Unknown'
                    order_pushed = final_status in valid_statuses
                    
                    if order_pushed:
                        print(f"SUCCESS: {symbol} STOP order pushed to IBKR - Status: {final_status}, OrderId: {trade.order.orderId}")
                        results.append({
                            'symbol': symbol,
                            'status': 'submitted',
                            'order_id': trade.order.orderId,
                            'action': action,
                            'quantity': order_quantity,
                            'stop_price': stop_price,
                            'order_status': final_status,
                            'message': 'Order submitted successfully.'
                        })
                    else:
                        print(f"WARNING: {symbol} order may not have been accepted - Status: {final_status}")
                        results.append({
                            'symbol': symbol,
                            'status': 'pending',
                            'order_id': trade.order.orderId if trade.order else 0,
                            'action': action,
                            'quantity': order_quantity,
                            'stop_price': stop_price,
                            'order_status': final_status,
                            'message': f'Order status: {final_status}'
                        })
                    
                except Exception as e:
                    print(f"Error processing stop loss for {symbol}: {e}")
                    results.append({
                        'symbol': symbol,
                        'status': 'error',
                        'message': str(e)
                    })
            
            success = True
            break  # Success, exit retry loop
            
        except Exception as e:
            error_msg = str(e)
            print(f"Attempt {attempt + 1} failed with clientId {client_id}: {error_msg}")
            
            if "client id" in error_msg.lower() or "clientid" in error_msg.lower():
                if attempt < max_retries - 1:
                    print("ClientId conflict detected, retrying with a different clientId...")
                    sleep(0.5)
                    continue
                else:
                    print("Max retries reached. Unable to connect.")
            else:
                print(f"Error connecting to IBKR: {e}")
                break
                
        finally:
            if ib.isConnected():
                ib.disconnect()
    
    return results, success

def get_market_statuses_for_all(contracts_info):
    """
    Checks the market status for a batch of contracts in a single connection.

    Args:
        contracts_info (dict): A dictionary mapping symbol to contract details dict.

    Returns:
        dict: A dictionary mapping symbol to its market status ('OPEN', 'CLOSED', 'UNKNOWN').
    """
    ib = IB()
    statuses = {symbol: 'UNKNOWN' for symbol in contracts_info}
    
    try:
        client_id = random.randint(100, 999)
        ib.connect('127.0.0.1', 7497, clientId=client_id, timeout=5)

        for symbol, details in contracts_info.items():
            con_id = details.get('conId')
            if not con_id:
                continue

            try:
                contract = Contract(conId=con_id)
                cds = ib.reqContractDetails(contract)
                
                if not cds:
                    continue

                cd = cds[0]
                trading_hours_str = cd.liquidHours or cd.tradingHours
                time_zone_id = cd.timeZoneId

                if not trading_hours_str or not time_zone_id:
                    continue

                tz = pytz.timezone(time_zone_id)
                now = datetime.now(tz)
                
                is_open = False
                sessions = trading_hours_str.split(';')
                for session in sessions:
                    if 'CLOSED' in session or not session:
                        continue
                    
                    # Format is YYYYMMDD:HHMM
                    parts = session.split('-')
                    if len(parts) != 2: continue
                    start_str, end_str = parts
                    
                    start_dt = datetime.strptime(start_str, '%Y%m%d:%H%M').astimezone(tz)
                    end_dt = datetime.strptime(end_str, '%Y%m%d:%H%M').astimezone(tz)

                    if start_dt <= now < end_dt:
                        is_open = True
                        break
                
                statuses[symbol] = 'OPEN' if is_open else 'CLOSED'
            except Exception as e:
                logging.error(f"Error checking market status for {symbol}: {e}")

    except Exception as e:
        logging.error(f"Error checking market status: {e}")
    finally:
        if ib.isConnected():
            ib.disconnect()
            
    return statuses
