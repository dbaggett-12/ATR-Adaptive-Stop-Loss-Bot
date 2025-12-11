from ib_insync import IB, util, Contract, StopOrder
from time import sleep
import random
from datetime import datetime
import logging
import pytz

def fetch_positions():
    """
    Connects to IBKR and returns a tuple: (results_list, connection_success)
    results_list: list of dicts with position data
    connection_success: True if successfully connected and retrieved data, False otherwise
    """
    # This function is now a wrapper for staged fetching.
    ib = IB()
    connection_success = False
    positions_data = []

    # Try multiple clientIds in case one is still in use
    max_retries = 5
    for attempt in range(max_retries):
        client_id = random.randint(100, 999)
        try:
            print(f"Attempting to connect with clientId {client_id}...")
            ib.connect('127.0.0.1', 7497, clientId=client_id)
            print(f"Successfully connected with clientId {client_id}")
            
            positions = ib.positions()
            
            # Stage 1: Get basic position data
            positions_data = fetch_basic_positions(ib, positions)
            # Stage 2: Enrich with market data
            positions_data = fetch_market_data_for_positions(ib, positions_data)

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

    return positions_data, connection_success


from ib_insync.objects import Position
from typing import List, Dict
import math


def _submit_stop_loss_orders_internal(ib, stop_loss_data):
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
    results = []
    
    if not stop_loss_data: # stop_loss_data is now just the orders_to_submit dict
        print("No stop loss data provided")
        return results
    
    try:
            print(f"\nSubmitting {len(stop_loss_data)} stop loss order(s)...")
            # Get all open trades and create a map of symbol to existing stop order
            open_trades = ib.openTrades()
            existing_stop_orders = {}
            for trade in open_trades:
                # Ensure it's a Stop order
                if trade.order.orderType == 'STP':
                    # Use conId for a more reliable key than symbol
                    conId = trade.contract.conId
                    existing_stop_orders[conId] = trade
                    existing_stop_orders[trade.contract.symbol] = trade
            print(f"Found {len(existing_stop_orders)} existing stop orders.")
            trades_to_monitor = []

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

                    # Check if an order already exists for this contract's conId
                    existing_trade = existing_stop_orders.get(con_id)

                    if not existing_trade:
                        # This is a new order, handle it in a separate pass
                        continue

                    # --- This is an existing order, handle modification ---
                    existing_order = existing_trade.order
                    if existing_order.stopPrice == round(stop_price, 2):
                        print(f"No change needed for {symbol}: Stop price is already {stop_price:.2f}")
                        results.append({'symbol': symbol, 'status': 'unchanged', 'message': 'Stop price is already correct.'})
                        continue
                    else:
                        # To safely modify, create a new order object with the existing order's ID
                        print(f"Modifying {symbol} stop order from {existing_order.stopPrice} to {stop_price:.2f}")
                        stop_order = StopOrder(
                            action=action,
                            totalQuantity=order_quantity,
                            stopPrice=round(stop_price, 2),
                            orderId=existing_order.orderId, # IMPORTANT: Use existing orderId
                            tif='GTC'
                        )
                        trade = ib.placeOrder(contract, stop_order)
                        trades_to_monitor.append(trade)

                except Exception as e:
                    print(f"Error processing modification for {symbol}: {e}")
                    results.append({'symbol': symbol, 'status': 'error', 'message': str(e)})
            
            # Give modifications a moment to process
            if any(t for t in trades_to_monitor if t.order.orderId != 0):
                print("Pausing for 1 second after modifications...")
                ib.sleep(1)

            # --- Second Pass: Create NEW orders ---
            for symbol, data in stop_loss_data.items():
                try:
                    con_id = data.get('contract_details', {}).get('conId', 0)
                    if con_id in existing_stop_orders:
                        # Already handled in the modification pass
                        continue

                    # This is a new order
                    stop_price = data.get('stop_price', 0)
                    quantity = data.get('quantity', 0)
                    contract = Contract(conId=con_id)
                    ib.qualifyContracts(contract)
                    
                    action = 'SELL' if quantity > 0 else 'BUY'
                    order_quantity = abs(quantity)

                    print(f"Creating new {action} STOP order for {symbol}: {order_quantity} @ {stop_price:.2f}")
                    stop_order = StopOrder(
                        action=action,
                        totalQuantity=order_quantity,
                        stopPrice=round(stop_price, 2),
                        tif='GTC'
                    )

                    # Place the new or modified order
                    trade = ib.placeOrder(contract, stop_order)
                    trades_to_monitor.append(trade)
                    
                except Exception as e:
                    print(f"Error processing stop loss for {symbol}: {e}")
                    results.append({
                        'symbol': symbol,
                        'status': 'error',
                        'message': str(e)
                    })
            
            # --- Wait for all submitted orders to be processed ---
            if trades_to_monitor:
                print(f"\nWaiting for {len(trades_to_monitor)} order(s) to be processed...")
                max_wait = 15  # seconds
                waited = 0
                while waited < max_wait:
                    ib.sleep(1) # Process events
                    waited += 1
                    if all(t.isDone() for t in trades_to_monitor):
                        print("All orders have reached a final state.")
                        break
                    print(f"  ... still waiting for orders to complete ({waited}s)")

            # --- Report final status for all monitored trades ---
            valid_statuses = ['PendingSubmit', 'PreSubmitted', 'Submitted', 'Filled', 'ApiPending']
            for trade in trades_to_monitor:
                symbol = trade.contract.symbol
                final_status = trade.orderStatus.status if trade.orderStatus else 'Unknown'
                order_pushed = final_status in valid_statuses

                # Safely get order details
                action = getattr(trade.order, 'action', 'N/A')
                quantity = getattr(trade.order, 'totalQuantity', 0)
                stop_price = 0
                if isinstance(trade.order, StopOrder):
                    stop_price = getattr(trade.order, 'stopPrice', 0)

                if order_pushed:
                    print(f"SUCCESS: {symbol} STOP order pushed to IBKR - Status: {final_status}, OrderId: {trade.order.orderId}")
                    results.append({
                        'symbol': symbol,
                        'status': 'submitted',
                        'order_id': trade.order.orderId,
                        'action': action,
                        'quantity': quantity,
                        'stop_price': stop_price,
                        'order_status': final_status,
                        'message': 'Order submitted successfully.'
                    })
                else:
                    print(f"WARNING: {symbol} order may not have been accepted - Status: {final_status}")
                    # Even on failure, try to get the orderId if it exists
                    order_id = getattr(trade.order, 'orderId', 0)
                    results.append({
                        'symbol': symbol,
                        'status': 'pending',
                        'order_id': order_id,
                        'action': action,
                        'quantity': quantity,
                        'stop_price': stop_price,
                        'order_status': final_status,
                        'message': f'Order status: {final_status}'
                    })

    except Exception as e:
        logging.error(f"An error occurred during stop loss submission: {e}")
    return results

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

def fetch_basic_positions(ib: IB, positions: List[Position]) -> List[Dict]:
    """
    Stage 1: Fetches basic position data without market data.
    Qualifies contracts and calculates entry price.
    """
    results = []
    if not positions:
        return results

    # Qualify contracts first to fill in missing details like exchange
    contracts = [pos.contract for pos in positions]
    ib.qualifyContracts(*contracts)

    for pos in positions:
        positions_held = float(pos.position)
        raw_avg_cost = float(pos.avgCost)
        symbol = pos.contract.symbol

        sec_type = pos.contract.secType if hasattr(pos.contract, 'secType') else 'STK'
        
        multiplier = 1.0
        if hasattr(pos.contract, 'multiplier') and pos.contract.multiplier:
            try:
                multiplier = float(pos.contract.multiplier)
            except (ValueError, TypeError):
                multiplier = 1.0
        
        if sec_type == 'FUT' and multiplier > 1:
            avg_cost = raw_avg_cost / multiplier
        else:
            avg_cost = raw_avg_cost

        contract_details = {
            'secType': sec_type,
            'exchange': pos.contract.exchange or '',
            'currency': pos.contract.currency or 'USD',
            'lastTradeDateOrContractMonth': pos.contract.lastTradeDateOrContractMonth or '',
            'conId': pos.contract.conId or 0,
        }

        results.append({
            'position': pos.contract.symbol,
            'symbol': symbol,
            'positions_held': positions_held,
            'avg_cost': avg_cost,
            'multiplier': multiplier,
            'contract_details': contract_details,
            # Placeholders for data to be fetched later
            'current_price': 0.0,
            'cost_basis': 0.0,
            'market_value': 0.0,
            'unrealized_pl': 0.0,
            'pl_percent': 0.0,
        })
    return results

def fetch_market_data_for_positions(ib: IB, positions_data: List[Dict]) -> List[Dict]:
    """
    Stage 2: Enriches position data with live market prices and contract details.
    """
    if not positions_data:
        return []

    # Create contracts from conIds for reliability
    contracts = [Contract(conId=p['contract_details']['conId']) for p in positions_data]
    ib.qualifyContracts(*contracts)

    # Get full contract details to retrieve minTick
    contract_details_objects = {}
    for contract in contracts:
        try:
            cds = ib.reqContractDetails(contract)
            if cds:
                contract_details_objects[contract.symbol] = cds[0]
        except Exception as e:
            logging.error(f"Could not get contract details for {contract.symbol}: {e}")

    # Request market data
    ib.reqMarketDataType(3) # Delayed data
    tickers = {}
    for p in positions_data:
        # Create contract with exchange to avoid validation errors
        contract = Contract(conId=p['contract_details']['conId'], exchange=p['contract_details'].get('exchange', ''))
        tickers[p['symbol']] = ib.reqMktData(contract, "", False, False)
    
    logging.info("Waiting for market data to populate...")
    ib.sleep(5)
    logging.info("Finished waiting for market data.")

    for p_data in positions_data:
        symbol = p_data['symbol']
        ticker = tickers.get(symbol)
        current_price = 0.0

        if ticker:
            if ticker.last and not math.isnan(ticker.last) and ticker.last > 0:
                current_price = ticker.last
            elif ticker.close and not math.isnan(ticker.close) and ticker.close > 0:
                current_price = ticker.close
            elif ticker.bid and ticker.ask and not math.isnan(ticker.bid) and not math.isnan(ticker.ask):
                current_price = (ticker.bid + ticker.ask) / 2
            elif ticker.marketPrice() and not math.isnan(ticker.marketPrice()):
                current_price = ticker.marketPrice()
        
        p_data['current_price'] = current_price

        # Update contract details with minTick
        cd = contract_details_objects.get(symbol)
        p_data['contract_details']['minTick'] = cd.minTick if cd else 0.01

        # Recalculate market values and P/L
        avg_cost = p_data['avg_cost']
        multiplier = p_data['multiplier']
        positions_held = p_data['positions_held']

        cost_basis = avg_cost * multiplier * abs(positions_held)
        market_value = current_price * multiplier * abs(positions_held)
        
        if positions_held > 0:
            unrealized_pl = market_value - cost_basis
        else:
            unrealized_pl = cost_basis - market_value
        
        p_data['cost_basis'] = cost_basis
        p_data['market_value'] = market_value
        p_data['unrealized_pl'] = unrealized_pl
        p_data['pl_percent'] = (unrealized_pl / cost_basis) * 100 if cost_basis != 0 else 0.0

    # Cancel subscriptions
    for ticker in tickers.values():
        ib.cancelMktData(ticker.contract)

    return positions_data
