from ib_insync import IB, util, Contract, StopOrder
from time import sleep
import asyncio
import random
from datetime import datetime
import logging
import pytz
import math
from decimal import Decimal, ROUND_DOWN, ROUND_UP

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

async def _submit_stop_loss_orders_internal(ib, stop_loss_data):
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
        # Get all open trades and create a map of conId to existing stop order
        open_trades = await ib.reqAllOpenOrdersAsync()
        existing_stop_orders = {}
        for trade in open_trades:
            # Ensure it's a Stop order
            if trade.order.orderType == 'STP':
                # Use conId for a more reliable key than symbol
                conId = trade.contract.conId
                existing_stop_orders[conId] = trade
        
        print(f"Found {len(existing_stop_orders)} existing stop orders.")
        trades_to_monitor = []

        for symbol, data in stop_loss_data.items():
            try:
                stop_price = data.get('stop_price', 0)
                quantity = data.get('quantity', 0)
                contract_details = data.get('contract_details', {})
                
                if stop_price <= 0:
                    # This case is now handled by the logic that builds stop_loss_data,
                    # but we keep it as a safeguard.
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
                
                if not contract_details:
                    print(f"Skipping {symbol}: No position quantity")
                    results.append({
                        'symbol': symbol,
                        'status': 'skipped',
                        'message': 'No position quantity'
                    })
                    continue
                
                # Create contract from details
                con_id = contract_details.get('conId', 0)
                if not con_id:
                    print(f"Skipping {symbol}: No conId available")
                    results.append({
                        'symbol': symbol,
                        'status': 'skipped',
                        'message': 'No contract ID available'
                    })
                    continue

                try:
                    contract = Contract(conId=con_id)
                    await ib.qualifyContractsAsync(contract)
                except Exception as qe:
                    print(f"Skipping {symbol}: Could not qualify contract with conId {con_id}. Error: {qe}")
                    results.append({
                        'symbol': symbol, 'status': 'error',
                        'message': f'Contract qualification failed: {qe}'
                    })
                    continue
                
                # Determine order action based on position
                # For long positions (quantity > 0), we SELL to close
                # For short positions (quantity < 0), we BUY to close
                action = 'SELL' if quantity > 0 else 'BUY'
                order_quantity = abs(quantity)

                # The stop_price is now pre-rounded by the UI logic. No further calculation is needed here.
                logging.info(f"Using pre-calculated stop price for {symbol}: {stop_price}")

                # Check if an order already exists for this contract's conId
                existing_trade = existing_stop_orders.get(con_id)

                if existing_trade:
                    # --- This is an existing order, handle modification ---
                    # Compare rounded prices to avoid floating point issues
                    if math.isclose(existing_trade.order.stopPrice, stop_price, rel_tol=1e-9, abs_tol=1e-9):
                        print(f"No change needed for {symbol}: Stop price is already {stop_price:.2f}")
                        results.append({'symbol': symbol, 'status': 'unchanged', 'message': 'Stop price is already correct.'})
                        continue
                    else:
                        # To modify, we must update the existing order object in-place.
                        print(f"Modifying {symbol} stop order from {existing_trade.order.stopPrice} to {stop_price:.2f}")
                        existing_order = existing_trade.order # Get the live order object
                        existing_order.action = action
                        existing_order.stopPrice = stop_price
                        trade = ib.placeOrder(contract, existing_order)
                        trades_to_monitor.append(trade)
                else:
                    # --- This is a new order, create it ---
                    print(f"Creating new {action} STOP order for {symbol}: {order_quantity} @ {stop_price:.2f}")
                    stop_order = StopOrder(
                        action=action,
                        totalQuantity=order_quantity,
                        stopPrice=stop_price, # Price is pre-rounded by calculator
                        tif='GTC',
                        orderId=ib.client.getReqId() # Get a new unique ID for a new order
                    )
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
                    await asyncio.sleep(1) # Process events asynchronously
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

async def get_market_statuses_for_all(ib: IB, contracts_info: dict) -> dict:
    """
    Checks the market status for a batch of contracts using their explicit trading sessions.
    Returns a detailed state: 'ACTIVE (RTH)', 'ACTIVE (NT)', or 'CLOSED'.

    Args:
        ib (IB): The connected ib_insync IB instance.
        contracts_info (dict): A dictionary mapping symbol to contract details dict.

    Returns:
        dict: A dictionary mapping symbol to its detailed market status.
    """
    now_utc = datetime.now(pytz.utc)

    async def check_status(symbol, details):
        con_id = details.get('conId')
        if not con_id:
            logging.warning(f"No conId for {symbol}, skipping market status check.")
            return symbol, 'CLOSED'

        try:
            contract = Contract(conId=con_id)
            cds = await ib.reqContractDetailsAsync(contract)
            if not cds:
                logging.warning(f"No contract details for {symbol}, marking as CLOSED.")
                return symbol, 'CLOSED'

            cd = cds[0]
            # liquidSessions() returns timezone-aware UTC datetimes for Regular Trading Hours (RTH)
            rth_sessions = cd.liquidSessions()
            # tradingSessions() returns timezone-aware UTC datetimes for the full session (including overnight)
            full_sessions = cd.tradingSessions()

            # 1. Check for ACTIVE (RTH)
            if rth_sessions:
                for session in rth_sessions:
                    if session.start <= now_utc < session.end:
                        logging.info(f"Market status for {symbol}: ACTIVE (RTH)")
                        return symbol, 'ACTIVE (RTH)'

            # 2. Check for ACTIVE (NT) - Night Trading / Electronic Hours
            if full_sessions:
                for session in full_sessions:
                    if session.start <= now_utc < session.end:
                        logging.info(f"Market status for {symbol}: ACTIVE (NT)")
                        return symbol, 'ACTIVE (NT)'

        except Exception as e:
            logging.error(f"Error checking market status for {symbol}: {e}")
            return symbol, 'CLOSED'

        # 3. If neither, it's CLOSED. This is the safe default.
        logging.info(f"Market status for {symbol}: CLOSED (no active session found)")
        return symbol, 'CLOSED'

    tasks = [check_status(symbol, details) for symbol, details in contracts_info.items()]
    results = await asyncio.gather(*tasks)
    return dict(results)

async def fetch_basic_positions(ib: IB, positions: List[Position]) -> List[Dict]:
    """
    Stage 1: Fetches basic position data without market data.
    Qualifies contracts and calculates entry price.
    This is an async function.
    """
    results = []
    if not positions:
        return results

    # Filter out positions with zero quantity before doing any work
    active_positions = [p for p in positions if p.position != 0]
    if not active_positions:
        logging.info("No active positions with non-zero quantity found.")
        return results
    
    logging.info(f"Found {len(active_positions)} active positions out of {len(positions)} total.")

    contracts_to_qualify = [pos.contract for pos in active_positions]
    await ib.qualifyContractsAsync(*contracts_to_qualify)

    for pos in active_positions:
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
        
        # For futures, avgCost is the total value (price * multiplier * quantity).
        # We need to derive the price per contract.
        # For stocks, avgCost is already the price per share.
        if sec_type == 'FUT' and multiplier > 1 and positions_held != 0:
            avg_cost = raw_avg_cost / (multiplier * positions_held)
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

async def fetch_market_data_for_positions(ib: IB, positions_data: List[Dict]) -> List[Dict]:
    """
    Stage 2: Enriches position data with live market prices and contract details.
    """
    if not positions_data:
        return []

    # Create contracts from conIds for reliability
    contracts = [Contract(conId=p['contract_details']['conId']) for p in positions_data]
    await ib.qualifyContractsAsync(*contracts)

    # Get full contract details to retrieve minTick
    contract_details_objects = {}
    for contract in contracts:
        try:
            cds = await ib.reqContractDetailsAsync(contract)
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
    await asyncio.sleep(5)
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
        if cd:
            p_data['contract_details']['minTick'] = cd.minTick or 0.01 # Default to 0.01 if None
            # Capture the price magnifier, which is crucial for contracts priced in cents.
            p_data['contract_details']['priceMagnifier'] = int(cd.priceMagnifier) if cd.priceMagnifier else 1
            # Capture the mdSizeMultiplier, which often represents the point value or contract size.
            p_data['contract_details']['mdSizeMultiplier'] = int(cd.mdSizeMultiplier) if cd.mdSizeMultiplier is not None else None

        # Recalculate market values and P/L
        avg_cost = p_data['avg_cost']
        multiplier = p_data['multiplier']
        positions_held = p_data['positions_held']

        # For long positions, P/L is (current price - entry price) * multiplier * quantity.
        # Both cost_basis and market_value should be positive.
        cost_basis = avg_cost * multiplier * positions_held
        market_value = current_price * multiplier * positions_held

        unrealized_pl = market_value - cost_basis

        p_data['cost_basis'] = cost_basis
        p_data['market_value'] = market_value
        p_data['unrealized_pl'] = unrealized_pl
        p_data['pl_percent'] = (unrealized_pl / cost_basis) * 100 if cost_basis != 0 else 0.0

    # Cancel subscriptions
    for ticker in tickers.values():
        ib.cancelMktData(ticker.contract)

    return positions_data
