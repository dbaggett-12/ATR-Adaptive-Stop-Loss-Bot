# orders.py
import asyncio
import logging
import struct
from functools import lru_cache
from datetime import datetime
from typing import Dict, List

from ib_insync import IB, Contract, StopOrder, Trade
from PyQt6.QtCore import pyqtSignal


@lru_cache(maxsize=None)
def get_order_ref(symbol: str) -> str:
    """Create a deterministic, unique, and valid orderRef for a symbol."""
    # Create a simple, clean reference string for the order.
    return f"atr_stop_{symbol.lower().replace('.', '_')}"


async def _submit_or_modify_single_order(
    ib: IB, symbol: str, order_data: dict
):
    """
    Submit a new stop loss order OR replace an existing one for a single symbol.
    Fully compatible with ib_insync:
    - Uses ib.openOrders() for live snapshot
    - Cancels existing stops safely
    - Places new StopOrder
    """
    contract_details = order_data['contract_details']
    quantity = order_data['quantity']
    stop_price = float(order_data['stop_price'])
    con_id = contract_details['conId']

    try:
        # Determine action based on position
        action = 'SELL' if quantity > 0 else 'BUY'
        total_quantity = abs(quantity)
        order_ref = get_order_ref(symbol)

        # 1. Qualify contract
        contract = Contract(conId=con_id)
        await ib.qualifyContractsAsync(contract)

        # 2. Get all open trades from the live state.
        # This is done inside the task to avoid race conditions with other concurrent tasks.
        # ib.openTrades() contains all live trades after reqAllOpenOrdersAsync() is called.
        open_trades = ib.openTrades()
        logging.debug(f"Found {len(open_trades)} live trades to check against for {symbol}.")

        # 3. Look for existing stop
        existing_trade: Trade | None = None
        for trade in open_trades:
            # Filter by orderRef (deterministic) and order type.
            if trade.order.orderRef == order_ref and trade.order.orderType == 'STP':
                existing_trade = trade
                logging.info(f"Found existing stop for {symbol} with OrderId {trade.order.orderId}")
                break

        # 4. Cancel existing order if needed
        if existing_trade:
            existing_order = existing_trade.order
            existing_stop = getattr(existing_order, 'stopPrice', 0.0)
            if round(existing_stop, 4) == round(stop_price, 4):
                logging.info(f"{symbol}: stop unchanged at {stop_price:.4f}")
                return {
                    'symbol': symbol,
                    'status': 'unchanged',
                    'message': f'Stop held at {stop_price:.4f}'
                }

            logging.info(f"Cancelling existing stop for {symbol} (OrderId {existing_order.orderId})")
            ib.cancelOrder(existing_order)

            # Poll until cancellation confirmed
            for _ in range(20):  # 2 seconds max
                await asyncio.sleep(0.1)
                # Poll the status from the trade object's orderStatus.
                if existing_trade.orderStatus.status == 'Cancelled':
                    logging.info(f"Cancellation confirmed for OrderId {existing_order.orderId}")
                    break
            else:
                logging.warning(f"Timeout waiting for cancellation of OrderId {existing_order.orderId}")
                # Continue anyway, as the order is likely being cancelled.

        # 5. Place new stop order
        logging.info(f"Placing new {action} stop for {symbol}: {total_quantity} @ {stop_price:.4f}")
        new_order = StopOrder(
            action=action,
            totalQuantity=total_quantity,
            stopPrice=stop_price,
            tif='GTC',
            orderRef=order_ref
        )
        trade: Trade = ib.placeOrder(contract, new_order)

        # 6. Wait for status update
        for _ in range(20):  # 2 seconds max
            await asyncio.sleep(0.1)
            status = trade.orderStatus.status
            if status in {'Submitted', 'PreSubmitted', 'PendingSubmit'}:
                break

        final_status = trade.orderStatus.status if trade.orderStatus else "Unknown"
        if final_status in {'Submitted', 'PreSubmitted', 'PendingSubmit'}:
            logging.info(f"{symbol} stop submitted successfully at {stop_price:.4f}")
            return {
                'symbol': symbol,
                'status': 'submitted',
                'message': f'Stop at {stop_price:.4f}',
            }

        logging.error(f"{symbol} stop failed. Status: {final_status}")
        return {
            'symbol': symbol,
            'status': 'error',
            'message': f'Failed: {final_status}',
        }

    except Exception as e:
        logging.exception(f"Error processing stop for {symbol}: {e}")
        return {
            'symbol': symbol,
            'status': 'error',
            'message': str(e),
        }


async def process_stop_orders(
    ib: IB,
    orders_to_submit: Dict[str, dict],
    log_signal: pyqtSignal,
):
    """
    Centralized entry point for stop-loss processing.
    Ensures existing stops are modified rather than duplicated.
    """
    if not orders_to_submit:
        logging.info("No stop orders to process.")
        return []

    timestamp = datetime.now().strftime("%H:%M:%S")
    # 6. Log high-level info from the main orchestrator function.
    log_signal.emit(f"--- [{timestamp}] Submitting Stops ---")

    for symbol, data in orders_to_submit.items():
        stop_price = data['stop_price']
        log_signal.emit(
            f"  {symbol}: Stop = {stop_price:.4f} "
            f"(Raw: 0x{struct.pack('>d', stop_price).hex()})"
        )

    # 7. Centralize broker state refresh and create a read-only snapshot.
    await ib.reqAllOpenOrdersAsync()
    logging.info(f"Refreshed open orders. Found {len(ib.openOrders())} total open orders.")

    # 8. Create and execute concurrent tasks, passing the snapshot.
    tasks = [
        _submit_or_modify_single_order(ib, symbol, data)
        for symbol, data in orders_to_submit.items()
    ]

    # 9. Gather results from all concurrent tasks.
    return await asyncio.gather(*tasks)
