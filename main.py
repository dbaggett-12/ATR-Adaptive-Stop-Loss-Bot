# main.py
import sys
import asyncio
import logging
import random
import json
import os
from datetime import datetime, timedelta
from PyQt6 import QtGui, QtCore
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QTableWidget, QTableWidgetItem, QVBoxLayout,
    QWidget, QDoubleSpinBox, QTabWidget, QTextEdit, QPushButton, QHeaderView, QAbstractSpinBox,
    QLabel, QHBoxLayout, QCheckBox
)
from PyQt6.QtCore import Qt, QTimer, QSize, QObject, QThread, pyqtSignal
from PyQt6.QtGui import QMovie
from ibkr_api import get_market_statuses_for_all, fetch_basic_positions, fetch_market_data_for_positions
from ib_insync import IB, util, Future, Stock, Contract, Order, StopOrder
import math
import pandas as pd

import struct
from decimal import Decimal, ROUND_DOWN
# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
ATR_HISTORY_FILE = 'atr_history.json'

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
    
    # Fallback: try to derive from contract details
    price_magnifier = contract_details.get('priceMagnifier', 1)
    md_size_multiplier = contract_details.get('mdSizeMultiplier')
    
    # Case 1: Contracts quoted in cents (e.g., agricultural, some metals)
    # For these, the priceMagnifier is often > 1 (e.g., 100).
    # The mdSizeMultiplier usually represents the contract size (e.g., 5000 bushels for ZC).
    # The point value per 1-cent move is (mdSizeMultiplier * $0.01).
    # Since a 1.00 price move *is* a 1-cent move, we need to divide by the magnifier
    # to get the value per dollar, then multiply by the contract size.
    # Simplified: point_value = mdSizeMultiplier / priceMagnifier
    if price_magnifier > 1 and md_size_multiplier is not None:
        # For contracts like ZC (Corn): 5000 / 100 = $50 per 1-cent move.
        # For HG (Copper): 25000 / 100 = $250 per 1-cent move.
        point_value = float(md_size_multiplier) / price_magnifier
        logging.warning(f"Unknown contract {symbol}. Derived point value: ${point_value:.2f} "
                       f"(mdSizeMultiplier={md_size_multiplier}, priceMagnifier={price_magnifier}). "
                       f"Consider adding to CONTRACT_POINT_VALUES.")
        return point_value
    
    # Case 2: Contracts where mdSizeMultiplier represents point value
    elif md_size_multiplier is not None and md_size_multiplier > 1:
        point_value = float(md_size_multiplier)
        logging.warning(f"Unknown contract {symbol}. Using mdSizeMultiplier as point value: ${point_value:.2f}. "
                       f"Consider adding to CONTRACT_POINT_VALUES.")
        return point_value
    
    # Case 3: Standard contracts where multiplier is the point value
    else:
        point_value = multiplier if multiplier > 0 else 1.0
        logging.warning(f"Unknown contract {symbol}. Using multiplier as point value: ${point_value:.2f}. "
                       f"Consider adding to CONTRACT_POINT_VALUES.")
        return point_value


class DataWorker(QObject):
    """
    Worker thread for fetching and processing all IBKR data.
    This runs in the background to keep the UI responsive.
    """
    finished = pyqtSignal()
    error = pyqtSignal(str)
    positions_ready = pyqtSignal(list)
    market_data_ready = pyqtSignal(list)
    atr_ready = pyqtSignal(dict)
    orders_submitted = pyqtSignal(list)
    log_message = pyqtSignal(str) # Signal to send log messages to the UI
    
    def __init__(self, atr_window, atr_ratios, highest_stop_losses):
        super().__init__()
        # Give worker access to the main window's methods and data
        self.atr_window = atr_window
        self.send_adaptive_stops = atr_window.send_adaptive_stops
        self.atr_ratios = atr_ratios
        self.highest_stop_losses = highest_stop_losses
        self.symbol_stop_enabled = atr_window.symbol_stop_enabled

    async def run_async(self):
        """Main worker method, executes all data stages sequentially."""
        ib = IB()
        try:
            client_id = random.randint(100, 999)
            await ib.connectAsync('127.0.0.1', 7497, clientId=client_id)
            
            # --- Stage 1: Fetch Positions ---
            # Using reqPositionsAsync for non-blocking behavior
            positions = await ib.reqPositionsAsync()
            basic_positions = await fetch_basic_positions(ib, positions)
            self.positions_ready.emit(basic_positions)
            
            # --- Stage 2: Fetch Market Data ---
            enriched_positions = await fetch_market_data_for_positions(ib, basic_positions)
            self.market_data_ready.emit(enriched_positions)
            
            # --- Stage 3: Calculate ATR ---
            contract_details_map = {p['symbol']: p['contract_details'] for p in enriched_positions}
            market_statuses = await get_market_statuses_for_all(ib, contract_details_map)
            self.atr_window.market_statuses = market_statuses # Update main window
            
            symbols = [p['symbol'] for p in enriched_positions]
            atr_results = await self.atr_window.calculate_atr_for_symbols(ib, symbols, market_statuses, contract_details_map)
            self.atr_ready.emit(atr_results)

            # --- Stage 4: Submit Orders ---
            # Read the latest state of the toggle directly from the window
            if not self.atr_window.send_adaptive_stops:
                logging.info("Adaptive stops are disabled, skipping order submission.")
                self.orders_submitted.emit([])
                return

            # The main window now holds the definitive list of positions
            self.atr_window.positions_data = enriched_positions
            stop_loss_data = self.build_stop_loss_data()
            final_results = stop_loss_data.get('statuses_only', [])
            orders_to_submit = stop_loss_data.get('orders_to_submit', {})
            
            # Get all open trades to check for existing stop orders
            open_trades = await ib.reqAllOpenOrdersAsync()
            existing_stop_orders = {}
            for trade in open_trades:
                if trade.order.orderType == 'STP':
                    # Use conId for a more reliable key than symbol
                    existing_stop_orders[trade.contract.conId] = trade

            if orders_to_submit:
                timestamp = datetime.now().strftime("%H:%M:%S")
                self.log_message.emit(f"--- [{timestamp}] Submitting Orders ---")
                for symbol, data in orders_to_submit.items():
                    stop_price = data['stop_price']
                    self.log_message.emit(f"  {symbol}: Stop Price = {stop_price:.4f} (Raw: 0x{struct.pack('>d', stop_price).hex()})")

                # Submit all orders concurrently using asyncio.gather
                tasks = [self.submit_or_modify_order(ib, symbol, data, existing_stop_orders) for symbol, data in orders_to_submit.items()]
                submission_results = await asyncio.gather(*tasks)

                final_results.extend(submission_results)
                self.orders_submitted.emit(final_results)
            else:
                logging.info("No new stop loss orders to submit.")
                self.orders_submitted.emit(final_results)

        except Exception as e:
            self.error.emit(str(e))
        finally:
            if ib.isConnected():
                ib.disconnect()
            self.finished.emit()

    def run(self):
        """Synchronous entry point that runs the async run_async method."""
        asyncio.run(self.run_async())

    async def submit_or_modify_order(self, ib, symbol, order_data, existing_stop_orders):
        """Submits a new stop loss order or modifies an existing one."""
        contract_details = order_data['contract_details']
        quantity = order_data['quantity']
        stop_price = order_data['stop_price']
        con_id = contract_details.get('conId')

        try:
            # Recreate the contract object for placing the order
            contract = Contract(
                conId=contract_details['conId'],
                exchange=contract_details.get('exchange', '')
            )
            await ib.qualifyContractsAsync(contract)

            # Determine order action based on position quantity
            action = 'SELL' if quantity > 0 else 'BUY'
            total_quantity = abs(quantity)

            existing_trade = existing_stop_orders.get(con_id)

            if existing_trade:
                # --- MODIFY EXISTING ORDER ---
                existing_order = existing_trade.order
                # Compare rounded prices to avoid floating point issues
                if round(existing_order.stopPrice, 4) == round(stop_price, 4):
                    logging.info(f"No change needed for {symbol}: Stop price is already {stop_price:.4f}")
                    return {'symbol': symbol, 'status': 'unchanged', 'message': f'Stop price is already {stop_price:.4f}'}
                
                logging.info(f"Modifying {symbol} stop order from {existing_order.stopPrice} to {stop_price:.4f}")
                order = StopOrder(
                    action=action,
                    totalQuantity=total_quantity,
                    stopPrice=stop_price,
                    orderId=existing_order.orderId, # IMPORTANT: Use existing orderId to modify
                    # transmit=False, # Optional: for manual submission flow
                    tif='GTC'
                )
            else:
                # --- CREATE NEW ORDER ---
                logging.info(f"Creating new {action} STOP for {symbol}: {total_quantity} @ {stop_price:.4f}")
                order = StopOrder(
                    action=action,
                    totalQuantity=total_quantity,
                    stopPrice=stop_price,
                    orderId=ib.client.getReqId(), # Explicitly get a new unique ID for a new order
                    tif='GTC'  # Good-Til-Canceled
                )

            # placeOrder is synchronous and returns a Trade object immediately. It is not awaitable.
            trade = ib.placeOrder(contract, order)
            logging.info(f"Placed order for {symbol}. OrderId: {trade.order.orderId}. Waiting for status...")

            # Wait for the order status to be updated.
            # The 'update' event on the trade is fired when its state changes.
            try:
                await asyncio.wait_for(trade.statusEvent, timeout=10)  # Wait up to 10 seconds
            except asyncio.TimeoutError:
                logging.warning(f"Timeout waiting for order status update for {symbol}. Current status: {trade.orderStatus.status}")

            final_status = trade.orderStatus.status
            if final_status in {'Submitted', 'PreSubmitted', 'ApiPending', 'PendingSubmit'}:
                logging.info(f"Successfully submitted stop loss for {symbol} at {stop_price}")
                return {'symbol': symbol, 'status': 'submitted', 'message': f'Stop at {stop_price:.4f} ({final_status})'}
            else:
                logging.error(f"Order for {symbol} was not submitted successfully. Status: {trade.orderStatus.status}")
                return {'symbol': symbol, 'status': 'error', 'message': f'Failed: {trade.orderStatus.status}'}

        except Exception as e:
            logging.error(f"Error processing stop loss for {symbol}: {e}")
            return {'symbol': symbol, 'status': 'error', 'message': str(e)}


    def build_stop_loss_data(self):
        """Prepares the data structure for submitting stop loss orders."""
        orders_to_submit = {}
        statuses_only = []

        # Use the position data from the main window, which was set by the worker
        for i, p_data in enumerate(self.atr_window.positions_data):
            symbol = p_data['symbol']
            
            # Use the already computed stop loss from the UI table's data model.
            # This ensures that what the user sees is what gets submitted.
            # The ratcheting logic is already applied when this value is calculated for the UI.
            final_stop_price = self.atr_window.computed_stop_losses[i] if i < len(self.atr_window.computed_stop_losses) else 0

            # Get the highest stop loss recorded to check if we should hold.
            # This is a safety check. The primary logic is now driven by the UI-calculated value.
            highest_stop = self.atr_window.highest_stop_losses.get(symbol, 0)

            # Check if stop submission is enabled for this specific symbol
            if not self.symbol_stop_enabled.get(symbol, True):
                logging.info(f"Worker: Skipping {symbol}: Stop submission is disabled for this symbol.")
                statuses_only.append({
                    'symbol': symbol,
                    'status': 'skipped',
                    'message': 'Individually disabled'
                })
                continue


            if final_stop_price <= 0:
                logging.info(f"Worker: Skipping {symbol}: No valid stop price computed.")
                statuses_only.append({
                    'symbol': symbol,
                    'status': 'error',
                    'message': 'Invalid stop price computed'
                })
                continue

            # Ratchet check: if the new stop is lower than the highest one, hold the order
            if highest_stop > 0 and final_stop_price < highest_stop:
                logging.info(f"Worker: Holding stop for {symbol}. New stop {final_stop_price:.4f} is lower than existing {highest_stop:.4f}")
                statuses_only.append({
                    'symbol': symbol,
                    'status': 'held',
                    'message': 'Stop Held'
                })
                continue

            orders_to_submit[symbol] = {
                'stop_price': final_stop_price, # Use the final, rounded, ratcheted stop price
                'quantity': p_data['positions_held'],
                'contract_details': p_data['contract_details']
            }
        return {'orders_to_submit': orders_to_submit, 'statuses_only': statuses_only}

class ATRWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("ATR Adaptive Stop Bot")
        self.setGeometry(100, 100, 1200, 800)

        # Placeholder data lists
        self.positions = []
        self.atr_values = []
        self.atr_ratios = []
        self.statuses = []
        self.symbols = []
        self.positions_held = []
        self.current_prices = []
        self.multipliers = []
        self.computed_stop_losses = []  # Current computed stop losses
        self.all_positions_details = [] # Store full position dictionaries
        self.highest_stop_losses = {}  # Track highest stop loss per symbol (never goes down)
        self.thread = None # Initialize thread attribute to None
        self.positions_data = [] # Store latest enriched position data for order submission
        self.contract_details_map = {}  # Store contract details by symbol
        self.symbol_stop_enabled = {}  # {symbol: bool} to track individual stop toggles
        
        # ATR calculation data
        self.atr_symbols = []
        self.tr_values = []
        self.atr_calculated = []
        self.previous_atr_values = []
        self.previous_atr_sources = []  # Track if value is "Calculated" or "User Inputted"
        
        # Track last market data values for each symbol to detect market closures
        self.last_market_data = {}  # {symbol: {'high': float, 'low': float, 'prev_close': float}}
        
        # ATR history file path
        self.atr_history_file = os.path.join(os.path.dirname(__file__), 'atr_history.json')
        
        # Load ATR history
        self.atr_history, self.user_overrides = self.load_atr_history()
        
        # Adaptive Stop Loss toggle state
        self.send_adaptive_stops = False
        self.market_statuses = {}  # {symbol: 'OPEN' | 'CLOSED' | 'UNKNOWN'}

        # Central widget & layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout()
        central_widget.setLayout(layout)

        # --- Status Bar at Top ---
        status_container = QWidget()
        status_layout = QHBoxLayout()
        status_container.setLayout(status_layout)
        
        # Adaptive Stop Loss Toggle (Left side)
        toggle_container = QWidget()
        toggle_layout = QHBoxLayout()
        toggle_container.setLayout(toggle_layout)
        toggle_layout.setContentsMargins(0, 0, 0, 0)
        
        toggle_label = QLabel("Send Adaptive Stop Losses:")
        toggle_label.setStyleSheet("font-weight: bold;")
        toggle_layout.addWidget(toggle_label)
        
        self.adaptive_stop_toggle = QCheckBox()
        self.adaptive_stop_toggle.setChecked(False)
        self.adaptive_stop_toggle.stateChanged.connect(self.on_adaptive_stop_toggled)
        self.adaptive_stop_toggle.setText("OFF")
        toggle_layout.addWidget(self.adaptive_stop_toggle)
        
        status_layout.addWidget(toggle_container)
        
        # Add stretch to push GIF to the center
        status_layout.addStretch()
        
        # --- Add GIF in the middle ---
        self.gif_label = QLabel()
        self.movie = QMovie("mambo-ume-usume.gif")
        self.gif_label.setMovie(self.movie)
        # Scale the GIF to 75% of its original size
        self.movie.setScaledSize(QSize(111, 111))
        self.movie.start()
        self.gif_label.setFixedSize(QSize(111, 111))
        status_layout.addWidget(self.gif_label)
        
        # Add stretch to push status to the right
        status_layout.addStretch()
        
        # Connection Status
        self.status_label = QLabel("Status:")
        status_layout.addWidget(self.status_label)
        
        self.connection_status = QLabel("Disconnected")
        self.connection_status.setStyleSheet("color: red; font-weight: bold;")
        status_layout.addWidget(self.connection_status)
        
        # Add some spacing
        status_layout.addSpacing(20)
        
        # Data Pull Timestamp
        self.timestamp_label = QLabel("Data Pulled:")
        status_layout.addWidget(self.timestamp_label)
        
        self.last_pull_time = QLabel("Never")
        status_layout.addWidget(self.last_pull_time)
        
        layout.addWidget(status_container)

        # Tabs
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        # --- Terminal/Log View ---
        log_label = QLabel("Log Output:")
        log_label.setStyleSheet("font-weight: bold; margin-top: 10px;")
        layout.addWidget(log_label)
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        # A simple dark theme for the log view
        self.log_view.setStyleSheet("background-color: #2B2B2B; color: #A9B7C6; font-family: 'Courier New';")
        self.log_view.setMaximumHeight(200) # Give it a fixed max height
        layout.addWidget(self.log_view)

        # --- Positions Table Tab ---
        self.positions_tab = QWidget()
        self.positions_layout = QVBoxLayout()
        self.positions_tab.setLayout(self.positions_layout)
        self.tabs.addTab(self.positions_tab, "Positions")

        self.table = QTableWidget()
        self.table.setColumnCount(10)
        self.table.setHorizontalHeaderLabels([
            "Send", "Position", "ATR", "ATR Ratio", "Positions Held", "Current Price",
            "Computed Stop Loss", "$ Risk", "% Risk", "Status"
        ])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setSectionsMovable(True)
        self.table.setColumnWidth(0, 50) # "Send" column
        self.table.setColumnWidth(9, 240) # "Status" column
        self.positions_layout.addWidget(self.table)

        # --- Raw Data Tab ---
        self.raw_tab = QWidget()
        self.raw_layout = QVBoxLayout()
        self.raw_tab.setLayout(self.raw_layout)
        self.tabs.addTab(self.raw_tab, "Raw Data")

        self.raw_data_view = QTextEdit()
        self.raw_data_view.setReadOnly(True)
        self.raw_layout.addWidget(self.raw_data_view)

        # --- ATR Calculations Tab ---
        self.atr_calc_tab = QWidget()
        self.atr_calc_layout = QVBoxLayout()
        self.atr_calc_tab.setLayout(self.atr_calc_layout)
        self.tabs.addTab(self.atr_calc_tab, "ATR Calculations")

        self.atr_table = QTableWidget()
        self.atr_table.setColumnCount(4)
        self.atr_table.setHorizontalHeaderLabels([
            "Symbol", "Previous ATR", "TR (True Range)", "ATR (14)"
        ])
        self.atr_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.atr_table.verticalHeader().setDefaultSectionSize(60)  # Increase row height for multi-line text
        self.atr_calc_layout.addWidget(self.atr_table)
        self.atr_table.cellChanged.connect(self.on_atr_changed)
        # Setup auto-refresh timer (60 seconds = 60000 ms)
        self.refresh_timer = QTimer(self)
        self.refresh_timer.timeout.connect(self.start_full_refresh)
        self.refresh_timer.start(60000)  # Refresh every 60 seconds
        
        # Fetch data immediately on startup
        self.start_full_refresh()

    def log_to_ui(self, message):
        """Appends a message to the log view and auto-scrolls to the bottom."""
        self.log_view.append(message)
        # Ensure the view scrolls to the latest message
        self.log_view.verticalScrollBar().setValue(self.log_view.verticalScrollBar().maximum())    
    
    def closeEvent(self, event):
        """
        Overrides the default close event to ensure background threads are
        properly shut down before the application exits.
        """
        logging.info("Close event triggered. Attempting to stop worker thread...")
        # Check if a worker thread exists and is running
        if hasattr(self, 'thread') and self.thread and self.thread.isRunning():
            self.thread.quit()  # Ask the thread's event loop to exit
            if not self.thread.wait(5000):  # Wait up to 5 seconds for the thread to finish
                logging.warning("Worker thread did not terminate gracefully. Forcing termination.")
                self.thread.terminate() # As a last resort
        
        event.accept() # Proceed with closing the window

    def calculate_tr_and_atr(self, df, prior_atr, symbol=None):
        """
        Calculate True Range (TR) and Average True Range (ATR) for a given dataframe.
        
        Uses 15-minute candle data:
        - Current high/low are from the current 15-minute candle
        - Previous close is from the previous 15-minute candle
        
        TR is the maximum of:
        - Current High - Current Low
        - |Current High - Previous Close|
        - |Current Low - Previous Close|
        
        ATR is calculated using exponential smoothing:
        ATR = (Prior ATR × 13 + Current TR) / 14
        
        If the market data values (high, low, prev_close) are the same as the previous
        calculation, this indicates a market closure and the previous ATR is reused.
        
        Args:
            df: DataFrame with columns 'high', 'low', 'close' (15-minute candles)
            prior_atr: The previous ATR value (from previous 15-minute interval)
            symbol: Optional symbol name for tracking market data changes
        
        Returns:
            tuple: (current_tr, current_atr) or (None, None) if insufficient data
        """
        if len(df) < 2:
            return None, None

        # Get a definitive market status instead of inferring it from price data
        if symbol:
            if self.market_statuses.get(symbol) == 'CLOSED':
                logging.info(f"Market for {symbol} is CLOSED. Reusing previous ATR: {prior_atr:.2f}")
                # We still calculate TR for display, but return the prior ATR
                prev_close = df['close'].iloc[-2]
                current_high = df['high'].iloc[-1]
                current_low = df['low'].iloc[-1]
                current_tr = max(current_high - current_low, abs(current_high - prev_close), abs(current_low - prev_close))
                return current_tr, prior_atr

        # Calculate current TR
        prev_close = df['close'].iloc[-2]
        current_high = df['high'].iloc[-1]
        current_low = df['low'].iloc[-1]

        # True Range (current)
        tr1 = current_high - current_low
        tr2 = abs(current_high - prev_close)
        tr3 = abs(current_low - prev_close)
        current_tr = max(tr1, tr2, tr3)

        # If TR is 0, keep ATR unchanged
        if current_tr == 0:
            print(f"TR is 0 for {symbol if symbol else 'symbol'} - keeping ATR unchanged at {prior_atr:.2f}")
            return current_tr, prior_atr

        # ATR using the calculated prior ATR from previous 15-minute interval
        # Formula: ATR = (Prior ATR × 13 + Current TR) / 14
        current_atr = (prior_atr * 13 + current_tr) / 14

        # Fallback for when prior_atr is None (e.g., first run for a symbol)
        if prior_atr is None:
            logging.warning(f"No prior ATR for {symbol}. Calculating ATR from historical bars.")
            try:
                # Use the ib_insync utility to calculate ATR from the full bar series
                atr_series = util.ATR(df['high'], df['low'], df['close'], 14)
                if atr_series is not None and not atr_series.empty:
                    current_atr = atr_series.iloc[-1] # Get the most recent ATR value
            except Exception as e:
                logging.error(f"Error during fallback ATR calculation for {symbol}: {e}")
                return current_tr, None # Return None if fallback fails
        return current_tr, current_atr

    def populate_positions_table(self):
        self.table.setRowCount(len(self.all_positions_details))
        for i, p_data in enumerate(self.all_positions_details):
            try:
                pos = p_data['position']
                symbol = p_data['symbol']
                
                # Column 0: Position
                # Column 0: "Send Stop" Checkbox
                checkbox_widget = QWidget()
                checkbox_layout = QHBoxLayout(checkbox_widget)
                checkbox = QCheckBox()
                checkbox.setChecked(self.symbol_stop_enabled.get(symbol, True))
                checkbox.stateChanged.connect(lambda state, s=symbol: self.on_symbol_toggle_changed(s, state))
                checkbox_layout.addWidget(checkbox)
                checkbox_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
                checkbox_layout.setContentsMargins(0,0,0,0)
                self.table.setCellWidget(i, 0, checkbox_widget)

                # Column 1: Position
                market_status = self.market_statuses.get(symbol, 'UNKNOWN')
                position_item = QTableWidgetItem(pos)

                # Create a colored circle icon
                pixmap = QtGui.QPixmap(16, 16)
                if market_status == 'OPEN':
                    pixmap.fill(Qt.GlobalColor.green)
                elif market_status == 'CLOSED':
                    pixmap.fill(Qt.GlobalColor.blue)
                else:  # UNKNOWN or other
                    pixmap.fill(Qt.GlobalColor.gray)
                
                icon = QtGui.QIcon(pixmap)
                position_item.setIcon(icon)
                self.table.setItem(i, 1, position_item)
                
                # Column 2: ATR - Get ATR value from ATR calculations tab
                atr_value = None
                atr_display = "N/A"
                if symbol in self.atr_symbols: # Use the full precision ATR value for calculation
                    atr_index = self.atr_symbols.index(symbol)
                    if self.atr_calculated[atr_index] is not None:
                        atr_value = self.atr_calculated[atr_index]
                        atr_display = f"{atr_value:.4f}" # Display with more precision
                self.table.setItem(i, 2, QTableWidgetItem(atr_display))

                # Column 3: ATR Ratio editable spin box
                spin = QDoubleSpinBox()
                spin.setMinimum(0.1)
                spin.setMaximum(10.0)
                spin.setSingleStep(0.1)
                spin.setDecimals(1)
                spin.setValue(self.atr_ratios[i])
                spin.setButtonSymbols(QAbstractSpinBox.ButtonSymbols.NoButtons)
                spin.valueChanged.connect(lambda val, row=i: self.update_atr_ratio(row, val))
                self.table.setCellWidget(i, 3, spin)

                # Column 4: Positions Held
                self.table.setItem(i, 4, QTableWidgetItem(str(p_data['positions_held'])))
                
                # Column 5: Current Price
                self.table.setItem(i, 5, QTableWidgetItem(f"{p_data['current_price']:.2f}"))

                # Column 6: Computed Stop Loss = Current Price - (ATR × ATR Ratio)
                # Stop loss never goes down - use highest computed value
                computed_stop = self.compute_stop_loss(p_data, p_data['current_price'], atr_value, self.atr_ratios[i])
                self.computed_stop_losses[i] = computed_stop # Store the final computed stop
                stop_item = QTableWidgetItem(f"{computed_stop:.2f}" if computed_stop else "N/A")
                self.table.setItem(i, 6, stop_item)

                # Column 7: $ Risk (MinTick column was removed)
                contract_details = p_data.get('contract_details', {})
                risk_value = 0
                if computed_stop and p_data['current_price'] > 0:
                    risk_in_points = p_data['current_price'] - computed_stop
                    # Ensure risk_in_points is always positive, as risk is an absolute value
                    risk_in_points = abs(risk_in_points)

                    multiplier = p_data.get('multiplier', 1.0)

                    # Use the centralized get_point_value function for accurate dollar risk calculation
                    # This uses explicit per-contract metadata when available, with fallback logic
                    point_value = get_point_value(symbol, contract_details, multiplier)

                    # Total Risk = (Absolute Price Difference) * (Point Value) * (Quantity)
                    risk_value = risk_in_points * point_value * abs(p_data['positions_held'])
                risk_item = QTableWidgetItem(f"${risk_value:,.2f}")
                self.table.setItem(i, 7, risk_item)

                # Column 8: % Risk
                hypothetical_account_value = 6000.0
                percent_risk = 0.0
                if hypothetical_account_value > 0:
                    percent_risk = (risk_value / hypothetical_account_value) * 100
                percent_risk_item = QTableWidgetItem(f"{percent_risk:.2f}%")

                # Turn the text red if risk is over 2%
                if percent_risk > 2.0:
                    percent_risk_item.setForeground(QtGui.QColor('red'))

                self.table.setItem(i, 8, percent_risk_item)

                # Column 9: Status
                # Note: self.statuses[i] corresponds to the index of the symbol in self.symbols, not necessarily p_data
                self.table.setItem(i, 9, QTableWidgetItem(self.statuses[i]))
            except Exception as e:
                symbol = p_data.get('symbol', 'UNKNOWN')
                logging.error(f"Error populating table for symbol {symbol}: {e}")
                # Optionally, display an error in the row
                error_item = QTableWidgetItem(f"Error: {e}")
                error_item.setForeground(QtGui.QColor('red'))
                self.table.setItem(i, 1, QTableWidgetItem(symbol))
                self.table.setItem(i, 9, error_item)

    def on_symbol_toggle_changed(self, symbol, state):
        """Handles when a user toggles the checkbox for an individual symbol."""
        is_enabled = state == Qt.CheckState.Checked.value
        self.symbol_stop_enabled[symbol] = is_enabled
        logging.info(f"Stop loss submission for {symbol} set to: {'ENABLED' if is_enabled else 'DISABLED'}")

    def compute_stop_loss(self, position_data, current_price, atr_value, atr_ratio, apply_ratchet=True):
        """
        Compute stop loss = Current Price - (ATR × ATR Ratio)
        - Stop loss never goes down (ratchets up).
        - Rounds to the contract's minimum tick size.
        - Rounds down for long positions (SELL stop), up for short positions (BUY stop).
        - `apply_ratchet` flag controls whether to enforce the "never goes down" rule.
        """
        symbol = position_data['symbol']

        if atr_value is None or current_price <= 0:
            return self.highest_stop_losses.get(symbol, 0)
        
        # --- Calculate Stop Price and Round to minTick using Decimal for precision ---
        raw_stop = current_price - (atr_value * atr_ratio)
        
        contract_details = position_data.get('contract_details', {})
        min_tick = contract_details.get('minTick') 
        quantity = position_data.get('positions_held', 0)

        final_stop = float(raw_stop) # Default to float if no rounding is needed
        if min_tick > 0 and quantity != 0:
            # Use Decimal for accurate rounding to the specified tick size.
            # We round down to be more conservative (lower stop for longs, higher stop for shorts is handled by the subtraction).
            # A lower SELL stop or a lower BUY stop (for shorts) is always safer.
            raw_stop_decimal = Decimal(str(raw_stop))
            min_tick_decimal = Decimal(str(min_tick))
            
            # Perform rounding: (value / tick_size).quantize(ROUND_DOWN) * tick_size
            rounded_stop_decimal = (raw_stop_decimal / min_tick_decimal).quantize(Decimal('1'), rounding=ROUND_DOWN) * min_tick_decimal
            final_stop = float(rounded_stop_decimal)
            logging.debug(f"Rounding for {symbol}: raw={raw_stop}, minTick={min_tick}, final={final_stop}")

        # Get previous highest stop loss for this symbol
        prev_highest = self.highest_stop_losses.get(symbol, 0)
        
        if apply_ratchet:
            # Stop loss never goes down - use max of new and previous
            if final_stop > prev_highest:
                self.highest_stop_losses[symbol] = final_stop
                return final_stop
            else:
                return prev_highest
        else:
            return final_stop # Return the raw computed value if not ratcheting

    def populate_atr_table(self):
        """Populate the ATR Calculations table"""
        # Temporarily disconnect the signal to prevent it from firing during population
        self.atr_table.cellChanged.disconnect(self.on_atr_changed)

        self.atr_table.setRowCount(len(self.atr_symbols))
        for i in range(len(self.atr_symbols)):
            # Symbol (read-only)
            symbol_item = QTableWidgetItem(self.atr_symbols[i])
            symbol_item.setFlags(symbol_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.atr_table.setItem(i, 0, symbol_item)

            # Previous ATR (read-only)
            prev_atr = self.previous_atr_values[i]
            if prev_atr is None:
                prev_atr_item = QTableWidgetItem("N/A")
            else:
                prev_atr_item = QTableWidgetItem(f"{prev_atr:.2f}")
            prev_atr_item.setFlags(prev_atr_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.atr_table.setItem(i, 1, prev_atr_item)

            # TR (read-only)
            tr_item = QTableWidgetItem(f"{self.tr_values[i]:.2f}")
            tr_item.setFlags(tr_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.atr_table.setItem(i, 2, tr_item)

            # ATR (editable)
            atr_item = QTableWidgetItem(f"{self.atr_calculated[i]:.2f}" if self.atr_calculated[i] is not None else "N/A")
            self.atr_table.setItem(i, 3, atr_item)

        # Reconnect the signal
        self.atr_table.cellChanged.connect(self.on_atr_changed)

    def load_atr_history(self):
        """Load ATR history and user overrides from JSON file"""
        if os.path.exists(self.atr_history_file):
            try:
                with open(self.atr_history_file, 'r') as f:
                    data = json.load(f)
                    # For backward compatibility, handle old format
                    if isinstance(data, dict) and 'atr_history' in data:
                        return data.get('atr_history', {}), data.get('user_overrides', {})
                    return data, {} # Old format, return empty overrides
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading ATR history: {e}")
                return {}, {}
        return {}, {}
    
    def save_atr_history(self):
        """Save ATR history to JSON file"""
        try:
            with open(self.atr_history_file, 'w') as f:
                # Save both history and overrides in one file
                data_to_save = {
                    'atr_history': self.atr_history,
                    'user_overrides': self.user_overrides
                }
                json.dump(data_to_save, f, indent=2)
            logging.info("ATR history saved successfully")
        except Exception as e:
            print(f"Error saving ATR history: {e}")
    
    def get_previous_atr(self, symbol):
        """Get the most recent ATR for a symbol from the last 15-minute interval, or None if not available
        
        If no ATR is found from exactly 15 minutes ago (e.g., during market closures),
        this will return the last available ATR value to account for nights and weekends.
        """
        if symbol not in self.atr_history:
            return None
        
        # Get current time rounded down to nearest 15-minute interval
        now = datetime.now()
        current_interval = self.get_15min_interval_key(now)
        
        # Get all timestamps for this symbol and sort them (most recent first)
        timestamps = sorted(self.atr_history[symbol].keys(), reverse=True)
        
        if not timestamps:
            return None
        
        # Find the most recent ATR value that's not from the current interval
        for timestamp in timestamps:
            if timestamp != current_interval:
                return self.atr_history[symbol][timestamp]
        
        # If all timestamps are from current interval (unlikely), return the most recent one
        # This handles edge cases where we only have current interval data
        return self.atr_history[symbol][timestamps[0]]
    
    def get_15min_interval_key(self, dt=None):
        """Get the 15-minute interval key for a given datetime (or current time)"""
        if dt is None:
            dt = datetime.now()
        
        # Round down to nearest 15-minute interval
        minute = (dt.minute // 15) * 15
        interval_time = dt.replace(minute=minute, second=0, microsecond=0)
        
        # Format: YYYY-MM-DD HH:MM
        return interval_time.strftime('%Y-%m-%d %H:%M')
    
    def cleanup_old_atr_data(self):
        """Remove ATR data older than 7 days (1 week)"""
        cutoff_time = datetime.now() - timedelta(days=7)
        
        symbols_to_remove = []
        for symbol in self.atr_history:
            timestamps_to_remove = []
            for timestamp in self.atr_history[symbol]:
                try:
                    # Parse the timestamp
                    timestamp_dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M')
                    # If older than 24 hours, mark for removal
                    if timestamp_dt < cutoff_time:
                        timestamps_to_remove.append(timestamp)
                except ValueError:
                    # Handle old format (date only) - remove it
                    try:
                        timestamp_dt = datetime.strptime(timestamp, '%Y-%m-%d')
                        if timestamp_dt < cutoff_time:
                            timestamps_to_remove.append(timestamp)
                    except ValueError:
                        print(f"Invalid timestamp format: {timestamp}")
                        timestamps_to_remove.append(timestamp)
            
            # Remove old timestamps
            for timestamp in timestamps_to_remove:
                del self.atr_history[symbol][timestamp]
                logging.info(f"Removed old ATR data for {symbol} at {timestamp}")
                # Also remove from user_overrides if it exists
                if symbol in self.user_overrides and timestamp in self.user_overrides[symbol]:
                    del self.user_overrides[symbol][timestamp]
                    logging.info(f"Removed old user override for {symbol} at {timestamp}")
            
            # If symbol has no more data, mark for removal
            if not self.atr_history[symbol]:
                symbols_to_remove.append(symbol)
        
        # Remove symbols with no data
        for symbol in symbols_to_remove:
            del self.atr_history[symbol]
            if symbol in self.user_overrides:
                del self.user_overrides[symbol]
            logging.info(f"Removed symbol {symbol} (no data remaining)")
        
        # Save the cleaned history
        if timestamps_to_remove or symbols_to_remove:
            self.save_atr_history()
    
    def save_today_atr(self, symbol, atr_value):
        """Save ATR for a symbol at the current 15-minute interval"""
        interval_key = self.get_15min_interval_key()
        
        if symbol not in self.atr_history:
            self.atr_history[symbol] = {}
        
        # Check if we're overwriting an existing value for this interval
        if interval_key in self.atr_history[symbol]:
            old_value = self.atr_history[symbol][interval_key]
            logging.info(f"Overwriting existing ATR for {symbol} at {interval_key}: {old_value:.2f} -> {atr_value:.2f}")
        else:
            logging.info(f"Saving new ATR for {symbol} at {interval_key}: {atr_value:.2f}")
        
        # Save/overwrite this interval's value
        self.atr_history[symbol][interval_key] = atr_value
        
        # Clean up old data before saving
        self.cleanup_old_atr_data()
        
        self.save_atr_history()

    def on_atr_changed(self, row, column):
        """Handle when a user manually edits the ATR value."""
        if column == 3:  # "ATR (14)" column
            item = self.atr_table.item(row, column)
            if not item:
                return

            try:
                new_atr_value = float(item.text())
                symbol = self.atr_table.item(row, 0).text()

                # Update the internal data structure
                self.atr_calculated[row] = new_atr_value

                # Save the user-inputted value to history for the current interval
                logging.info(f"User manually set ATR for {symbol} to {new_atr_value:.2f}")
                
                # Mark this interval as a user override
                interval_key = self.get_15min_interval_key()
                if symbol not in self.user_overrides:
                    self.user_overrides[symbol] = {}
                self.user_overrides[symbol][interval_key] = True

                self.save_today_atr(symbol, new_atr_value)

                # Repopulate the positions table to update computed stop loss
                self.populate_positions_table()

            except ValueError:
                logging.warning(f"Invalid input for ATR: '{item.text()}'. Please enter a number.")
                # Optionally, revert to the old value or show an error
                if self.atr_calculated[row] is not None:
                    item.setText(f"{self.atr_calculated[row]:.2f}")

    def start_single_atr_recalc_worker(self, row, symbol, prior_atr):
        """Starts a worker to recalculate ATR for a single symbol asynchronously."""
        # We can reuse the DataWorker if we give it a specific task, or create a new one.
        # For simplicity, let's create a small, dedicated async runner.
        
        async def task():
            ib = IB()
            try:
                client_id = random.randint(100, 999)
                await ib.connectAsync('127.0.0.1', 7497, clientId=client_id)
                await self.recalculate_single_symbol_atr_async(ib, row, symbol, prior_atr)
            except Exception as e:
                logging.error(f"Error in single ATR recalc worker for {symbol}: {e}")
            finally:
                if ib.isConnected():
                    ib.disconnect()

        # We need a way to run this async task in a separate thread.
        # The simplest way is to use a QThread and a QObject runner.
        class Runner(QObject):
            finished = pyqtSignal()
            def run(self):
                asyncio.run(task())
                self.finished.emit()

        self.recalc_thread = QThread()
        self.recalc_runner = Runner()
        self.recalc_runner.moveToThread(self.recalc_thread)
        self.recalc_thread.started.connect(self.recalc_runner.run)
        self.recalc_runner.finished.connect(self.recalc_thread.quit)
        self.recalc_runner.finished.connect(self.recalc_runner.deleteLater)
        self.recalc_thread.finished.connect(self.recalc_thread.deleteLater)
        self.recalc_thread.start()

    async def recalculate_single_symbol_atr_async(self, ib, row, symbol, prior_atr):
        """Recalculates ATR for a single symbol asynchronously using a provided IB connection."""
        contract_info = self.contract_details_map.get(symbol, {})
        
        if not contract_info.get('conId'):
            logging.error(f"No conId available for {symbol} to recalculate ATR.")
            return

        contract = Contract(conId=contract_info['conId'], exchange=contract_info.get('exchange', ''))
        await ib.qualifyContractsAsync(contract)
        logging.info(f"Recalculating ATR for conId {contract.conId} on exchange '{contract.exchange}'")

        bars = await ib.reqHistoricalDataAsync(
            contract, endDateTime='', durationStr='1 D', barSizeSetting='15 mins',
            whatToShow='TRADES', useRTH=True, formatDate=1
        )
        
        if bars and len(bars) >= 2:
            df = util.df(bars)
            tr, atr = self.calculate_tr_and_atr(df, prior_atr=prior_atr, symbol=symbol)

            if tr is not None and atr is not None:
                # Since this runs in a background thread, we need to update UI elements
                # safely. We can store the results and update the UI in the main thread,
                # but for this direct update, let's assume it's safe enough for now.
                # A more robust solution would use signals.
                self.tr_values[row] = tr

    async def calculate_atr_for_symbols(self, ib, symbols, market_statuses, contract_details_map):
        """Calculate ATR for each symbol in the list"""
        # This function is now async and uses the provided 'ib' instance.
        # It no longer creates its own connection.
        atr_symbols = []
        tr_values = []
        atr_calculated = []
        previous_atr_values = []

        for symbol in symbols:
            try:
                # Get previous ATR for the current symbol
                previous_atr = self.get_previous_atr(symbol)
                
                contract_info = contract_details_map.get(symbol, {})
                if contract_info.get('conId'):
                    # Use conId and exchange for most accurate contract identification
                    contract = Contract(
                        conId=contract_info['conId'],
                        exchange=contract_info.get('exchange', '') # Explicitly add exchange
                    )
                    await ib.qualifyContractsAsync(contract)
                    logging.info(f"Using conId {contract_info['conId']} and exchange '{contract.exchange}' for {symbol}")
                elif contract_info.get('lastTradeDateOrContractMonth'):
                    # Use actual expiration date from position
                    contract = Future(
                        symbol=symbol,
                        lastTradeDateOrContractMonth=contract_info['lastTradeDateOrContractMonth'],
                        exchange=contract_info.get('exchange', 'CME'),
                        currency=contract_info.get('currency', 'USD')
                    )
                    await ib.qualifyContractsAsync(contract)
                    print(f"Using expiration {contract_info['lastTradeDateOrContractMonth']} for {symbol}")
                else:
                    # Fallback - skip if we don't have contract details
                    print(f"No contract details available for {symbol}, skipping ATR calculation")
                    continue
                
                # Get historical bars (1 day of 15-minute candles for ATR calculation)
                bars = await ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime='',
                    durationStr='1 D',
                    barSizeSetting='15 mins',
                    whatToShow='TRADES',
                    useRTH=True,
                    formatDate=1,
                    keepUpToDate=False
                )
                
                if not bars or len(bars) < 2:
                    print(f"Not enough historical data for {symbol}")
                    continue
                
                df = util.df(bars)
                
                if df is not None and not df.empty:
                    # Calculate TR always (doesn't need prior ATR)
                    prev_close = df['close'].iloc[-2]
                    current_high = df['high'].iloc[-1]
                    current_low = df['low'].iloc[-1]
                    tr1 = current_high - current_low
                    tr2 = abs(current_high - prev_close)
                    tr3 = abs(current_low - prev_close)
                    current_tr = max(tr1, tr2, tr3)
                    
                    # Calculate ATR at the END of the interval (when candle closes)
                    # The last closed candle represents the most recently COMPLETED interval
                    # We calculate ATR for that completed interval, not the current one
                    
                    # Get the interval key for the last CLOSED candle (most recent completed interval)
                    # The last bar in the data is the most recently closed 15-min candle
                    last_bar_time = bars[-1].date
                    completed_interval_key = self.get_15min_interval_key(last_bar_time)
                    
                    # Check if ATR exists and was a user override. If so, don't recalculate.
                    is_user_override = self.user_overrides.get(symbol, {}).get(completed_interval_key, False)
                    if is_user_override and completed_interval_key in self.atr_history.get(symbol, {}):
                        current_atr = self.atr_history[symbol][completed_interval_key]
                        logging.info(f"User override for {symbol} exists for interval {completed_interval_key}. Using value: {current_atr:.2f}")
                    elif symbol in self.atr_history and completed_interval_key in self.atr_history[symbol] and not is_user_override:
                        # The value was calculated previously, but it wasn't a user override, so we can re-calculate it to be safe.
                        logging.info(f"Recalculating ATR for {symbol} for interval {completed_interval_key} as it was not a user override.")
                        tr, current_atr = self.calculate_tr_and_atr(df, prior_atr=previous_atr, symbol=symbol)
                        # Even if we skip calculation, ensure the value is saved for this interval if it's somehow missing
                        if completed_interval_key not in self.atr_history.get(symbol, {}):
                            self.atr_history[symbol][completed_interval_key] = current_atr
                            self.save_atr_history()

                    elif previous_atr is not None:
                        # Calculate new ATR for the completed interval
                        tr, current_atr = self.calculate_tr_and_atr(df, prior_atr=previous_atr, symbol=symbol)
                        logging.info(f"Symbol: {symbol}, Calculating ATR for completed interval {completed_interval_key}")
                        logging.info(f"  Previous ATR: {previous_atr:.2f}, TR: {tr:.2f}, New ATR: {current_atr:.2f}")
                        
                        # Save ATR for the completed interval
                        if symbol not in self.atr_history:
                            self.atr_history[symbol] = {}
                        self.atr_history[symbol][completed_interval_key] = current_atr
                        self.save_atr_history()
                    else:
                        # If there's no previous ATR, we can't calculate a new one.
                        current_atr = None
                        logging.warning(f"Symbol: {symbol}, TR: {current_tr:.2f}, ATR: N/A (No previous ATR from 15-min interval)")

                    
                    # Store the values
                    atr_symbols.append(symbol)
                    tr_values.append(current_tr)
                    atr_calculated.append(current_atr)
                    previous_atr_values.append(previous_atr)
            
            except Exception as e:
                print(f"Error calculating ATR for {symbol}: {e}")
                continue
        
        return {
            'atr_symbols': atr_symbols,
            'tr_values': tr_values,
            'atr_calculated': atr_calculated,
            'previous_atr_values': previous_atr_values
        }

    def start_full_refresh(self):
        """Starts the first stage of the data loading sequence."""
        self.update_status(False) # Show as disconnected/refreshing
        self.connection_status.setText("Refreshing...")
        self.connection_status.setStyleSheet("color: orange; font-weight: bold;")
        self.start_worker()

    def start_worker(self):
        """Creates and starts a single worker for the entire refresh cycle."""
        # If a thread is already running, don't start a new one.
        if hasattr(self, 'thread') and self.thread and self.thread.isRunning():
            logging.warning("Refresh already in progress. Skipping new request.")
            return

        self.thread = QThread()
        self.worker = DataWorker(self, self.atr_ratios, self.highest_stop_losses) # self.symbol_stop_enabled is now passed via `self`
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.on_worker_finished)
        self.worker.error.connect(self.handle_data_error)
        self.worker.log_message.connect(self.log_to_ui)

        # Connect all signals at once
        self.worker.positions_ready.connect(self.handle_positions_ready)
        self.worker.market_data_ready.connect(self.handle_market_data_ready)
        self.worker.atr_ready.connect(self.handle_atr_ready)
        self.worker.orders_submitted.connect(self.handle_orders_submitted)

        self.thread.start()
        logging.info("Worker started for full refresh cycle.")

    def on_worker_finished(self):
        """Called when the worker's run() method completes."""
        logging.info("Worker has finished all stages.")
        self.update_status(True)
        self.update_timestamp()

    def update_timestamp(self):
        """Updates the 'Data Pulled' timestamp in the UI."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.last_pull_time.setText(timestamp)

        # Cleanly shut down the thread and worker
        if self.thread and self.thread.isRunning():
            self.thread.quit()
            self.thread.wait()
        
        if hasattr(self, 'worker') and self.worker:
            # Schedule for deletion and clear the Python reference
            self.worker.deleteLater()
            self.worker = None
        if hasattr(self, 'thread') and self.thread:
            # Schedule for deletion and clear the Python reference
            self.thread.deleteLater()
            self.thread = None

    def handle_positions_ready(self, positions_data):
        """Stage 1: Basic positions are ready. Populate table with what we have."""
        logging.info(f"Stage 1 Complete: Received {len(positions_data)} basic positions.")
        if not positions_data:
            self.raw_data_view.setPlainText("No positions returned from IBKR")
            self.table.setRowCount(0)
            self.atr_table.setRowCount(0)
            return
        
        self.update_ui_with_position_data(positions_data)

    def handle_market_data_ready(self, positions_data):
        """Stage 2: Market data is ready. Repopulate table with prices."""
        logging.info("Stage 2 Complete: Received market data.")
        self.positions_data = positions_data # Store the latest data
        self.update_ui_with_position_data(positions_data)

    def handle_atr_ready(self, atr_results):
        """Stage 3: ATR data is ready. Populate ATR table and update positions table."""
        logging.info("Stage 3 Complete: Received ATR calculations.")
        self.atr_symbols = atr_results.get('atr_symbols', [])
        self.tr_values = atr_results.get('tr_values', [])
        self.atr_calculated = atr_results.get('atr_calculated', [])
        self.previous_atr_values = atr_results.get('previous_atr_values', [])
        self.populate_atr_table()
        self.populate_positions_table() # Re-populate to show ATR and computed stops

    def handle_orders_submitted(self, order_results):
        """Stage 4: Order submission is complete. Update statuses."""
        logging.info("Stage 4 Complete: Processed order submissions.")
        self.process_order_results(order_results)

    def update_ui_with_position_data(self, positions_data):
        """A helper to update UI state from a list of position data dicts."""
        old_atr_ratios_map = {sym: ratio for sym, ratio in zip(self.symbols, self.atr_ratios)}

        self.all_positions_details.clear()
        self.positions.clear()
        self.atr_ratios.clear() # Will be repopulated
        self.statuses.clear() # Will be repopulated
        self.symbols.clear() # Will be repopulated
        self.positions_held.clear()
        self.current_prices.clear()
        self.multipliers.clear()
        self.computed_stop_losses.clear()

        display_text = ""
        for p in positions_data:
            self.all_positions_details.append(p)
            self.positions.append(p['position'])
            self.symbols.append(p['symbol'])
            self.positions_held.append(p['positions_held'])
            self.current_prices.append(p['current_price'])
            self.multipliers.append(p.get('multiplier', 1.0))
            self.contract_details_map[p['symbol']] = p['contract_details']

            self.atr_ratios.append(old_atr_ratios_map.get(p['symbol'], 1.5))
            self.statuses.append("Updating...")
            self.computed_stop_losses.append(0) # Initialize with 0
            raw_line = f"{p['symbol']} | Qty: {p['positions_held']} | Avg Cost: ${p['avg_cost']:.2f} | Price: ${p['current_price']:.2f} | P/L: ${p.get('unrealized_pl', 0):.2f} ({p.get('pl_percent', 0):.2f}%)"
            display_text += raw_line + "\n"

        self.raw_data_view.setPlainText(display_text)
        self.populate_positions_table()

    def handle_data_error(self, error_message):
        """Slot to handle errors from the worker thread."""
        logging.error(f"Error in worker thread: {error_message}")
        self.raw_data_view.setPlainText(f"Error fetching data: {error_message}")
        self.update_status(False)

    def process_order_results(self, results):
        """Updates the UI based on the results of order submissions."""
        if not results:
            logging.info("No order submission results to process.")
            return

        for result in results:
            symbol = result.get('symbol', 'Unknown')
            status = result.get('status', 'unknown')
            
            if symbol in self.symbols:
                idx = self.symbols.index(symbol)
                timestamp = datetime.now().strftime("%H:%M:%S")
                message = result.get('message', 'Unknown')

                if status in ['submitted', 'unchanged']:
                    self.statuses[idx] = f"Order Updated - {timestamp}"
                elif status == 'held':
                    self.statuses[idx] = f"Held - {message}"
                elif status == 'pending': # IBKR rejected or has non-final status
                    self.statuses[idx] = f"Order Rejected - {message}"
                elif status in ['error', 'skipped']:
                    self.statuses[idx] = f"Error - {message}"
        self.populate_positions_table()

    def on_adaptive_stop_toggled(self, state):
        """Handles the state change of the adaptive stop loss toggle switch."""
        if state == Qt.CheckState.Checked.value:
            self.send_adaptive_stops = True
            self.adaptive_stop_toggle.setText("ON")
            self.log_to_ui(">>> Adaptive stop loss submission ENABLED <<<")
        else:
            self.send_adaptive_stops = False
            self.adaptive_stop_toggle.setText("OFF")
            self.log_to_ui(">>> Adaptive stop loss submission DISABLED <<<")

    def update_status(self, connected):
        """Updates the connection status label in the UI."""
        if connected:
            self.connection_status.setText("Connected")
            self.connection_status.setStyleSheet("color: green; font-weight: bold;")
        else:
            self.connection_status.setText("Disconnected")
            self.connection_status.setStyleSheet("color: red; font-weight: bold;")


def main():
    app = QApplication(sys.argv)
    window = ATRWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
