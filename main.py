# main.py
import sys
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
from PyQt6.QtGui import QMovie, QColor
from ibkr_api import get_market_statuses_for_all, fetch_basic_positions, fetch_market_data_for_positions
from ib_insync import IB, util, Future, Contract, StopOrder
import math
import asyncio
import struct
from decimal import Decimal, ROUND_DOWN

from calculator import PortfolioCalculator
from utils import get_point_value # Import from the new utils file
# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
ATR_HISTORY_FILE = 'atr_history.json'


class DataWorker(QObject):
    """
    Worker thread for fetching and processing all IBKR data.
    This runs in the background to keep the UI responsive.
    """
    finished = pyqtSignal()
    error = pyqtSignal(str)
    # This single signal will carry all the calculated data for the UI
    data_ready = pyqtSignal(list)
    orders_submitted = pyqtSignal(list)
    log_message = pyqtSignal(str) # Signal to send log messages to the UI
    
    def __init__(self, atr_window):
        super().__init__()
        # Give worker access to the main window's methods and data
        self.atr_window = atr_window
        self.send_adaptive_stops = atr_window.send_adaptive_stops
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
            
            # --- Stage 2: Fetch Market Data ---
            enriched_positions = await fetch_market_data_for_positions(ib, basic_positions)
            
            # --- Stage 3: Calculations (ATR, Stops, Risk) ---
            contract_details_map = {p['symbol']: p['contract_details'] for p in enriched_positions}
            market_statuses = await get_market_statuses_for_all(ib, contract_details_map)
            self.atr_window.market_statuses = market_statuses # Update main window
            
            symbols = [p['symbol'] for p in enriched_positions]
            atr_results = await self.atr_window.calculate_atr_for_symbols(ib, symbols, market_statuses, contract_details_map)

            # --- Instantiate and use the calculator ---
            # Pass copies of state to ensure thread safety
            atr_ratios_map = {p['symbol']: self.atr_window.get_atr_ratio_for_symbol(p['symbol']) for p in enriched_positions}
            
            calculator = PortfolioCalculator(
                self.atr_window.atr_history.copy(),
                self.atr_window.user_overrides.copy(),
                self.atr_window.highest_stop_losses.copy(),
                atr_ratios_map,
                market_statuses
            )
            final_positions_data = calculator.process_positions(enriched_positions, atr_results)
            self.data_ready.emit(final_positions_data)

            # --- Stage 4: Submit Orders ---
            # Read the latest state of the toggle directly from the window
            if not self.atr_window.send_adaptive_stops:
                logging.info("Adaptive stops are disabled, skipping order submission.")
                self.orders_submitted.emit([])
                return

            # The main window now holds the definitive list of positions
            stop_loss_data = self.build_stop_loss_data(final_positions_data)
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


    def build_stop_loss_data(self, processed_positions):
        """Prepares the data structure for submitting stop loss orders."""
        orders_to_submit = {}
        statuses_only = []

        for p_data in processed_positions:
            symbol = p_data['symbol']
            final_stop_price = p_data.get('computed_stop_loss', 0)

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

            # Ratchet check: The calculator already applied this logic.
            # We just need to check if the final stop price is the same as the held-over one.
            is_long = p_data['positions_held'] > 0
            held_stop = (is_long and final_stop_price < p_data['current_price'] and final_stop_price == highest_stop) or \
                        (not is_long and final_stop_price > p_data['current_price'] and final_stop_price == highest_stop)

            if held_stop and final_stop_price != 0:
                logging.info(f"Worker: Holding stop for {symbol} at {final_stop_price:.4f}")
                statuses_only.append({
                    'symbol': symbol,
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
        self.setGeometry(100, 100, 1400, 800)

        # Data stores
        self.positions_data = [] # This will hold the fully processed data from the worker
        self.highest_stop_losses = {}  # Track highest stop loss per symbol (never goes down)
        self.contract_details_map = {}  # Store contract details by symbol
        self.symbol_stop_enabled = {}  # {symbol: bool} to track individual stop toggles

        # ATR calculation data
        self.atr_symbols = []
        self.tr_values = []
        self.atr_calculated = []
        self.previous_atr_values = []
        # ATR history file path
        self.atr_history_file = os.path.join(os.path.dirname(__file__), 'atr_history.json')
        
        # Load ATR history
        self.atr_history, self.user_overrides = self.load_atr_history()

        # --- New: User Settings File and Loading ---
        self.user_settings_file = os.path.join(os.path.dirname(__file__), 'user_settings.json')
        self.symbol_stop_enabled = self.load_user_settings()

        # Adaptive Stop Loss toggle state
        self.send_adaptive_stops = False
        self.market_statuses = {}  # {symbol: 'OPEN' | 'CLOSED' | 'UNKNOWN'}

        # Threading
        self.worker_thread = None

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
        self.table.setColumnWidth(9, 180) # "Status" column
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
        if self.worker_thread and self.worker_thread.isRunning():
            self.worker_thread.quit()
            if not self.worker_thread.wait(5000):
                logging.warning("Worker thread did not terminate gracefully. Forcing termination.")
                self.worker_thread.terminate()

        self.save_user_settings() # Save checkbox states on exit
        event.accept() # Proceed with closing the window

    def populate_positions_table(self):
        """Populates the main table with fully processed data. No calculations here."""
        self.table.setRowCount(len(self.positions_data))
        for i, p_data in enumerate(self.positions_data):
            try:
                symbol = p_data['symbol']
                
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
                market_status = self.market_statuses.get(symbol, 'UNKNOWN') # From main window state
                position_item = QTableWidgetItem(p_data['position'])

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
                atr_value = p_data.get('atr_value')
                atr_display = f"{atr_value:.4f}" if atr_value is not None else "N/A"
                self.table.setItem(i, 2, QTableWidgetItem(atr_display)) # Display with more precision

                # Column 3: ATR Ratio editable spin box
                spin = QDoubleSpinBox()
                spin.setMinimum(0.1)
                spin.setMaximum(10.0)
                spin.setSingleStep(0.1)
                spin.setDecimals(1)
                spin.setValue(p_data.get('atr_ratio', 1.5))
                spin.setButtonSymbols(QAbstractSpinBox.ButtonSymbols.NoButtons)
                spin.valueChanged.connect(lambda val, row=i: self.update_atr_ratio(row, val))
                self.table.setCellWidget(i, 3, spin)

                # Column 4: Positions Held
                self.table.setItem(i, 4, QTableWidgetItem(str(p_data['positions_held'])))
                
                # Column 5: Current Price
                self.table.setItem(i, 5, QTableWidgetItem(f"{p_data.get('current_price', 0):.2f}"))

                # Column 6: Computed Stop Loss
                computed_stop = p_data.get('computed_stop_loss')
                stop_display = f"{computed_stop:.4f}" if computed_stop is not None else "N/A"
                stop_item = QTableWidgetItem(stop_display)
                self.table.setItem(i, 6, stop_item)

                # Column 7: $ Risk
                risk_value = p_data.get('dollar_risk', 0)
                risk_item = QTableWidgetItem(f"${risk_value:,.2f}")
                self.table.setItem(i, 7, risk_item)

                # Column 8: % Risk
                percent_risk = p_data.get('percent_risk', 0.0)
                percent_risk_item = QTableWidgetItem(f"{percent_risk:.2f}%")

                if percent_risk > 2.0:
                    percent_risk_item.setForeground(QColor('red'))
                self.table.setItem(i, 8, percent_risk_item)

                # Column 9: Status
                self.table.setItem(i, 9, QTableWidgetItem(p_data.get('status', '...')))

            except Exception as e:
                symbol = p_data.get('symbol', 'UNKNOWN')
                logging.error(f"Error populating table for symbol {symbol}: {e}")
                # Optionally, display an error in the row
                error_item = QTableWidgetItem(f"Error: {e}")
                error_item.setForeground(QtGui.QColor('red'))
                self.table.setItem(i, 1, QTableWidgetItem(symbol))
                self.table.setItem(i, 9, error_item)

    def get_atr_ratio_for_symbol(self, symbol):
        """Finds the ATR ratio for a symbol from the UI table."""
        for i in range(self.table.rowCount()):
            if self.table.item(i, 1) and self.table.item(i, 1).text() == symbol:
                spin_box = self.table.cellWidget(i, 3)
                return spin_box.value() if spin_box else 1.5
        return 1.5 # Default

    def load_user_settings(self):
        """Load user settings from user_settings.json"""
        if os.path.exists(self.user_settings_file):
            try:
                with open(self.user_settings_file, 'r') as f:
                    settings = json.load(f)
                    # The file should contain the symbol_stop_enabled dictionary
                    return settings.get('symbol_stop_enabled', {})
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading user settings: {e}")
                return {}
        return {}

    def save_user_settings(self):
        """Save user settings to user_settings.json"""
        try:
            with open(self.user_settings_file, 'w') as f:
                settings_to_save = {
                    'symbol_stop_enabled': self.symbol_stop_enabled
                }
                json.dump(settings_to_save, f, indent=2)
            logging.info("User settings saved successfully")
        except Exception as e:
            print(f"Error saving user settings: {e}")

    def on_symbol_toggle_changed(self, symbol, state):
        """Handles when a user toggles the checkbox for an individual symbol."""
        is_enabled = state == Qt.CheckState.Checked.value
        self.symbol_stop_enabled[symbol] = is_enabled
        logging.info(f"Stop loss submission for {symbol} set to: {'ENABLED' if is_enabled else 'DISABLED'}")

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
                    self.user_overrides[symbol] = {} # pyright: ignore
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
            tr, atr = PortfolioCalculator(None, None, None, None, self.market_statuses).calculate_tr_and_atr(df, prior_atr=prior_atr, symbol=symbol)

            if tr is not None and atr is not None:
                # Since this runs in a background thread, we need to update UI elements
                # safely. We can store the results and update the UI in the main thread,
                # but for this direct update, let's assume it's safe enough for now.
                # A more robust solution would use signals.
                self.tr_values[row] = tr

    async def calculate_atr_for_symbols(self, ib, symbols, market_statuses, contract_details_map):
        """Calculate ATR for each symbol in the list"""
        # This function is now async and uses the provided 'ib' instance.
        # It returns a list of dictionaries, one for each symbol.
        results = []
        calculator = PortfolioCalculator(
            self.atr_history, self.user_overrides, self.highest_stop_losses, {}, market_statuses
        )

        tasks = []
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
                        logging.info(f"User override for {symbol} exists for interval {completed_interval_key}. Using value: {current_atr:.4f}")
                    elif symbol in self.atr_history and completed_interval_key in self.atr_history[symbol] and not is_user_override:
                        # The value was calculated previously, but it wasn't a user override, so we can re-calculate it to be safe.
                        logging.info(f"Recalculating ATR for {symbol} for interval {completed_interval_key} as it was not a user override.")
                        tr, current_atr = calculator.calculate_tr_and_atr(df, prior_atr=previous_atr, symbol=symbol)
                        # Even if we skip calculation, ensure the value is saved for this interval if it's somehow missing
                        if completed_interval_key not in self.atr_history.get(symbol, {}):
                            self.atr_history[symbol][completed_interval_key] = current_atr
                            self.save_atr_history()

                    elif previous_atr is not None:
                        # Calculate new ATR for the completed interval
                        tr, current_atr = calculator.calculate_tr_and_atr(df, prior_atr=previous_atr, symbol=symbol)
                        logging.info(f"Symbol: {symbol}, Calculating ATR for completed interval {completed_interval_key}")
                        logging.info(f"  Previous ATR: {previous_atr:.4f}, TR: {tr:.4f}, New ATR: {current_atr:.4f}")

                        # Save ATR for the completed interval
                        if symbol not in self.atr_history:
                            self.atr_history[symbol] = {}
                        self.atr_history[symbol][completed_interval_key] = current_atr
                        self.save_atr_history()
                    else:
                        # If there's no previous ATR, we can't calculate a new one.
                        _, current_atr = calculator.calculate_tr_and_atr(df, prior_atr=previous_atr, symbol=symbol)
                        logging.warning(f"Symbol: {symbol}, TR: {current_tr:.4f}, ATR: N/A (No previous ATR from 15-min interval)")

                    results.append({
                        'symbol': symbol,
                        'tr': current_tr,
                        'atr': current_atr,
                        'previous_atr': previous_atr
                    })
            
            except Exception as e:
                print(f"Error calculating ATR for {symbol}: {e}")
                continue
        return results

    def start_full_refresh(self):
        """Starts the first stage of the data loading sequence."""
        self.update_status(False) # Show as disconnected/refreshing
        self.connection_status.setText("Refreshing...")
        self.connection_status.setStyleSheet("color: orange; font-weight: bold;")
        self.start_worker()

    def start_worker(self):
        """Creates and starts a single worker for the entire refresh cycle."""
        if self.worker_thread and self.worker_thread.isRunning():
            logging.warning("Refresh already in progress. Skipping new request.")
            return

        self.worker_thread = QThread()
        self.worker = DataWorker(self)
        self.worker.moveToThread(self.worker_thread)

        self.worker_thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.on_worker_finished)
        self.worker.error.connect(self.handle_data_error)
        self.worker.log_message.connect(self.log_to_ui)

        # Connect the new consolidated signal
        self.worker.data_ready.connect(self.handle_data_ready)
        self.worker.orders_submitted.connect(self.handle_orders_submitted)

        self.worker_thread.start()
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
        if self.worker_thread and self.worker_thread.isRunning():
            self.worker_thread.quit()
            self.worker_thread.wait()
        
        self.worker_thread.deleteLater()
        self.worker.deleteLater()
        self.worker_thread = None

    def handle_data_ready(self, positions_data):
        """Handles the fully processed data from the worker."""
        logging.info(f"Data ready: Received {len(positions_data)} fully processed positions.")
        if not positions_data:
            self.raw_data_view.setPlainText("No positions returned from IBKR")
            self.table.setRowCount(0)
            self.atr_table.setRowCount(0)
            return

        self.positions_data = positions_data

        # Update ATR table data from the processed positions
        self.atr_symbols = [p['symbol'] for p in self.positions_data]
        self.tr_values = [p.get('tr', 0) for p in self.positions_data] # Assuming TR is added to p_data
        self.atr_calculated = [p.get('atr_value') for p in self.positions_data]
        self.previous_atr_values = [p.get('previous_atr') for p in self.positions_data] # Assuming this is added

        # Update UI
        self.update_raw_data_view()
        self.populate_atr_table()
        self.populate_positions_table()

    def handle_orders_submitted(self, order_results):
        """Stage 4: Order submission is complete. Update statuses."""
        logging.info("Stage 4 Complete: Processed order submissions.")
        self.process_order_results(order_results)
        self.populate_positions_table() # Repopulate to show final statuses

    def update_raw_data_view(self):
        """Updates the raw data text view based on the latest positions data."""
        display_text = ""
        for p in self.positions_data:
            self.contract_details_map[p['symbol']] = p['contract_details'] # Keep this map updated
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
            
            # Find the corresponding position data and update its status
            for p_data in self.positions_data:
                if p_data['symbol'] == symbol:
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    message = result.get('message', 'Unknown')

                    if status in ['submitted', 'unchanged']:
                        p_data['status'] = f"Order Updated - {timestamp}"
                    elif status == 'held':
                        p_data['status'] = f"Held - {message}"
                    elif status == 'pending':
                        p_data['status'] = f"Order Rejected - {message}"
                    elif status in ['error', 'skipped']:
                        p_data['status'] = f"Error - {message}"
                    break

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
