# main.py
import sys
import logging
import threading
import random
import json
import os
from datetime import datetime, timedelta
from PyQt6 import QtGui, QtCore
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QTableWidget, QTableWidgetItem, QVBoxLayout, QDialog, QFormLayout, QLineEdit, QDialogButtonBox, QComboBox,
    QWidget, QDoubleSpinBox, QTabWidget, QTextEdit, QPushButton, QHeaderView, QAbstractSpinBox,
    QLabel, QHBoxLayout, QCheckBox, QStyle
)
from PyQt6.QtCore import Qt, QTimer, QSize, QObject, QThread, pyqtSignal
from PyQt6.QtGui import QMovie, QColor
from ibkr_api import get_market_statuses_for_all, fetch_basic_positions, fetch_market_data_for_positions
import pyqtgraph as pg
from ib_insync import IB, util, Future, Contract, StopOrder
import math
import asyncio
from orders import process_stop_orders, get_active_stop_symbols
from atr_processor import ATRProcessor
import struct
from decimal import Decimal, ROUND_DOWN

from calculator import PortfolioCalculator
from utils import get_point_value # Import from the new utils file
# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
ATR_STATE_FILE = 'atr_state.json'
ATR_HISTORY_FILE = 'atr_history.json' # For graphing
STOP_HISTORY_FILE = 'stop_history.json'


class DataWorker(QObject):
    """
    Worker thread for fetching and processing all IBKR data.
    This runs in the background to keep the UI responsive.
    """
    finished = pyqtSignal()
    error = pyqtSignal(str)
    # This single signal will carry all the calculated data for the UI
    data_ready = pyqtSignal(list, dict, dict) # Emits (final_positions_data, updated_atr_state, updated_atr_history)
    orders_submitted = pyqtSignal(list)
    log_message = pyqtSignal(str) # Signal to send log messages to the UI
    stops_updated = pyqtSignal(dict) # Signal to send back updated stops
    
    def __init__(self, atr_window, client_id):
        super().__init__()
        # Give worker access to the main window's methods and data
        self.atr_window = atr_window
        self.highest_stop_losses = atr_window.highest_stop_losses # Use a direct reference
        self.symbol_stop_enabled = atr_window.symbol_stop_enabled
        self.client_id = client_id # Use a consistent client ID

    async def run_async(self):
        """Main worker method, executes all data stages sequentially."""
        ib = IB()
        try:
            # Determine port based on trading mode
            port = 7496 if self.atr_window.trading_mode == 'LIVE' else 7497
            self.log_message.emit(f"Connecting in {self.atr_window.trading_mode} mode on port {port}...")
            await ib.connectAsync('127.0.0.1', port, clientId=self.client_id)
            
            # --- CRITICAL RECONCILIATION STEP ---
            # Before any calculations, get the ground truth of active stops from the brokerage.
            active_stop_symbols = await get_active_stop_symbols(ib)
            
            # Reconcile the local stop history. Remove any symbol from our ratcheting
            # history if it does NOT have an active stop order in the brokerage.
            symbols_to_clear = set(self.highest_stop_losses.keys()) - active_stop_symbols
            if symbols_to_clear:
                self.log_message.emit(f"Reconciliation: Clearing stale ratchet history for: {', '.join(symbols_to_clear)}")
                for symbol in symbols_to_clear:
                    del self.highest_stop_losses[symbol]
            # --- END RECONCILIATION ---

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
            # --- New ATR Calculation using ATRProcessor ---
            atr_processor = ATRProcessor(
                self.atr_window.atr_state_file, 
                self.atr_window.atr_history_file, 
                self.atr_window.atr_state_file_lock, 
                self.atr_window.atr_history_file_lock)
            atr_results, updated_atr_state, updated_atr_history = await atr_processor.run(ib, enriched_positions)
            # --- Instantiate and use the calculator ---
            # Pass copies of state to ensure thread safety
            atr_ratios_map = {p['symbol']: self.atr_window.get_atr_ratio_for_symbol(p['symbol']) for p in enriched_positions}
            
            calculator = PortfolioCalculator(
                self.atr_window.atr_state, # Use direct reference, not a copy
                {}, # user_overrides is no longer used
                self.highest_stop_losses, # Pass the direct reference, not a copy
                atr_ratios_map,
                market_statuses
            )
            final_positions_data = calculator.process_positions(enriched_positions, atr_results)            
            self.data_ready.emit(final_positions_data, updated_atr_state, updated_atr_history)
            
            # Emit the updated stops dictionary back to the main thread
            self.stops_updated.emit(self.highest_stop_losses)

            # --- Stage 4: Submit Orders (if enabled) ---
            if self.atr_window.send_adaptive_stops:
                logging.info("Adaptive stops are ENABLED. Proceeding with order submission logic.")
                stop_loss_data = self.build_stop_loss_data(final_positions_data)
                final_results = stop_loss_data.get('statuses_only', []) # Skipped/error statuses

                # CRITICAL SAFETY CHECK: Only submit orders for symbols in RTH.
                orders_to_submit = {
                    symbol: data for symbol, data in stop_loss_data.get('orders_to_submit', {}).items()
                    if market_statuses.get(symbol, '').startswith('ACTIVE')
                }
                
                if orders_to_submit:
                    logging.info(f"Submitting orders for {len(orders_to_submit)} symbols in active sessions.")
                    submission_results = await process_stop_orders(ib, orders_to_submit, self.log_message)
                    final_results.extend(submission_results)
                
                self.orders_submitted.emit(final_results)
            else:
                logging.info("Adaptive stops are DISABLED. Skipping order submission.")
                self.orders_submitted.emit([]) # Emit empty list to clear old statuses

        except Exception as e:
            self.error.emit(str(e))
        finally:
            if ib.isConnected():
                ib.disconnect()
            self.finished.emit()

    def run(self):
        """Synchronous entry point that runs the async run_async method."""
        asyncio.run(self.run_async())

    def build_stop_loss_data(self, processed_positions):
        """Prepares the data structure for submitting stop loss orders."""
        orders_to_submit = {}
        statuses_only = []

        for p_data in processed_positions:
            symbol = p_data['symbol']
            final_stop_price = p_data.get('computed_stop_loss', 0)

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

            # The calculator now determines the status. If it's 'held', we don't submit a new order.
            # The 'computed_stop_loss' will be the held value, so the broker check will see no change.
            if p_data.get('stop_status') == 'held':
                logging.info(f"Worker: Stop for {symbol} is held. No new order will be submitted.")
                continue

            orders_to_submit[symbol] = {
                'stop_price': final_stop_price, # Use the final, rounded, ratcheted stop price
                'quantity': p_data['positions_held'],
                'contract_details': p_data['contract_details']
            }
        return {'orders_to_submit': orders_to_submit, 'statuses_only': statuses_only}

class SettingsWindow(QDialog):
    """A dialog window for application settings."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.setMinimumWidth(300)

        # Main layout
        layout = QVBoxLayout(self)
        form_layout = QFormLayout()

        # Client ID setting
        self.client_id_edit = QLineEdit(str(parent.client_id))
        form_layout.addRow("Client ID:", self.client_id_edit)

        # Trading Mode setting
        self.trading_mode_combo = QComboBox()
        self.trading_mode_combo.addItems(["PAPER", "LIVE"])
        self.trading_mode_combo.setCurrentText(parent.trading_mode)
        form_layout.addRow("Trading Mode:", self.trading_mode_combo)

        layout.addLayout(form_layout)

        # OK and Cancel buttons
        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

class ATRWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("ATR Adaptive Stop Bot")
        self.setGeometry(100, 100, 1400, 800)

        # Data stores
        self.positions_data = [] # This will hold the fully processed data from the worker
        self.contract_details_map = {}  # Store contract details by symbol
        self.symbol_stop_enabled = {}  # {symbol: bool} to track individual stop toggles
        self.atr_ratios = {} # {symbol: float} to store user-set ATR ratios from the UI

        # ATR calculation data
        self.atr_symbols = []
        self.tr_values = []
        self.atr_calculated = []
        self.previous_atr_values = []
        # State and History file paths and locks
        self.atr_state_file_lock = threading.Lock()
        self.atr_history_file_lock = threading.Lock()
        self.atr_state_file = os.path.join(os.path.dirname(__file__), ATR_STATE_FILE)
        self.atr_history_file = os.path.join(os.path.dirname(__file__), ATR_HISTORY_FILE)
        
        self.atr_state = self.load_atr_state()
        self.atr_history = self.load_atr_history()

        # Load persistent stop loss history
        self.stop_history_file = os.path.join(os.path.dirname(__file__), STOP_HISTORY_FILE)
        self.highest_stop_losses = self.load_stop_history() # This is now loaded from a file

        # --- New: User Settings File and Loading ---
        # Set a default client_id before loading settings
        self.client_id = 1000 
        self.trading_mode = "PAPER" # Default trading mode
        self.user_settings_file = os.path.join(os.path.dirname(__file__), 'user_settings.json')
        # Load settings, which will update client_id if it exists in the file
        self.load_user_settings()

        # Adaptive Stop Loss toggle state
        self.send_adaptive_stops = False
        self.market_statuses = {}  # {symbol: 'ACTIVE (RTH)' | 'ACTIVE (NT)' | 'CLOSED'}

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
        left_status_container = QWidget()
        left_status_layout = QVBoxLayout()
        left_status_container.setLayout(left_status_layout)
        left_status_layout.setContentsMargins(0,0,0,0)

        # -- Toggle Switch --
        toggle_widget = QWidget()
        toggle_hbox = QHBoxLayout(toggle_widget)
        toggle_hbox.setContentsMargins(0,0,0,0)
        toggle_label = QLabel("Send Adaptive Stop Losses:")
        toggle_label.setStyleSheet("font-weight: bold;")
        toggle_hbox.addWidget(toggle_label)
        self.adaptive_stop_toggle = QCheckBox()
        self.adaptive_stop_toggle.setChecked(False)
        self.adaptive_stop_toggle.stateChanged.connect(self.on_adaptive_stop_toggled)
        self.adaptive_stop_toggle.setText("OFF")
        toggle_hbox.addWidget(self.adaptive_stop_toggle)
        left_status_layout.addWidget(toggle_widget)

        # -- Client ID Display --
        self.client_id_label = QLabel(f"Client ID: {self.client_id}") # This will now show the loaded/default ID
        self.client_id_label.setObjectName("client_id_label") # Set object name for robustness
        self.client_id_label.setStyleSheet("font-size: 10pt; color: grey;")
        left_status_layout.addWidget(self.client_id_label)

        # -- Trading Mode Display --
        self.trading_mode_label = QLabel(f"Mode: {self.trading_mode}")
        self.trading_mode_label.setObjectName("trading_mode_label")
        self.trading_mode_label.setStyleSheet("font-size: 10pt; color: grey;")
        left_status_layout.addWidget(self.trading_mode_label)

        status_layout.addWidget(left_status_container)
        
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
        
        # Add spacing before settings button
        status_layout.addSpacing(20)

        # --- Settings Button ---
        self.settings_button = QPushButton()
        # Use a standard icon that looks like a gear/settings
        settings_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_FileDialogDetailedView)
        self.settings_button.setIcon(settings_icon)
        self.settings_button.clicked.connect(self.open_settings_window)
        status_layout.addWidget(self.settings_button)

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
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet("QTableWidget { background-color: black; alternate-background-color: #111111; }")
        self.table.setColumnCount(12)
        self.table.setHorizontalHeaderLabels([
            "Send", "Position", "ATR", "ATR Ratio", "Positions Held", "Margin",
            "Current Price", "Computed Stop Loss", "", "$ Risk", "% Risk", "Status"
        ])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setSectionResizeMode(11, QHeaderView.ResizeMode.Stretch)
        self.table.horizontalHeader().setSectionsMovable(True)
        self.table.setColumnWidth(0, 50) # "Send" column
        self.table.setColumnWidth(8, 50) # Ratchet status column
        self.positions_layout.addWidget(self.table)

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

        # --- Graphing Tab ---
        self.graphing_tab = QWidget()
        self.graphing_layout = QVBoxLayout()
        self.graphing_tab.setLayout(self.graphing_layout)
        self.tabs.addTab(self.graphing_tab, "Graphing")

        # Controls for the graphing tab
        graph_controls_widget = QWidget()
        graph_controls_layout = QHBoxLayout(graph_controls_widget)
        graph_controls_layout.setContentsMargins(0, 0, 0, 0)
        
        self.symbol_selector = QComboBox()
        self.symbol_selector.setMinimumWidth(150)
        self.symbol_selector.currentIndexChanged.connect(self.update_atr_graph)
        graph_controls_layout.addWidget(QLabel("Symbol:"))
        graph_controls_layout.addWidget(self.symbol_selector)
        graph_controls_layout.addStretch() # Push controls to the left

        self.graphing_layout.addWidget(graph_controls_widget)

        # Plot widget
        self.atr_plot = pg.PlotWidget()
        self.graphing_layout.addWidget(self.atr_plot)

        # Setup auto-refresh timer (60 seconds = 60000 ms)
        self.refresh_timer = QTimer(self)
        self.refresh_timer.timeout.connect(self.start_full_refresh)
        self.refresh_timer.start(60000)  # Refresh every 60 seconds
        
        # Fetch data immediately on startup
        self.start_full_refresh()

    def populate_symbol_selector(self):
        """Populates the symbol selector dropdown in the graphing tab."""
        current_selection = self.symbol_selector.currentText()
        self.symbol_selector.blockSignals(True)
        self.symbol_selector.clear()
        
        symbols = sorted(self.atr_history.keys())
        if not symbols:
            self.symbol_selector.blockSignals(False)
            return

        self.symbol_selector.addItems(symbols)

        # Restore previous selection if it still exists
        if current_selection in symbols:
            self.symbol_selector.setCurrentText(current_selection)
        
        self.symbol_selector.blockSignals(False)
        # Manually trigger an update if the selection is valid
        if self.symbol_selector.currentText():
            self.update_atr_graph()

    def update_atr_graph(self):
        """Updates the ATR graph based on the selected symbol."""
        symbol = self.symbol_selector.currentText()
        self.atr_plot.clear()
        self.atr_plot.setTitle(f"ATR History for {symbol}")
        self.atr_plot.setLabel('left', 'ATR Value')
        self.atr_plot.setLabel('bottom', 'Time')
        self.atr_plot.showGrid(x=True, y=True)

        if not symbol or symbol not in self.atr_history:
            return

        # Plot the ATR history, not the TR history from the state file.
        symbol_data = self.atr_history.get(symbol, {})
        if not symbol_data:
            return

        # Sort data by timestamp and prepare for plotting
        sorted_timestamps = sorted(symbol_data.keys()) # Timestamps are ISO strings
        x_data = [datetime.fromisoformat(ts).timestamp() for ts in sorted_timestamps]
        y_data = [symbol_data[ts] for ts in sorted_timestamps]

        self.atr_plot.getAxis('bottom').setTickSpacing(3600, 1800) # Major tick every hour, minor every 30 mins
        self.atr_plot.getAxis('bottom').setStyle(tickTextOffset = 10, autoExpandTextSpace=True)
        self.atr_plot.setAxisItems({'bottom': pg.DateAxisItem()})
        self.atr_plot.plot(x_data, y_data, pen=pg.mkPen('y', width=2), symbol='o', symbolBrush='y', symbolSize=5)

    def open_settings_window(self):
        """Opens the settings dialog window."""
        dialog = SettingsWindow(self)
        if dialog.exec():  # This is a blocking call
            try:
                # Update Client ID
                new_client_id = int(dialog.client_id_edit.text())
                if self.client_id != new_client_id:
                    self.client_id = new_client_id
                    self.log_to_ui(f"Client ID updated to {self.client_id}. Changes will apply on next refresh.")
                    self.client_id_label.setText(f"Client ID: {self.client_id}")

                # Update Trading Mode
                new_trading_mode = dialog.trading_mode_combo.currentText()
                if self.trading_mode != new_trading_mode:
                    self.trading_mode = new_trading_mode
                    self.log_to_ui(f"Trading Mode set to {self.trading_mode}. Changes will apply on next refresh.")
                    self.trading_mode_label.setText(f"Mode: {self.trading_mode}")

                self.save_user_settings() # Save all settings at once
            except ValueError:
                self.log_to_ui("Invalid Client ID entered. It must be an integer.")
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
        self.save_stop_history() # Save stop history on exit
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

                # Column 1: Position with new status indicator
                market_status = self.market_statuses.get(symbol, 'CLOSED')
                position_item = QTableWidgetItem(p_data['position'])

                # Create a colored circle icon
                pixmap = QtGui.QPixmap(16, 16)
                if market_status == 'ACTIVE (RTH)':
                    pixmap.fill(Qt.GlobalColor.green)
                elif market_status == 'ACTIVE (NT)':
                    pixmap.fill(QColor('orange')) # Orange for overnight/non-RTH
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
                spin.valueChanged.connect(lambda val, row=i, symbol=symbol: self.on_atr_ratio_changed(row, symbol, val))
                self.table.setCellWidget(i, 3, spin)

                # Column 4: Positions Held
                self.table.setItem(i, 4, QTableWidgetItem(str(p_data['positions_held'])))
                
                # Column 5: Margin
                self.table.setItem(i, 5, QTableWidgetItem(f"${p_data.get('margin', 0):,.2f}"))

                # Column 6: Current Price
                self.table.setItem(i, 6, QTableWidgetItem(f"{p_data.get('current_price', 0):.2f}"))

                # Column 7: Computed Stop Loss
                computed_stop = p_data.get('computed_stop_loss')
                stop_display = f"{computed_stop:.4f}" if computed_stop is not None else "N/A"
                self.table.setItem(i, 7, QTableWidgetItem(stop_display))

                # Column 8: Stop Status Icon (New)
                stop_status = p_data.get('stop_status', 'new') # Default to 'new'
                status_item = QTableWidgetItem()
                status_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

                if stop_status == 'new':
                    status_item.setText("New")
                    status_item.setForeground(QColor('green'))
                    status_item.setToolTip("New, higher stop loss calculated.")
                elif stop_status == 'held':
                    status_item.setText("Held")
                    status_item.setForeground(QColor('orange'))
                    status_item.setToolTip("Stop loss held by ratchet (previous stop was higher).")

                self.table.setItem(i, 8, status_item)

                # Column 9: $ Risk
                risk_value = p_data.get('dollar_risk', 0)
                risk_item = QTableWidgetItem(f"${risk_value:,.2f}")
                self.table.setItem(i, 9, risk_item)

                # Column 10: % Risk
                percent_risk = p_data.get('percent_risk', 0.0)
                percent_risk_item = QTableWidgetItem(f"{percent_risk:.2f}%")

                if percent_risk > 2.0:
                    percent_risk_item.setForeground(QColor('red'))
                self.table.setItem(i, 10, percent_risk_item)

                # Column 11: Status
                self.table.setItem(i, 11, QTableWidgetItem(p_data.get('status', '...')))

            except Exception as e:
                symbol = p_data.get('symbol', 'UNKNOWN')
                logging.error(f"Error populating table for symbol {symbol}: {e}")

    def on_atr_ratio_changed(self, row, symbol, value):
        """
        Slot for when a user changes the ATR ratio spinbox.
        This method updates the internal state and triggers a recalculation.
        It does NOT perform calculations itself.
        """
        # 1. Update the internal state for ATR ratios
        self.atr_ratios[symbol] = value
        logging.info(f"User set ATR Ratio for {symbol} to {value:.1f}. Triggering recalculation.")

        # 2. Trigger the recalculation for the specific row
        self.recalculate_row(row)

    def recalculate_row(self, row):
        """
        Recalculates stop loss and risk for a single row using the PortfolioCalculator.
        This is called after a user input (like ATR Ratio) changes.
        """
        if row >= len(self.positions_data):
            return

        p_data = self.positions_data[row]
        symbol = p_data['symbol']

        # Use a temporary calculator instance for this single operation
        # It uses the application's current state
        calculator = PortfolioCalculator(
            self.atr_state, {}, self.highest_stop_losses, self.atr_ratios, self.market_statuses
        )

        # Recalculate stop loss for this position, but WITHOUT applying the ratchet.
        # This gives the user immediate feedback on the stop level for that ratio.
        # The ratchet will apply on the next full refresh cycle.
        # The function returns a tuple (stop_price, status), so we unpack it.
        new_stop, _ = calculator.compute_stop_loss(p_data, p_data['current_price'], p_data.get('atr_value'), self.atr_ratios.get(symbol, 1.5), apply_ratchet=False)

        # Recalculate risk based on the new un-ratcheted stop
        new_risk_dollar, new_risk_percent = calculator.calculate_risk(p_data, new_stop)

        # Update the UI with the new values
        self.table.item(row, 7).setText(f"{new_stop:.4f}" if new_stop is not None and isinstance(new_stop, (int, float)) else "N/A")
        self.table.item(row, 9).setText(f"${new_risk_dollar:,.2f}")
        self.table.item(row, 10).setText(f"{new_risk_percent:.2f}%")

    def get_atr_ratio_for_symbol(self, symbol):
        """Finds the ATR ratio for a symbol from the UI table."""
        # Read from our internal state dictionary first, then fall back to the widget
        return self.atr_ratios.get(symbol, 1.5)

    def load_user_settings(self):
        """Load user settings from user_settings.json"""
        if os.path.exists(self.user_settings_file):
            try:
                with open(self.user_settings_file, 'r') as f:
                    settings = json.load(f)
                    # Load client_id, defaulting to the pre-set value if not in file
                    self.client_id = settings.get('client_id', self.client_id)
                    self.trading_mode = settings.get('trading_mode', self.trading_mode)
                    # Load symbol toggles
                    self.symbol_stop_enabled = settings.get('symbol_stop_enabled', {})
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading user settings: {e}")
                self.symbol_stop_enabled = {}
        else:
            self.symbol_stop_enabled = {}

    def save_user_settings(self):
        """Save all user settings to user_settings.json"""
        try:
            with open(self.user_settings_file, 'w') as f:
                settings_to_save = {
                    'client_id': self.client_id,
                    'trading_mode': self.trading_mode,
                    'symbol_stop_enabled': self.symbol_stop_enabled,
                    # Add any other settings here in the future
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

        # If the user disables the symbol, reset its stop loss ratchet.
        if not is_enabled and symbol in self.highest_stop_losses:
            del self.highest_stop_losses[symbol]
            self.save_stop_history() # Persist the change immediately
            self.log_to_ui(f"Ratchet for {symbol} has been reset. Its stop loss history is cleared.")
            logging.info(f"Removed {symbol} from highest_stop_losses to reset ratchet.")

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
            tr_value = self.tr_values[i]
            tr_display = f"{tr_value:.2f}" if tr_value is not None else "N/A"
            tr_item = QTableWidgetItem(tr_display)
            tr_item.setFlags(tr_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.atr_table.setItem(i, 2, tr_item)

            # ATR (editable)
            atr_item = QTableWidgetItem(f"{self.atr_calculated[i]:.2f}" if self.atr_calculated[i] is not None else "N/A")
            self.atr_table.setItem(i, 3, atr_item)

        # Reconnect the signal
        self.atr_table.cellChanged.connect(self.on_atr_changed)

    def load_stop_history(self):
        """Load the persistent stop loss history from stop_history.json."""
        if os.path.exists(self.stop_history_file):
            try:
                with open(self.stop_history_file, 'r') as f:
                    history = json.load(f)
                    logging.info(f"Loaded {len(history)} symbols from stop history.")
                    return history
            except (json.JSONDecodeError, IOError) as e:
                logging.error(f"Error loading stop history: {e}")
                return {}
        return {}

    def save_stop_history(self):
        """Save the current highest stop losses to stop_history.json."""
        try:
            with open(self.stop_history_file, 'w') as f:
                json.dump(self.highest_stop_losses, f, indent=2)
            logging.info("Stop history saved successfully.")
        except Exception as e:
            logging.error(f"Error saving stop history: {e}")

    def load_atr_history(self):
        """Load ATR history for graphing from JSON file"""
        if os.path.exists(self.atr_history_file):
            try:
                with open(self.atr_history_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logging.error(f"Error loading ATR history: {e}")
                return {}
        return {}

    def save_atr_history(self):
        """Save ATR history for graphing to JSON file"""
        with self.atr_history_file_lock:
            try:
                with open(self.atr_history_file, 'w') as f:
                    json.dump(self.atr_history, f, indent=2)
                logging.info("ATR history saved successfully.")
            except Exception as e:
                logging.error(f"Error saving ATR history: {e}")

    def load_atr_state(self):
        """Load ATR state (TR history and last ATR) from JSON file"""
        if os.path.exists(self.atr_state_file):
            try:
                with open(self.atr_state_file, 'r') as f:
                    # The new format is just the history dictionary
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading ATR state: {e}")
                return {}
        return {}
    
    def save_atr_state(self):
        """Save ATR state to JSON file"""
        try:
            with open(self.atr_state_file, 'w') as f:
                json.dump(self.atr_state, f, indent=2)
            logging.info("ATR state saved successfully")
        except Exception as e:
            print(f"Error saving ATR state: {e}")

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

                # This functionality is now deprecated as ATR is derived from TR history.
                # A user would need to edit the TR history to change the ATR.
                # For now, we just log this action.
                logging.warning(f"User manually edited ATR for {symbol} to {new_atr_value:.2f}. This is a display-only change and will be overwritten on the next refresh.")
            except ValueError:
                logging.warning(f"Invalid input for ATR: '{item.text()}'. Please enter a number.")
                # Optionally, revert to the old value or show an error
                if self.atr_calculated[row] is not None:
                    item.setText(f"{self.atr_calculated[row]:.2f}")

    def start_single_atr_recalc_worker(self, row, symbol, prior_atr):
        """This function is deprecated and no longer used."""
        logging.warning("start_single_atr_recalc_worker is deprecated and has been called. No action taken.")
        pass

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
        self.worker = DataWorker(self, self.client_id) # Pass the stable client ID
        self.worker.moveToThread(self.worker_thread)

        self.worker_thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.on_worker_finished)
        self.worker.error.connect(self.handle_data_error)
        self.worker.log_message.connect(self.log_to_ui)

        # Connect the new consolidated signal
        self.worker.data_ready.connect(self.handle_data_ready)
        self.worker.orders_submitted.connect(self.handle_orders_submitted)
        self.worker.stops_updated.connect(self.handle_stops_updated)

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
    
    def handle_data_ready(self, positions_data, updated_atr_state, updated_atr_history):
        """Handles the fully processed data from the worker. The 'atr_history' is now TR history."""
        logging.info(f"Data ready: Received {len(positions_data)} fully processed positions.")
        if not positions_data:
            self.table.setRowCount(0)
            self.atr_table.setRowCount(0)
            return

        # --- CRITICAL: Update the main window's history state from the worker ---
        self.atr_state = updated_atr_state
        self.atr_history = updated_atr_history
        logging.info(f"Main window ATR state updated with {len(self.atr_state)} symbols.")
        logging.info(f"Main window ATR history updated with {len(self.atr_history)} symbols.")

        self.positions_data = positions_data

        # Update ATR table data from the processed positions
        self.atr_symbols = [p['symbol'] for p in self.positions_data]
        self.tr_values = [p.get('tr') for p in self.positions_data] # Can be None, handled in populate_atr_table
        self.atr_calculated = [p.get('atr_value') for p in self.positions_data]
        self.previous_atr_values = [p.get('previous_atr') for p in self.positions_data] # Assuming this is added

        # Update the contract details map, which was previously in update_raw_data_view
        for p in self.positions_data:
            self.contract_details_map[p['symbol']] = p['contract_details']

        # Update UI
        self.populate_atr_table()
        self.populate_positions_table()
        self.populate_symbol_selector() # Populate the new dropdown

    def handle_stops_updated(self, updated_stops):
        """Receives the updated stop dictionary from the worker and saves it."""
        logging.info("Main thread received updated stop-loss dictionary from worker.")
        self.highest_stop_losses = updated_stops
        self.save_stop_history() # Persist the changes


    def handle_orders_submitted(self, order_results):
        """Stage 4: Order submission is complete. Update statuses."""
        logging.info("Stage 4 Complete: Processed order submissions.")
        self.process_order_results(order_results)
        self.populate_positions_table() # Repopulate to show final statuses

    def handle_data_error(self, error_message):
        """Slot to handle errors from the worker thread."""
        logging.error(f"Error in worker thread: {error_message}")
        self.log_to_ui(f"Error fetching data: {error_message}")
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
