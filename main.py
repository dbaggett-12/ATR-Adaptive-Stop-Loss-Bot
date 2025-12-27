# main.py
import sys
import logging
import threading
import random
import json
import os
import shutil
from datetime import datetime, timedelta
from PyQt6 import QtGui, QtCore
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QTableWidget, QTableWidgetItem, QVBoxLayout, QDialog, QFormLayout, QLineEdit, QDialogButtonBox, QComboBox,
    QWidget, QDoubleSpinBox, QTabWidget, QTextEdit, QPushButton, QHeaderView, QAbstractSpinBox,
    QLabel, QHBoxLayout, QCheckBox, QStyle
)
from PyQt6.QtCore import Qt, QTimer, QSize, QObject, QThread, pyqtSignal
from PyQt6.QtGui import QMovie, QColor, QIcon
from ibkr_api import get_market_statuses_for_all, fetch_basic_positions, fetch_market_data_for_positions
import pyqtgraph as pg
from ib_insync import IB
import asyncio
from orders import process_stop_orders, get_active_stop_symbols
from atr_processor import ATRProcessor

from calculator import PortfolioCalculator
# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- User Data Directory ---
# This is the correct place for user-writable files like settings and state.
USER_DATA_DIR = os.path.join(os.path.expanduser("~"), ".atrTrailingStop")
os.makedirs(USER_DATA_DIR, exist_ok=True)
logging.info(f"Using user data directory: {USER_DATA_DIR}")

def resource_path(relative_path):
    """Get absolute path to resource, works for PyInstaller bundle and dev"""
    try:
        base_path = sys._MEIPASS  # PyInstaller temporary folder
    except AttributeError:
        base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, relative_path)

USER_SETTINGS_FILE = "user_settings.json"

# --- Default Settings File Handling ---
# This ensures a default user_settings.json is available in the user's data directory.
default_settings_src = resource_path(USER_SETTINGS_FILE)
user_settings_dest = os.path.join(USER_DATA_DIR, USER_SETTINGS_FILE)

# Only copy if the destination doesn't exist and the source (in bundle) does.
if not os.path.exists(user_settings_dest) and os.path.exists(default_settings_src):
    shutil.copy(default_settings_src, user_settings_dest)
    logging.info(f"Created default settings file at: {user_settings_dest}")

# --- Constants ---
ATR_STATE_FILE = 'atr_state.json'
ATR_HISTORY_FILE = 'atr_history.json' # For graphing
STOP_HISTORY_FILE = 'stop_history.json'

class NumericTableWidgetItem(QTableWidgetItem):
    """
    Custom TableWidgetItem to enable proper numerical sorting.
    Stores the raw numerical value in UserRole.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __lt__(self, other):
        try:
            # Retrieve data from UserRole
            val1 = self.data(Qt.ItemDataRole.UserRole)
            val2 = other.data(Qt.ItemDataRole.UserRole)
            
            v1 = float(val1) if val1 is not None else -float('inf')
            v2 = float(val2) if val2 is not None else -float('inf')
            
            return v1 < v2
        except (ValueError, TypeError):
            # Fallback to string comparison if conversion fails
            return super().__lt__(other)

class DataWorker(QObject):
    """
    Worker thread for fetching and processing all IBKR data.
    This runs in the background to keep the UI responsive.
    """
    finished = pyqtSignal(bool)
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
        success = False
        try:
            # Determine port based on trading mode
            port = 7496 if self.atr_window.trading_mode == 'LIVE' else 7497
            # self.log_message.emit(f"Connecting in {self.atr_window.trading_mode} mode on port {port}...")
            await ib.connectAsync('127.0.0.1', port, clientId=self.client_id)
            
            # --- CRITICAL RECONCILIATION STEP ---
            # Before any calculations, get the ground truth of active stops from the brokerage.
            active_stop_symbols = await get_active_stop_symbols(ib)
            
            # Reconcile the local stop history. Remove any symbol from our ratcheting
            # history if it does NOT have an active stop order in the brokerage.
            symbols_to_clear = set(self.highest_stop_losses.keys()) - active_stop_symbols
            if symbols_to_clear:
                # self.log_message.emit(f"Reconciliation: Clearing stale ratchet history for: {', '.join(symbols_to_clear)}")
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
            
            candle_settings = self.atr_window.get_all_candle_sizes()
            atr_results, updated_atr_state, updated_atr_history = await atr_processor.run(ib, enriched_positions, candle_settings)
            # --- Instantiate and use the calculator ---
            # Pass copies of state to ensure thread safety
            atr_ratios_map = {p['symbol']: self.atr_window.get_atr_ratio_for_symbol(p['symbol']) for p in enriched_positions}
            
            calculator = PortfolioCalculator(
                updated_atr_state, # Use the state just calculated by the processor
                {}, # user_overrides is no longer used
                self.highest_stop_losses, # Pass the direct reference, not a copy
                atr_ratios_map,
                market_statuses,
                log_callback=self.log_message.emit
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
            
            success = True

        except Exception as e:
            self.error.emit(str(e))
        finally:
            if ib.isConnected():
                ib.disconnect()
            self.finished.emit(success)

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

class LogBridge(QObject):
    log_signal = pyqtSignal(str)

class QtLogHandler(logging.Handler):
    def __init__(self, bridge):
        super().__init__()
        self.bridge = bridge

    def emit(self, record):
        try:
            msg = self.format(record)
            self.bridge.log_signal.emit(msg)
        except Exception:
            self.handleError(record)

class SettingsWindow(QDialog):
    """A dialog window for application settings."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.setMinimumWidth(400)

        # Main layout
        layout = QVBoxLayout(self)
        form_layout = QFormLayout()

        # Client ID setting
        self.client_id_edit = QLineEdit(str(parent.client_id))
        form_layout.addRow("Client ID:", self.client_id_edit)

        # Trading Mode setting
        self.trading_mode_combo = QComboBox()
        self.trading_mode_combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
        self.trading_mode_combo.setMinimumWidth(150)
        self.trading_mode_combo.addItems(["PAPER", "LIVE"])
        self.trading_mode_combo.setCurrentText(parent.trading_mode)
        form_layout.addRow("Trading Mode:", self.trading_mode_combo)

        # Debug Log setting
        self.debug_log_check = QCheckBox()
        self.debug_log_check.setChecked(parent.debug_log_enabled)
        form_layout.addRow("Debug Log:", self.debug_log_check)

        # Debug Full Log setting
        self.debug_full_log_check = QCheckBox()
        self.debug_full_log_check.setChecked(parent.debug_full_log_enabled)
        form_layout.addRow("Debug Full Log:", self.debug_full_log_check)

        # Theme setting
        self.theme_combo = QComboBox()
        self.theme_combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
        self.theme_combo.setMinimumWidth(150)
        self.theme_combo.addItems(["Dark", "Light"])
        self.theme_combo.setCurrentText(parent.theme)
        form_layout.addRow("Theme:", self.theme_combo)

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
        self.setWindowIcon(QIcon(resource_path(os.path.join("windows assets", "PaceChaser.ico"))))
        self.setGeometry(100, 100, 1400, 800)

        # Data stores
        self.positions_data = [] # This will hold the fully processed data from the worker
        self.contract_details_map = {}  # Store contract details by symbol
        self.symbol_stop_enabled = {}  # {symbol: bool} to track individual stop toggles
        self.symbol_candle_size = {} # {symbol: "1 day"|"1 hour"|"15 mins"}
        self.atr_ratios = {} # {symbol: float} to store user-set ATR ratios from the UI

        # ATR calculation data
        self.atr_symbols = []
        self.tr_values = []
        self.atr_calculated = []
        self.previous_atr_values = []
        # State and History file paths and locks
        self.atr_state_file_lock = threading.Lock()
        self.atr_history_file_lock = threading.Lock()
        self.atr_state_file = os.path.join(USER_DATA_DIR, ATR_STATE_FILE)
        self.atr_history_file = os.path.join(USER_DATA_DIR, ATR_HISTORY_FILE)
        
        self.atr_state = self.load_atr_state()
        self.atr_history = self.load_atr_history()

        # Load persistent stop loss history
        self.stop_history_file = os.path.join(USER_DATA_DIR, STOP_HISTORY_FILE)
        self.highest_stop_losses = self.load_stop_history() # This is now loaded from a file

        # --- New: User Settings File and Loading ---
        # Set a default client_id before loading settings
        self.client_id = 1000 
        self.trading_mode = "PAPER" # Default trading mode
        self.debug_log_enabled = True # Default debug log
        self.debug_full_log_enabled = False # Default full log
        self.theme = "Dark" # Default theme

        # Setup Log Bridge for Full Log
        self.log_bridge = LogBridge()
        self.log_bridge.log_signal.connect(self.log_to_ui)
        self.qt_log_handler = QtLogHandler(self.log_bridge)
        self.qt_log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        self.user_settings_file = os.path.join(USER_DATA_DIR, USER_SETTINGS_FILE)
        # Load settings, which will update client_id if it exists in the file
        self.load_user_settings()
        self.update_full_log_state()

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
        gif_path = resource_path(os.path.join('assets', 'mambo-ume-usume.gif'))
        self.movie = QMovie(gif_path)
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
        self.log_label = QLabel("Log Output:")
        self.log_label.setStyleSheet("font-weight: bold; margin-top: 10px;")
        layout.addWidget(self.log_label)
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        # Stylesheet is now handled by apply_theme()
        self.log_view.setMaximumHeight(200) # Give it a fixed max height
        layout.addWidget(self.log_view)

        # Set initial visibility based on settings
        self.update_log_visibility()

        # --- Positions Table Tab ---
        self.positions_tab = QWidget()
        self.positions_layout = QVBoxLayout()
        self.positions_tab.setLayout(self.positions_layout)
        self.tabs.addTab(self.positions_tab, "Positions")

        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        # Stylesheet is now handled by apply_theme()
        self.table.setColumnCount(14)
        self.table.setHorizontalHeaderLabels([
            "Send", "Position", "Candle", "ATR", "ATR Ratio", "Positions Held", "Margin", "Avg Cost",
            "Current Price", "Computed Stop Loss", "", "$ Risk", "% Risk", "Status"
        ])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setSectionResizeMode(13, QHeaderView.ResizeMode.Stretch)
        self.table.horizontalHeader().setSectionsMovable(True)
        
        # Apply saved column widths if available, otherwise use defaults
        if self.column_widths:
            for col_idx, width in self.column_widths.items():
                try:
                    self.table.setColumnWidth(int(col_idx), int(width))
                except (ValueError, TypeError):
                    pass
        else:
            self.table.setColumnWidth(0, 50) # "Send" column
            self.table.setColumnWidth(2, 80) # Candle column
            self.table.setColumnWidth(10, 50) # Ratchet status column
            
        self.table.setSortingEnabled(True)
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

        # --- Graphing Tab ---
        self.graphing_tab = QWidget()
        self.graphing_layout = QVBoxLayout()
        self.graphing_tab.setLayout(self.graphing_layout)
        self.tabs.addTab(self.graphing_tab, "Graphing")

        # Controls for the graphing tab
        graph_controls_widget = QWidget()
        graph_controls_layout = QHBoxLayout(graph_controls_widget)
        graph_controls_layout.setContentsMargins(0, 0, 0, 0)
        graph_controls_layout.setSpacing(5)
        
        self.symbol_selector = QComboBox()
        self.symbol_selector.setMinimumWidth(150)
        self.symbol_selector.currentIndexChanged.connect(self.update_atr_graph)
        graph_controls_layout.addWidget(QLabel("Symbol:"))
        graph_controls_layout.addWidget(self.symbol_selector)

        # Add a button to reset the graph view to the most recent data
        self.reset_view_button = QPushButton()
        # Use a "reset" icon, as a standard crosshair is not available.
        reset_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_DialogResetButton)
        self.reset_view_button.setIcon(reset_icon)
        self.reset_view_button.setToolTip("Reset view to show the most recent data")
        self.reset_view_button.setFixedSize(28, 28)
        self.reset_view_button.clicked.connect(self.update_atr_graph) # Re-running update_atr_graph resets the view
        graph_controls_layout.addWidget(self.reset_view_button)
        graph_controls_layout.addStretch() # Push remaining controls to the left

        self.graphing_layout.addWidget(graph_controls_widget)

        # Plot widget
        self.atr_plot = pg.PlotWidget()
        self.graphing_layout.addWidget(self.atr_plot)

        # Setup auto-refresh timer (60 seconds = 60000 ms)
        self.refresh_timer = QTimer(self)
        self.refresh_timer.timeout.connect(self.start_full_refresh)
        self.refresh_timer.start(60000)  # Refresh every 60 seconds
        
        self.apply_theme()
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
        
        # Reset ViewBox limits to defaults to prevent carry-over between symbols
        view_box = self.atr_plot.plotItem.getViewBox()
        view_box.setLimits(xMin=None, xMax=None, yMin=None, yMax=None,
                           minXRange=None, maxXRange=None, minYRange=None, maxYRange=None)

        if not symbol or symbol not in self.atr_history:
            self.atr_plot.setTitle(f"ATR History for {symbol}")
            return

        # Get the candle size setting for this symbol to plot the correct data
        candle_size = self.get_candle_size(symbol)
        self.atr_plot.setTitle(f"ATR History for {symbol} ({candle_size})")
        self.atr_plot.setLabel('left', 'ATR Value')
        self.atr_plot.setLabel('bottom', 'Time')
        self.atr_plot.showGrid(x=True, y=True)

        # The data is now nested: symbol -> candle_size -> {timestamp: atr}
        symbol_candle_data = self.atr_history.get(symbol, {}).get(candle_size, {})
        if not symbol_candle_data:
            return

        # Sort data by timestamp and prepare for plotting
        valid_timestamps = [ts for ts in symbol_candle_data.keys() if 'T' in ts]
        sorted_timestamps = sorted(valid_timestamps)
        x_data = [datetime.fromisoformat(ts).timestamp() for ts in sorted_timestamps]
        y_data = [symbol_candle_data[ts] for ts in sorted_timestamps]

        if not y_data:
            return

        # --- Y-Axis Scaling ---
        max_atr = max(y_data)
        # Limit Y zoom to 3x the max ATR value.
        # yMin=0 ensures we don't see negative ATR.
        # maxYRange ensures we don't zoom out past 3x max_atr.
        view_box.setLimits(yMin=0, maxYRange=max_atr * 3)
        view_box.setYRange(0, max_atr * 3, padding=0)

        # --- X-Axis Scaling ---
        seconds_in_day = 86400
        max_x_span = None
        
        if candle_size == "15 mins":
            max_x_span = 3 * seconds_in_day
        elif candle_size == "1 hour":
            max_x_span = 7 * seconds_in_day
        elif candle_size == "1 day":
            max_x_span = 30 * seconds_in_day
            
        if max_x_span:
            view_box.setLimits(maxXRange=max_x_span)
            # Set initial view to the most recent data within the span
            if x_data:
                last_ts = x_data[-1]
                half_span = max_x_span / 2
                view_box.setXRange(last_ts - half_span, last_ts + half_span, padding=0)

        # Create and configure the DateAxisItem for the bottom axis
        axis = pg.DateAxisItem()
        axis.setStyle(tickTextOffset=10, autoExpandTextSpace=True)

        # Set tick spacing based on the candle size for clarity
        if candle_size == "1 day":
            # 3M view: Major ticks per week, minor per day
            axis.setTickSpacing(86400 * 7, 86400)
        elif candle_size == "1 hour":
            # 1W view: Major ticks per day, minor per 6 hours
            axis.setTickSpacing(86400, 3600 * 6)
        elif candle_size == "15 mins":
            # 2D view: Major ticks per 4 hours, minor per hour
            axis.setTickSpacing(3600 * 4, 3600)

        self.atr_plot.plotItem.setAxisItems({'bottom': axis})
        
        pen_color = getattr(self, 'plot_pen', 'y')
        self.atr_plot.plot(x_data, y_data, pen=pg.mkPen(pen_color, width=2), symbol='o', symbolBrush=pen_color, symbolSize=5)

    def update_log_visibility(self):
        """Updates the visibility of the log view based on the setting."""
        self.log_label.setVisible(self.debug_log_enabled)
        self.log_view.setVisible(self.debug_log_enabled)

    def open_settings_window(self):
        """Opens the settings dialog window."""
        dialog = SettingsWindow(self)
        if dialog.exec():  # This is a blocking call
            try:
                # Update Client ID
                new_client_id = int(dialog.client_id_edit.text())
                if self.client_id != new_client_id:
                    self.client_id = new_client_id
                    # self.log_to_ui(f"Client ID updated to {self.client_id}. Changes will apply on next refresh.")
                    self.client_id_label.setText(f"Client ID: {self.client_id}")

                # Update Trading Mode
                new_trading_mode = dialog.trading_mode_combo.currentText()
                if self.trading_mode != new_trading_mode:
                    self.trading_mode = new_trading_mode
                    # self.log_to_ui(f"Trading Mode set to {self.trading_mode}. Changes will apply on next refresh.")
                    self.trading_mode_label.setText(f"Mode: {self.trading_mode}")

                # Update Debug Log
                if self.debug_log_enabled != dialog.debug_log_check.isChecked():
                    self.debug_log_enabled = dialog.debug_log_check.isChecked()
                    self.update_log_visibility()

                # Update Debug Full Log
                if self.debug_full_log_enabled != dialog.debug_full_log_check.isChecked():
                    self.debug_full_log_enabled = dialog.debug_full_log_check.isChecked()
                    self.update_full_log_state()
                
                # Update Theme
                if self.theme != dialog.theme_combo.currentText():
                    self.theme = dialog.theme_combo.currentText()
                    self.apply_theme()

                self.save_user_settings() # Save all settings at once
            except ValueError:
                # self.log_to_ui("Invalid Client ID entered. It must be an integer.")
                pass
    def log_to_ui(self, message):
        """Appends a message to the log view and auto-scrolls to the bottom."""
        self.log_view.append(message)
        # Ensure the view scrolls to the latest message
        self.log_view.verticalScrollBar().setValue(self.log_view.verticalScrollBar().maximum())    

    def update_full_log_state(self):
        root_logger = logging.getLogger()
        if self.debug_full_log_enabled:
            if self.qt_log_handler not in root_logger.handlers:
                root_logger.addHandler(self.qt_log_handler)
        else:
            if self.qt_log_handler in root_logger.handlers:
                root_logger.removeHandler(self.qt_log_handler)
    
    def apply_theme(self):
        """Applies the selected theme (Light/Dark) to the application."""
        if self.theme == "Light":
            # Light Theme
            bg_color = "#F9F9F9" # Tasteful off-white
            fg_color = "#000000"
            table_bg = "#FFFFFF"
            table_alt_bg = "#F0F0F0"
            input_bg = "#FFFFFF"
            border_color = "#CCCCCC"
            self.plot_pen = 'b' # Blue for light theme
            
            self.atr_plot.setBackground('w')
            self.atr_plot.getAxis('bottom').setPen('k')
            self.atr_plot.getAxis('left').setPen('k')

            stylesheet = f"""
                QMainWindow, QWidget {{ background-color: {bg_color}; color: {fg_color}; font-family: "Segoe UI", "Helvetica Neue", Helvetica, Arial, sans-serif; font-size: 10pt; }}
                
                /* Tabs */
                QTabWidget::pane {{ border: 1px solid {border_color}; border-radius: 4px; top: -1px; }} 
                QTabBar::tab {{ background-color: #E0E0E0; color: {fg_color}; border: 1px solid {border_color}; border-bottom: none; border-top-left-radius: 4px; border-top-right-radius: 4px; padding: 6px 12px; margin-right: 2px; }}
                QTabBar::tab:selected {{ background-color: {table_bg}; font-weight: bold; border-bottom: 1px solid {table_bg}; }}
                QTabBar::tab:hover {{ background-color: #EEEEEE; }}

                /* Table */
                QTableWidget {{ background-color: {table_bg}; alternate-background-color: {table_alt_bg}; color: {fg_color}; gridline-color: {border_color}; border: 1px solid {border_color}; border-radius: 4px; }}
                QHeaderView::section {{ background-color: #E8E8E8; color: {fg_color}; border: 1px solid #D0D0D0; padding: 4px; font-weight: bold; }}
                QTableCornerButton::section {{ background-color: #E8E8E8; border: 1px solid #D0D0D0; }}
                
                /* Inputs & Combos */
                QTextEdit {{ background-color: {input_bg}; color: {fg_color}; border: 1px solid {border_color}; border-radius: 4px; padding: 4px; font-family: 'Courier New'; }}
                QLineEdit, QComboBox, QDoubleSpinBox {{ background-color: {input_bg}; color: {fg_color}; border: 1px solid {border_color}; border-radius: 4px; padding: 4px; }}
                QComboBox::drop-down {{ subcontrol-origin: padding; subcontrol-position: top right; width: 20px; border-left-width: 1px; border-left-color: {border_color}; border-left-style: solid; border-top-right-radius: 4px; border-bottom-right-radius: 4px; }}
                QComboBox QAbstractItemView {{ background-color: {input_bg}; border: 1px solid {border_color}; selection-background-color: {table_alt_bg}; selection-color: {fg_color}; }}
                QComboBox::down-arrow {{ image: none; border-left: 5px solid transparent; border-right: 5px solid transparent; border-top: 5px solid #666666; margin-top: 2px; margin-right: 2px; }}
                QDoubleSpinBox::up-button, QDoubleSpinBox::down-button {{ width: 16px; border-left: 1px solid {border_color}; background: #F0F0F0; }}
                QDoubleSpinBox::up-button:hover, QDoubleSpinBox::down-button:hover {{ background: #E0E0E0; }}
                
                /* Buttons */
                QPushButton {{ background-color: #FFFFFF; border: 1px solid {border_color}; border-radius: 4px; padding: 6px 12px; min-width: 60px; }}
                QPushButton:hover {{ background-color: #F0F0F0; border-color: #BBBBBB; }}
                QPushButton:pressed {{ background-color: #E0E0E0; border-color: #AAAAAA; }}
                
                /* Scrollbars */
                QScrollBar:vertical {{ border: none; background: #F0F0F0; width: 10px; margin: 0px; border-radius: 5px; }}
                QScrollBar::handle:vertical {{ background: #C0C0C0; min-height: 20px; border-radius: 5px; }}
                QScrollBar::handle:vertical:hover {{ background: #A0A0A0; }}
                QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0px; }}

                /* Specific overrides */
                QTableWidget QWidget {{ background-color: transparent; }}
                QTableWidget QComboBox {{ margin: 2px; background-color: {input_bg}; }}
                QTableWidget QDoubleSpinBox {{ margin: 2px; }}
            """
        else:
            # Dark Theme
            bg_color = "#2B2B2B"
            fg_color = "#FFFFFF"
            table_bg = "#000000"
            table_alt_bg = "#111111"
            input_bg = "#333333"
            border_color = "#555555"
            self.plot_pen = 'y' # Yellow for dark theme

            self.atr_plot.setBackground('k')
            self.atr_plot.getAxis('bottom').setPen('#A9B7C6')
            self.atr_plot.getAxis('left').setPen('#A9B7C6')

            stylesheet = f"""
                QMainWindow, QWidget {{ background-color: {bg_color}; color: {fg_color}; font-family: "Segoe UI", "Helvetica Neue", Helvetica, Arial, sans-serif; font-size: 10pt; }}
                
                /* Tabs */
                QTabWidget::pane {{ border: 1px solid {border_color}; border-radius: 4px; top: -1px; }}
                QTabBar::tab {{ background-color: #3C3F41; color: #BBBBBB; border: 1px solid {border_color}; border-bottom: none; border-top-left-radius: 4px; border-top-right-radius: 4px; padding: 6px 12px; margin-right: 2px; }}
                QTabBar::tab:selected {{ background-color: {bg_color}; color: {fg_color}; font-weight: bold; border-bottom: 1px solid {bg_color}; }}
                QTabBar::tab:hover {{ background-color: #454749; }}

                /* Table */
                QTableWidget {{ background-color: {table_bg}; alternate-background-color: {table_alt_bg}; color: {fg_color}; gridline-color: #333333; border: 1px solid {border_color}; border-radius: 4px; }}
                QHeaderView::section {{ background-color: #333333; color: {fg_color}; border: 1px solid {border_color}; padding: 4px; font-weight: bold; }}
                QTableCornerButton::section {{ background-color: #333333; border: 1px solid {border_color}; }}
                
                /* Inputs & Combos */
                QTextEdit {{ background-color: {bg_color}; color: #A9B7C6; border: 1px solid {border_color}; border-radius: 4px; padding: 4px; font-family: 'Courier New'; }}
                QLineEdit, QComboBox, QDoubleSpinBox {{ background-color: {input_bg}; color: {fg_color}; border: 1px solid {border_color}; border-radius: 4px; padding: 4px; }}
                QComboBox::drop-down {{ subcontrol-origin: padding; subcontrol-position: top right; width: 20px; border-left-width: 1px; border-left-color: {border_color}; border-left-style: solid; border-top-right-radius: 4px; border-bottom-right-radius: 4px; }}
                QComboBox QAbstractItemView {{ background-color: {input_bg}; border: 1px solid {border_color}; selection-background-color: #454749; selection-color: {fg_color}; }}
                QComboBox::down-arrow {{ image: none; border-left: 5px solid transparent; border-right: 5px solid transparent; border-top: 5px solid #AAAAAA; margin-top: 2px; margin-right: 2px; }}
                QDoubleSpinBox::up-button, QDoubleSpinBox::down-button {{ width: 16px; border-left: 1px solid {border_color}; background: #3C3F41; }}
                QDoubleSpinBox::up-button:hover, QDoubleSpinBox::down-button:hover {{ background: #4C4F51; }}
                
                /* Buttons */
                QPushButton {{ background-color: #3C3F41; border: 1px solid {border_color}; border-radius: 4px; padding: 6px 12px; min-width: 60px; color: {fg_color}; }}
                QPushButton:hover {{ background-color: #4C4F51; border-color: #666666; }}
                QPushButton:pressed {{ background-color: #2D2F31; border-color: #444444; }}
                
                /* Scrollbars */
                QScrollBar:vertical {{ border: none; background: {bg_color}; width: 10px; margin: 0px; border-radius: 5px; }}
                QScrollBar::handle:vertical {{ background: #555555; min-height: 20px; border-radius: 5px; }}
                QScrollBar::handle:vertical:hover {{ background: #666666; }}
                QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0px; }}

                /* Specific overrides */
                QTableWidget QWidget {{ background-color: transparent; }}
                QTableWidget QComboBox {{ margin: 2px; background-color: {input_bg}; }}
                QTableWidget QDoubleSpinBox {{ margin: 2px; }}
            """
        self.setStyleSheet(stylesheet)
        
        if self.symbol_selector.currentText():
            self.update_atr_graph()

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
        # Save current sort state to restore after repopulating
        current_sort_col = self.table.horizontalHeader().sortIndicatorSection()
        current_sort_order = self.table.horizontalHeader().sortIndicatorOrder()
        
        # Disable sorting during population to improve performance and prevent auto-sorting artifacts
        self.table.setSortingEnabled(False)
        
        self.table.setRowCount(len(self.positions_data))
        for i, p_data in enumerate(self.positions_data):
            try:
                symbol = p_data['symbol']
                
                # Column 0: "Send Stop" Checkbox
                # Add a hidden item for sorting based on enabled state
                is_enabled = self.symbol_stop_enabled.get(symbol, True)
                item_0 = NumericTableWidgetItem()
                item_0.setData(Qt.ItemDataRole.UserRole, 1 if is_enabled else 0)
                self.table.setItem(i, 0, item_0)

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
                
                # Column 2: Candle Size
                candle_options = ["15 mins", "1 hour", "1 day"]
                current_candle = self.get_candle_size(symbol)
                
                combo = QComboBox()
                combo.addItems(candle_options)
                combo.blockSignals(True)
                combo.setCurrentText(current_candle)
                combo.blockSignals(False)
                combo.currentTextChanged.connect(lambda text, s=symbol: self.on_candle_size_changed(s, text))
                self.table.setCellWidget(i, 2, combo)

                # Column 3: ATR - Get ATR value from ATR calculations tab
                atr_value = p_data.get('atr_value')
                atr_display = f"{atr_value:.4f}" if atr_value is not None else "N/A"
                item_2 = NumericTableWidgetItem(atr_display)
                item_2.setData(Qt.ItemDataRole.UserRole, atr_value if atr_value is not None else -1.0)
                self.table.setItem(i, 3, item_2)

                # Column 4: ATR Ratio editable spin box
                ratio_val = p_data.get('atr_ratio', 1.5)
                item_3 = NumericTableWidgetItem()
                item_3.setData(Qt.ItemDataRole.UserRole, ratio_val)
                self.table.setItem(i, 4, item_3)

                spin = QDoubleSpinBox()
                spin.setMinimum(0.1)
                spin.setMaximum(10.0)
                spin.setSingleStep(0.1)
                spin.setDecimals(1)
                spin.setValue(ratio_val)
                spin.setButtonSymbols(QAbstractSpinBox.ButtonSymbols.NoButtons)
                spin.valueChanged.connect(lambda val, row=i, symbol=symbol: self.on_atr_ratio_changed(row, symbol, val))
                self.table.setCellWidget(i, 4, spin)

                # Column 5: Positions Held
                pos_held = p_data['positions_held']
                item_4 = NumericTableWidgetItem(str(pos_held))
                item_4.setData(Qt.ItemDataRole.UserRole, pos_held)
                self.table.setItem(i, 5, item_4)
                
                # Column 6: Margin
                margin = p_data.get('margin', 0)
                item_5 = NumericTableWidgetItem(f"${margin:,.2f}")
                item_5.setData(Qt.ItemDataRole.UserRole, margin)
                self.table.setItem(i, 6, item_5)

                # Column 7: Avg Cost
                avg_cost = p_data.get('avg_cost', 0.0)
                item_6 = NumericTableWidgetItem(f"{avg_cost:,.2f}")
                item_6.setData(Qt.ItemDataRole.UserRole, avg_cost)
                self.table.setItem(i, 7, item_6)

                # Column 8: Current Price
                price = p_data.get('current_price', 0)
                item_7 = NumericTableWidgetItem(f"{price:.2f}")
                item_7.setData(Qt.ItemDataRole.UserRole, price)
                self.table.setItem(i, 8, item_7)

                # Column 9: Computed Stop Loss
                computed_stop = p_data.get('computed_stop_loss')
                stop_display = f"{computed_stop:.4f}" if computed_stop is not None else "N/A"
                item_8 = NumericTableWidgetItem(stop_display)
                item_8.setData(Qt.ItemDataRole.UserRole, computed_stop if computed_stop is not None else -1.0)
                self.table.setItem(i, 9, item_8)

                # Column 10: Stop Status Icon (New)
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

                self.table.setItem(i, 10, status_item)

                # Column 11: $ Risk
                risk_value = p_data.get('dollar_risk', 0)
                if risk_value == "NO RISK":
                    item_10 = NumericTableWidgetItem("NO RISK")
                    item_10.setData(Qt.ItemDataRole.UserRole, 0)
                    item_10.setBackground(QColor(0, 50, 0))
                    item_10.setForeground(QColor('lightgreen'))
                else:
                    item_10 = NumericTableWidgetItem(f"${risk_value:,.2f}")
                    item_10.setData(Qt.ItemDataRole.UserRole, risk_value)
                
                self.table.setItem(i, 11, item_10)

                # Column 12: % Risk
                percent_risk = p_data.get('percent_risk', 0.0)
                item_11 = NumericTableWidgetItem(f"{percent_risk:.2f}%")
                item_11.setData(Qt.ItemDataRole.UserRole, percent_risk)

                if percent_risk > 2.0:
                    item_11.setForeground(QColor('red'))
                self.table.setItem(i, 12, item_11)

                # Column 13: Status
                self.table.setItem(i, 13, QTableWidgetItem(p_data.get('status', '...')))

            except Exception as e:
                symbol = p_data.get('symbol', 'UNKNOWN')
                logging.error(f"Error populating table for symbol {symbol}: {e}")
        
        # Re-enable sorting
        self.table.setSortingEnabled(True)
        # Restore previous sort if it existed
        if current_sort_col != -1:
            try:
                self.table.sortItems(current_sort_col, current_sort_order)
            except Exception as e:
                logging.error(f"Error sorting table: {e}")

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
        self.table.item(row, 9).setText(f"{new_stop:.4f}" if new_stop is not None and isinstance(new_stop, (int, float)) else "N/A")
        
        item_10 = self.table.item(row, 11)
        if new_risk_dollar == "NO RISK":
            item_10.setText("NO RISK")
            item_10.setData(Qt.ItemDataRole.UserRole, 0)
            item_10.setBackground(QColor(0, 50, 0))
            item_10.setForeground(QColor('lightgreen'))
        else:
            item_10.setText(f"${new_risk_dollar:,.2f}")
            item_10.setData(Qt.ItemDataRole.UserRole, new_risk_dollar)
            item_10.setData(Qt.ItemDataRole.BackgroundRole, None)
            item_10.setData(Qt.ItemDataRole.ForegroundRole, None)
            
        self.table.item(row, 12).setText(f"{new_risk_percent:.2f}%")

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
                    self.debug_log_enabled = settings.get('debug_log_enabled', True)
                    self.debug_full_log_enabled = settings.get('debug_full_log_enabled', False)
                    self.theme = settings.get('theme', self.theme)
                    # Load symbol toggles
                    self.symbol_stop_enabled = settings.get('symbol_stop_enabled', {})
                    self.symbol_candle_size = settings.get('symbol_candle_size', {})
                    self.column_widths = settings.get('column_widths', {})
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading user settings: {e}")
                self.symbol_stop_enabled = {}
                self.symbol_candle_size = {}
                self.column_widths = {}
        else:
            self.symbol_stop_enabled = {}
            self.symbol_candle_size = {}
            self.column_widths = {}

    def save_user_settings(self):
        """Save all user settings to user_settings.json"""
        try:
            # Capture current column widths if table exists
            if hasattr(self, 'table'):
                current_widths = {}
                for i in range(self.table.columnCount()):
                    current_widths[str(i)] = self.table.columnWidth(i)
                self.column_widths = current_widths

            with open(self.user_settings_file, 'w') as f:
                settings_to_save = {
                    'client_id': self.client_id,
                    'trading_mode': self.trading_mode,
                    'debug_log_enabled': self.debug_log_enabled,
                    'debug_full_log_enabled': self.debug_full_log_enabled,
                    'theme': self.theme,
                    'symbol_stop_enabled': self.symbol_stop_enabled,
                    'symbol_candle_size': self.symbol_candle_size,
                    'column_widths': self.column_widths,
                    # Add any other settings here in the future
                }
                json.dump(settings_to_save, f, indent=2)
            logging.info("User settings saved successfully")
        except Exception as e:
            print(f"Error saving user settings: {e}")

    def get_candle_size(self, symbol):
        return self.symbol_candle_size.get(symbol, "1 day")

    def set_candle_size(self, symbol, size):
        self.symbol_candle_size[symbol] = size
        self.save_user_settings()

    def get_all_candle_sizes(self):
        return self.symbol_candle_size

    def on_candle_size_changed(self, symbol, new_size):
        current_size = self.get_candle_size(symbol)
        if current_size == new_size:
            return

        logging.info(f"Candle size for {symbol} changed from {current_size} to {new_size}. Wiping history.")
        self.set_candle_size(symbol, new_size)

        # Wipe ATR state and history to force re-initialization
        if symbol in self.atr_state:
            del self.atr_state[symbol]
            self.save_atr_state()
        
        if symbol in self.atr_history:
            del self.atr_history[symbol]
            self.save_atr_history()
            
        # self.log_to_ui(f"History wiped for {symbol} due to timeframe change. ATR will re-initialize.")
        
        # Refresh graph if needed
        if self.symbol_selector.currentText() == symbol:
            self.update_atr_graph()

    def on_symbol_toggle_changed(self, symbol, state):
        """Handles when a user toggles the checkbox for an individual symbol."""
        is_enabled = state == Qt.CheckState.Checked.value
        self.symbol_stop_enabled[symbol] = is_enabled
        logging.info(f"Stop loss submission for {symbol} set to: {'ENABLED' if is_enabled else 'DISABLED'}")

        # If the user disables the symbol, reset its stop loss ratchet.
        if not is_enabled and symbol in self.highest_stop_losses:
            del self.highest_stop_losses[symbol]
            self.save_stop_history() # Persist the change immediately
            # self.log_to_ui(f"Ratchet for {symbol} has been reset. Its stop loss history is cleared.")
            logging.info(f"Removed {symbol} from highest_stop_losses to reset ratchet.")

    def populate_atr_table(self):
        """Populate the ATR Calculations table"""
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
            atr_item.setFlags(atr_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.atr_table.setItem(i, 3, atr_item)

    def load_stop_history(self):
        """Load the persistent stop loss history from stop_history.json."""
        if os.path.exists(self.stop_history_file):
            try:
                with open(self.stop_history_file, 'r') as f:
                    history = json.load(f)
                    if not isinstance(history, dict):
                        logging.warning(f"Stop history file is corrupt (not a dictionary). Ignoring. Path: {self.stop_history_file}")
                        return {}
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
        with self.atr_state_file_lock:
            try:
                with open(self.atr_state_file, 'w') as f:
                    json.dump(self.atr_state, f, indent=2)
                logging.info("ATR state saved successfully")
            except Exception as e:
                print(f"Error saving ATR state: {e}")

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

    def on_worker_finished(self, success):
        """Called when the worker's run() method completes."""
        logging.info("Worker has finished all stages.")
        self.update_status(success)
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
        # self.log_to_ui(f"Error fetching data: {error_message}")
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
            # self.log_to_ui(">>> Adaptive stop loss submission ENABLED <<<")
        else:
            self.send_adaptive_stops = False
            self.adaptive_stop_toggle.setText("OFF")
            # self.log_to_ui(">>> Adaptive stop loss submission DISABLED <<<")

    def update_status(self, connected):
        """Updates the connection status label in the UI."""
        if connected:
            self.connection_status.setText("Connected")
            self.connection_status.setStyleSheet("color: green; font-weight: bold;")
        else:
            self.connection_status.setText("Disconnected")
            self.connection_status.setStyleSheet("color: red; font-weight: bold;")


def main():
    if sys.platform == "win32":
        import ctypes
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
            "PaceChaser.App"
        )

    app = QApplication(sys.argv)
    
    icon_path = resource_path(os.path.join("windows assets", "PaceChaser.ico"))
    if not os.path.exists(icon_path):
        logging.warning(f"Icon file not found at: {icon_path}")

    app.setWindowIcon(QIcon(icon_path))
    window = ATRWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
