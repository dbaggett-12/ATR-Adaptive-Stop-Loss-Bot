# main.py
import sys
import asyncio
import logging
import random
import json
import os
from datetime import datetime, timedelta, time
from PyQt6 import QtGui, QtCore
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QTableWidget, QTableWidgetItem, QVBoxLayout,
    QWidget, QDoubleSpinBox, QTabWidget, QTextEdit, QPushButton, QHeaderView, QAbstractSpinBox,
    QLabel, QHBoxLayout, QCheckBox
)
from PyQt6.QtCore import Qt, QTimer, QSize, QObject, QThread, pyqtSignal
from PyQt6.QtGui import QMovie
from ibkr_api import get_market_statuses_for_all, _submit_stop_loss_orders_internal, fetch_basic_positions, fetch_market_data_for_positions
from ib_insync import IB, Future, Stock, Contract
import pandas as pd

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
ATR_HISTORY_FILE = 'atr_history.json'

class DataWorker(QObject):
    """
    Worker thread for fetching and processing all IBKR data.
    This runs in the background to keep the UI responsive.
    """
    finished = pyqtSignal(str) # Pass stage name
    error = pyqtSignal(str)
    positions_ready = pyqtSignal(list)
    market_data_ready = pyqtSignal(list)
    atr_ready = pyqtSignal(dict)
    orders_submitted = pyqtSignal(list)
    
    def __init__(self, atr_window, atr_ratios, highest_stop_losses):
        super().__init__()
        # Give worker access to the main window's methods and data
        self.atr_window = atr_window
        self.send_adaptive_stops = atr_window.send_adaptive_stops
        self.atr_ratios = atr_ratios
        self.highest_stop_losses = highest_stop_losses
        self.stage = "initial"
        self.positions_data = []

    def run(self):
        """Main worker method, executes a specific stage."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        ib = IB()
        try:
            client_id = random.randint(100, 999)
            ib.connect('127.0.0.1', 7497, clientId=client_id)

            if self.stage == 'fetch_positions':
                positions = ib.positions()
                basic_positions = fetch_basic_positions(ib, positions)
                self.positions_ready.emit(basic_positions)

            elif self.stage == 'fetch_market_data':
                enriched_positions = fetch_market_data_for_positions(ib, self.positions_data)
                self.market_data_ready.emit(enriched_positions)

            elif self.stage == 'calculate_atr':
                contract_details_map = {p['symbol']: p['contract_details'] for p in self.positions_data}
                market_statuses = get_market_statuses_for_all(contract_details_map)
                self.atr_window.market_statuses = market_statuses # Update main window
                
                symbols = [p['symbol'] for p in self.positions_data]
                atr_results = self.atr_window.calculate_atr_for_symbols(symbols, market_statuses, contract_details_map)
                self.atr_ready.emit(atr_results)

            elif self.stage == 'submit_orders':
                if not self.send_adaptive_stops:
                    logging.info("Adaptive stops are disabled, skipping order submission.")
                    self.orders_submitted.emit([])
                    return

                stop_loss_data = self.build_stop_loss_data()
                # stop_loss_data now contains both orders to submit and statuses for skipped items
                final_results = stop_loss_data.get('statuses_only', [])
                orders_to_submit = stop_loss_data.get('orders_to_submit', {})

                if orders_to_submit:
                    submission_results = _submit_stop_loss_orders_internal(ib, orders_to_submit)
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
            self.finished.emit(self.stage)
            loop.close()

    def build_stop_loss_data(self):
        """Prepares the data structure for submitting stop loss orders."""
        orders_to_submit = {}
        statuses_only = []

        for i, p_data in enumerate(self.positions_data):
            symbol = p_data['symbol']
            if self.atr_window.market_statuses.get(symbol) == 'CLOSED':
                logging.info(f"Worker: Skipping stop loss for {symbol}: Market is closed.")
                statuses_only.append({
                    'symbol': symbol,
                    'status': 'held',
                    'message': 'Market Closed'
                })
                continue

            atr_value = None
            # Get the highest stop loss computed so far (the one ratcheting up)
            highest_stop = self.atr_window.highest_stop_losses.get(symbol, 0)
            # Get the current ATR ratio from the UI
            atr_ratio = self.atr_ratios[i] if i < len(self.atr_ratios) else 1.5


            if symbol in self.atr_window.atr_symbols:
                try:
                    atr_index = self.atr_window.atr_symbols.index(symbol)
                    atr_value = self.atr_window.atr_calculated[atr_index]
                except (ValueError, IndexError):
                    pass

            # This computes the *potential* new stop loss, without the ratcheting logic
            potential_new_stop = self.atr_window.compute_stop_loss(
                p_data,
                p_data['current_price'],
                atr_value,
                atr_ratio,
                apply_ratchet=False # We need the raw computed value to compare
            )

            if potential_new_stop <= 0:
                logging.info(f"Worker: Skipping {symbol}: No valid stop price computed.")
                statuses_only.append({
                    'symbol': symbol,
                    'status': 'error',
                    'message': 'Invalid stop price computed'
                })
                continue

            # Ratchet check: if the new stop is lower than the highest one, hold the order
            if highest_stop > 0 and potential_new_stop < highest_stop:
                logging.info(f"Worker: Holding stop for {symbol}. New stop {potential_new_stop:.2f} is lower than existing {highest_stop:.2f}")
                statuses_only.append({
                    'symbol': symbol,
                    'status': 'held',
                    'message': 'Stop Held'
                })
                continue

            orders_to_submit[symbol] = {
                'stop_price': potential_new_stop, # Use the new valid stop price
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
        self.contract_details_map = {}  # Store contract details by symbol
        
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
        self.atr_history = self.load_atr_history()
        
        # Track user-submitted ATR values (symbol -> timestamp when user submitted)
        self.user_submitted_atr = {}  # {symbol: timestamp_key}
        
        # Adaptive Stop Loss toggle state
        self.send_adaptive_stops = True
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
        self.adaptive_stop_toggle.setChecked(True)
        self.adaptive_stop_toggle.stateChanged.connect(self.on_adaptive_stop_toggled)
        self.adaptive_stop_toggle.setText("ON")
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

        # --- Positions Table Tab ---
        self.positions_tab = QWidget()
        self.positions_layout = QVBoxLayout()
        self.positions_tab.setLayout(self.positions_layout)
        self.tabs.addTab(self.positions_tab, "Positions")

        self.table = QTableWidget()
        self.table.setColumnCount(9)
        self.table.setHorizontalHeaderLabels([
            "Position", "ATR", "ATR Ratio", "Positions Held",
            "Current Price", "Computed Stop Loss", "$ Risk", "% Risk", "Status"
        ])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setSectionsMovable(True)
        # Make the "Status" column (index 6) wider to accommodate text
        self.table.setColumnWidth(8, 240)
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
            pos = p_data['position']
            symbol = p_data['symbol']
            
            # Column 0: Position
            position_item = QTableWidgetItem(pos)
            market_status = self.market_statuses.get(symbol, 'UNKNOWN')

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
            self.table.setItem(i, 0, position_item)
            
            # Column 1: ATR - Get ATR value from ATR calculations tab
            atr_value = None
            atr_display = "N/A"
            if symbol in self.atr_symbols:
                atr_index = self.atr_symbols.index(symbol)
                if self.atr_calculated[atr_index] is not None:
                    atr_value = self.atr_calculated[atr_index]
                    atr_display = f"{atr_value:.2f}"
            self.table.setItem(i, 1, QTableWidgetItem(atr_display))

            # Column 2: ATR Ratio editable spin box
            spin = QDoubleSpinBox()
            spin.setMinimum(0.0)
            spin.setMaximum(10.0)
            spin.setSingleStep(0.1)
            spin.setDecimals(1)
            spin.setValue(self.atr_ratios[i])
            spin.setButtonSymbols(QAbstractSpinBox.ButtonSymbols.NoButtons)
            spin.valueChanged.connect(lambda val, row=i: self.update_atr_ratio(row, val))
            self.table.setCellWidget(i, 2, spin)

            # Column 3: Positions Held
            self.table.setItem(i, 3, QTableWidgetItem(str(p_data['positions_held'])))
            
            # Column 4: Current Price
            self.table.setItem(i, 4, QTableWidgetItem(f"{p_data['current_price']:.2f}"))

            # Column 5: Computed Stop Loss = Current Price - (ATR × ATR Ratio)
            # Stop loss never goes down - use highest computed value
            computed_stop = self.compute_stop_loss(p_data, p_data['current_price'], atr_value, self.atr_ratios[i]) # self.atr_ratios is still correct by index
            stop_item = QTableWidgetItem(f"{computed_stop:.2f}" if computed_stop else "N/A")
            self.table.setItem(i, 5, stop_item)

            # Column 6: $ Risk
            risk_value = 0
            if computed_stop and p_data['current_price'] > 0:
                risk_in_points = p_data['current_price'] - computed_stop
                multiplier = p_data.get('multiplier', 1.0) # Get the contract multiplier
                risk_value = risk_in_points * abs(p_data['positions_held']) * multiplier # Risk = (Price - Stop) * Quantity * Multiplier
            risk_item = QTableWidgetItem(f"${risk_value:,.2f}")
            self.table.setItem(i, 6, risk_item)

            # Column 7: % Risk
            hypothetical_account_value = 6000.0
            percent_risk = 0.0
            if hypothetical_account_value > 0:
                percent_risk = (risk_value / hypothetical_account_value) * 100
            percent_risk_item = QTableWidgetItem(f"{percent_risk:.2f}%")
            
            # Turn the text red if risk is over 2%
            if percent_risk > 2.0:
                percent_risk_item.setForeground(QtGui.QColor('red'))

            self.table.setItem(i, 7, percent_risk_item)
    
            # Column 8: Status
            # Note: self.statuses[i] corresponds to the index of the symbol in self.symbols, not necessarily p_data
            self.table.setItem(i, 8, QTableWidgetItem(self.statuses[i]))

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
        
        # Calculate new stop loss
        new_stop = current_price - (atr_value * atr_ratio)

        # --- Round to minTick ---
        contract_details = self.contract_details_map.get(symbol, {})
        min_tick = contract_details.get('minTick', 0.01)  # Default to 0.01
        quantity = position_data.get('positions_held', 0)

        if min_tick > 0:
            # For long positions (SELL stop), round down to be more conservative
            if quantity > 0:
                rounded_stop = (new_stop // min_tick) * min_tick
            # For short positions (BUY stop), round up to be more conservative
            elif quantity < 0:
                rounded_stop = ((new_stop + min_tick - 1e-9) // min_tick) * min_tick # Add epsilon for precision
            else: # No position
                rounded_stop = new_stop
        else:
            rounded_stop = new_stop

        # Get previous highest stop loss for this symbol
        prev_highest = self.highest_stop_losses.get(symbol, 0)
        
        if apply_ratchet:
            # Stop loss never goes down - use max of new and previous
            if rounded_stop > prev_highest:
                self.highest_stop_losses[symbol] = rounded_stop
                return rounded_stop
            else:
                return prev_highest
        else:
            return rounded_stop # Return the raw computed value if not ratcheting

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
        """Load ATR history from JSON file"""
        if os.path.exists(self.atr_history_file):
            try:
                with open(self.atr_history_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading ATR history: {e}")
                return {}
        return {}
    
    def save_atr_history(self):
        """Save ATR history to JSON file"""
        try:
            with open(self.atr_history_file, 'w') as f:
                json.dump(self.atr_history, f, indent=2)
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
            
            # If symbol has no more data, mark for removal
            if not self.atr_history[symbol]:
                symbols_to_remove.append(symbol)
        
        # Remove symbols with no data
        for symbol in symbols_to_remove:
            del self.atr_history[symbol]
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
                self.save_today_atr(symbol, new_atr_value)

                # Repopulate the positions table to update computed stop loss
                self.populate_positions_table()

            except ValueError:
                logging.warning(f"Invalid input for ATR: '{item.text()}'. Please enter a number.")
                # Optionally, revert to the old value or show an error
                if self.atr_calculated[row] is not None:
                    item.setText(f"{self.atr_calculated[row]:.2f}")

    def recalculate_single_symbol_atr(self, row, symbol, prior_atr):
        """Recalculate ATR for a single symbol with the given prior ATR"""
        ib = IB()
        try:
            ib.connect('127.0.0.1', 7497, clientId=random.randint(100, 999))
            
            # Get contract details from position data if available
            contract_info = self.contract_details_map.get(symbol, {})
            
            # Create contract using actual details from position
            if contract_info.get('conId'):
                # Use conId for most accurate contract identification
                contract = Contract(
                    conId=contract_info['conId'],
                    exchange=contract_info.get('exchange', '') # Explicitly add exchange
                )
                ib.qualifyContracts(contract)
                logging.info(f"Recalculating ATR for conId {contract.conId} on exchange '{contract.exchange}'")
            elif contract_info.get('lastTradeDateOrContractMonth'):
                # Use actual expiration date from position
                contract = Future(
                    symbol=symbol,
                    lastTradeDateOrContractMonth=contract_info['lastTradeDateOrContractMonth'],
                    exchange=contract_info.get('exchange', 'CME'),
                    currency=contract_info.get('currency', 'USD')
                )
                ib.qualifyContracts(contract)
            else:
                print(f"No contract details available for {symbol}")
                return
            
            bars = ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr='1 D',
                barSizeSetting='15 mins',
                whatToShow='TRADES',
                useRTH=True,
                formatDate=1
            )
            
            if bars and len(bars) >= 2:
                df = pd.DataFrame([{
                    'open': b.open,
                    'high': b.high,
                    'low': b.low,
                    'close': b.close,
                    'volume': b.volume
                } for b in bars])
                
                tr, atr = self.calculate_tr_and_atr(df, prior_atr=prior_atr, symbol=symbol)
                
                if tr is not None and atr is not None:
                    # Update the values
                    self.tr_values[row] = tr
                    self.atr_calculated[row] = atr
                    
                    self.atr_table.item(row, 2).setText(f"{tr:.2f}")
                    self.atr_table.item(row, 3).setText(f"{atr:.2f}")
                    # Save today's ATR
                    self.save_today_atr(symbol, atr)
                    logging.info(f"Updated: Symbol: {symbol}, TR: {tr:.2f}, ATR: {atr:.2f}")
                    
                    # Update the Positions table to show the new ATR value
                    self.populate_positions_table()
                    
        except Exception as e:
            print(f"Error recalculating ATR for {symbol}: {e}")
        finally:
            if ib.isConnected():
                ib.disconnect()

    def on_adaptive_stop_toggled(self, state):
        """Handle adaptive stop loss toggle change"""
        self.send_adaptive_stops = self.adaptive_stop_toggle.isChecked()
        # Update checkbox text
        if self.send_adaptive_stops:
            self.adaptive_stop_toggle.setText("ON")
            # Trigger a data fetch and order submission cycle immediately when enabled
            logging.info("Adaptive stops enabled. Triggering a full data refresh.")
            self.start_data_fetch_thread()
        else:
            self.adaptive_stop_toggle.setText("OFF")
        status_text = "ENABLED" if self.send_adaptive_stops else "DISABLED"
        logging.info(f"Adaptive Stop Losses: {status_text}")

    def update_atr_ratio(self, row, value):
        self.atr_ratios[row] = value
        # Recalculate and update stop loss when ratio changes
        self.populate_positions_table()

    def update_status(self, connected):
        """Update the connection status display"""
        if connected:
            self.connection_status.setText("Connected")
            self.connection_status.setStyleSheet("color: green; font-weight: bold;")
        else:
            self.connection_status.setText("Disconnected")
            self.connection_status.setStyleSheet("color: red; font-weight: bold;")
    
    def update_timestamp(self):
        """Update the last data pull timestamp"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.last_pull_time.setText(current_time)

    def calculate_atr_for_symbols(self, symbols, market_statuses, contract_details_map):
        """Calculate ATR for each symbol in the list"""
        # This function is now called by the worker, so it should not modify self directly
        # but return the results.
        atr_symbols = []
        tr_values = []
        atr_calculated = []
        previous_atr_values = []

        ib = IB()
        try:
            ib.connect('127.0.0.1', 7497, clientId=random.randint(100, 999))
            
            for symbol in symbols:
                try:
                    # Use the market statuses passed into the function
                    if market_statuses.get(symbol) == 'CLOSED':
                        logging.info(f"Market for {symbol} is closed. Skipping ATR calculation and using last known value.")
                        previous_atr = self.get_previous_atr(symbol)
                        # When closed, current ATR is the same as the last known ATR, and TR is 0
                        atr_symbols.append(symbol)
                        tr_values.append(0) # No new candle, so TR is 0
                        atr_calculated.append(previous_atr)
                        previous_atr_values.append(previous_atr)
                        continue # Move to the next symbol

                    # Get previous ATR from history
                    previous_atr = self.get_previous_atr(symbol)
                    
                    # Get contract details from position data if available
                    contract_info = contract_details_map.get(symbol, {})
                    sec_type = contract_info.get('secType', 'FUT')
                    
                    # Skip stocks/ETFs for ATR calculation (they use different method)
                    if sec_type == 'STK':
                        print(f"Skipping {symbol} - Stock/ETF (ATR calculation not applicable)")
                        continue
                    
                    # Create contract using actual details from position
                    if contract_info.get('conId'):
                        # Use conId and exchange for most accurate contract identification
                        contract = Contract(
                            conId=contract_info['conId'],
                            exchange=contract_info.get('exchange', '') # Explicitly add exchange
                        )
                        ib.qualifyContracts(contract)
                        logging.info(f"Using conId {contract_info['conId']} and exchange '{contract.exchange}' for {symbol}")
                    elif contract_info.get('lastTradeDateOrContractMonth'):
                        # Use actual expiration date from position
                        contract = Future(
                            symbol=symbol,
                            lastTradeDateOrContractMonth=contract_info['lastTradeDateOrContractMonth'],
                            exchange=contract_info.get('exchange', 'CME'), # Default to CME if not specified
                            currency=contract_info.get('currency', 'USD')
                        )
                        ib.qualifyContracts(contract)
                        print(f"Using expiration {contract_info['lastTradeDateOrContractMonth']} for {symbol}")
                    else:
                        # Fallback - skip if we don't have contract details
                        print(f"No contract details available for {symbol}, skipping ATR calculation")
                        continue
                    
                    # Get historical bars (1 day of 15-minute candles for ATR calculation)
                    bars = ib.reqHistoricalData(
                        contract,
                        endDateTime='',
                        durationStr='1 D',
                        barSizeSetting='15 mins',
                        whatToShow='TRADES',
                        useRTH=True,
                        formatDate=1
                    )
                    
                    if not bars or len(bars) < 2:
                        print(f"Not enough historical data for {symbol}")
                        continue
                    
                    df = pd.DataFrame([{
                        'open': b.open,
                        'high': b.high,
                        'low': b.low,
                        'close': b.close,
                        'volume': b.volume
                    } for b in bars])
                    
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
                    
                    # Check if the user has already manually entered an ATR for this interval
                    if (symbol in self.atr_history and 
                        completed_interval_key in self.atr_history[symbol] and
                        symbol in self.atr_symbols):
                        # If a manual edit happened, the value in atr_calculated is the source of truth
                        atr_index = self.atr_symbols.index(symbol)
                        current_atr = self.atr_calculated[atr_index]
                        logging.info(f"User-edited ATR for {symbol} exists for interval {completed_interval_key}. Using value: {current_atr:.2f}")


                    # Check if ATR already exists for this completed interval
                    elif symbol in self.atr_history and completed_interval_key in self.atr_history[symbol]:
                        # ATR already calculated for this completed interval - use existing value
                        current_atr = self.atr_history[symbol][completed_interval_key]
                        logging.info(f"Symbol: {symbol}, ATR already calculated for completed interval {completed_interval_key}: {current_atr:.2f} (skipping calculation)")
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
        
        except Exception as e:
            print(f"Error connecting to IBKR for ATR calculations: {e}")
        finally:
            if ib.isConnected():
                ib.disconnect()
        
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
        self.start_worker_stage('fetch_positions')

    def start_worker_stage(self, stage, positions_data=None):
        """Creates and starts a worker for a specific stage."""
        self.thread = QThread()
        self.worker = DataWorker(self, self.atr_ratios, self.highest_stop_losses)
        self.worker.stage = stage
        if positions_data:
            self.worker.positions_data = positions_data

        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.on_worker_finished)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.worker.error.connect(self.handle_data_error)

        # Connect stage-specific signals
        if stage == 'fetch_positions':
            self.worker.positions_ready.connect(self.handle_positions_ready)
        elif stage == 'fetch_market_data':
            self.worker.market_data_ready.connect(self.handle_market_data_ready)
        elif stage == 'calculate_atr':
            self.worker.atr_ready.connect(self.handle_atr_ready)
        elif stage == 'submit_orders':
            self.worker.orders_submitted.connect(self.handle_orders_submitted)

        self.thread.start()
        logging.info(f"Worker started for stage: {stage}")

    def on_worker_finished(self, stage):
        """Called when any worker stage finishes. Triggers the next stage."""
        # Ensure the thread is properly quit before we proceed.
        if self.thread and self.thread.isRunning():
            self.thread.quit()
            self.thread.wait() # Wait for the thread to fully terminate

        logging.info(f"Worker finished stage: {stage}")

        if stage == 'fetch_positions':
            self.start_worker_stage('fetch_market_data', self.all_positions_details)
        elif stage == 'fetch_market_data':
            self.start_worker_stage('calculate_atr', self.all_positions_details)
        elif stage == 'calculate_atr':
            self.start_worker_stage('submit_orders', self.all_positions_details)
        elif stage == 'submit_orders':
            self.update_status(True)
            self.update_timestamp()
            logging.info("All data loading stages complete.")

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
                    self.statuses[idx] = f"Order Held - {message}"
                elif status == 'pending': # IBKR rejected or has non-final status
                    self.statuses[idx] = f"Order Rejected - {message}"
                elif status in ['error', 'skipped']:
                    self.statuses[idx] = f"Error - {message}"
        self.populate_positions_table()

def main():
    app = QApplication(sys.argv)
    window = ATRWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
