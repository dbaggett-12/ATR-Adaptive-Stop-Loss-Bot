# main.py
import sys
import logging
import random
import json
import os
from datetime import datetime, timedelta, time
from PyQt6 import QtGui
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QTableWidget, QTableWidgetItem, QVBoxLayout,
    QWidget, QDoubleSpinBox, QTabWidget, QTextEdit, QPushButton, QHeaderView,
    QLabel, QHBoxLayout, QCheckBox
)
from PyQt6.QtCore import Qt, QTimer
from ibkr_api import fetch_positions, submit_stop_loss_orders  # Import our separate IBKR module
from ib_insync import IB, Future, Stock, Contract
import pandas as pd

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
ATR_HISTORY_FILE = 'atr_history.json'


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
        self.computed_stop_losses = []  # Current computed stop losses
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
        self.send_adaptive_stops = False

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
        self.table.setColumnCount(7)
        self.table.setHorizontalHeaderLabels([
            "Position", "ATR", "ATR Ratio", "Status",
            "Positions Held", "Current Price", "Computed Stop Loss"
        ])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setSectionsMovable(True)
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
        # Setup auto-refresh timer (60 seconds = 60000 ms)
        self.refresh_timer = QTimer(self)
        self.refresh_timer.timeout.connect(self.fetch_ibkr_data)
        self.refresh_timer.start(60000)  # Refresh every 60 seconds
        
        # Fetch data immediately on startup
        self.fetch_ibkr_data()
    
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

        # Calculate current TR
        prev_close = df['close'].iloc[-2]
        current_high = df['high'].iloc[-1]
        current_low = df['low'].iloc[-1]

        # Check if market data is the same as previous calculation (market closure)
        if symbol and symbol in self.last_market_data:
            last_data = self.last_market_data[symbol]
            if (last_data['high'] == current_high and 
                last_data['low'] == current_low and 
                last_data['prev_close'] == prev_close):
                # Market data unchanged - reuse previous ATR (market likely closed)
                print(f"Market data unchanged for {symbol} - reusing previous ATR: {prior_atr:.2f}")
                current_tr = max(current_high - current_low, 
                               abs(current_high - prev_close), 
                               abs(current_low - prev_close))
                return current_tr, prior_atr

        # Store current market data for next comparison
        if symbol:
            self.last_market_data[symbol] = {
                'high': current_high,
                'low': current_low,
                'prev_close': prev_close
            }

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

        return current_tr, current_atr

    def populate_positions_table(self):
        self.table.setRowCount(len(self.positions))
        for i, pos in enumerate(self.positions):
            symbol = self.symbols[i]
            
            # Column 0: Position
            self.table.setItem(i, 0, QTableWidgetItem(pos))
            
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
            spin.valueChanged.connect(lambda val, row=i: self.update_atr_ratio(row, val))
            self.table.setCellWidget(i, 2, spin)

            # Column 3: Status
            self.table.setItem(i, 3, QTableWidgetItem(self.statuses[i]))
            
            # Column 4: Positions Held
            self.table.setItem(i, 4, QTableWidgetItem(str(self.positions_held[i])))
            
            # Column 5: Current Price
            self.table.setItem(i, 5, QTableWidgetItem(f"{self.current_prices[i]:.2f}"))

            # Column 6: Computed Stop Loss = Current Price - (ATR × ATR Ratio)
            # Stop loss never goes down - use highest computed value
            computed_stop = self.compute_stop_loss(symbol, self.current_prices[i], atr_value, self.atr_ratios[i])
            stop_item = QTableWidgetItem(f"{computed_stop:.2f}" if computed_stop else "N/A")
            self.table.setItem(i, 6, stop_item)
    
    def compute_stop_loss(self, symbol, current_price, atr_value, atr_ratio):
        """
        Compute stop loss = Current Price - (ATR × ATR Ratio)
        Stop loss never goes down - track highest value per symbol
        """
        if atr_value is None or current_price <= 0:
            return self.highest_stop_losses.get(symbol, 0)
        
        # Calculate new stop loss
        new_stop = current_price - (atr_value * atr_ratio)
        
        # Get previous highest stop loss for this symbol
        prev_highest = self.highest_stop_losses.get(symbol, 0)
        
        # Stop loss never goes down - use max of new and previous
        if new_stop > prev_highest:
            self.highest_stop_losses[symbol] = new_stop
            return new_stop
        else:
            return prev_highest

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
            tr_item = QTableWidgetItem(f"{self.tr_values[i]:.2f}")
            tr_item.setFlags(tr_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.atr_table.setItem(i, 2, tr_item)

            # ATR (editable)
            atr_item = QTableWidgetItem(f"{self.atr_calculated[i]:.2f}" if self.atr_calculated[i] is not None else "N/A")
            self.atr_table.setItem(i, 3, atr_item)

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
                contract = Contract(conId=contract_info['conId'])
                ib.qualifyContracts(contract)
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
            # Submit stop loss orders immediately when enabled
            self.submit_adaptive_stop_losses()
        else:
            self.adaptive_stop_toggle.setText("OFF")
        status_text = "ENABLED" if self.send_adaptive_stops else "DISABLED"
        logging.info(f"Adaptive Stop Losses: {status_text}")
    
    def submit_adaptive_stop_losses(self):
        """Submit stop loss orders for all positions based on computed stop losses"""
        if not self.send_adaptive_stops:
            print("Adaptive stop losses are disabled, skipping order submission")
            return
        
        # Build stop loss data for each symbol
        stop_loss_data = {}
        
        for i, symbol in enumerate(self.symbols):
            # Get ATR value
            atr_value = None
            if symbol in self.atr_symbols:
                atr_index = self.atr_symbols.index(symbol)
                if self.atr_calculated[atr_index] is not None:
                    atr_value = self.atr_calculated[atr_index]
            
            # Get computed stop loss (using the same logic as the table)
            stop_price = self.compute_stop_loss(
                symbol,
                self.current_prices[i],
                atr_value,
                self.atr_ratios[i]
            )
            
            # Get contract details
            contract_details = self.contract_details_map.get(symbol, {})
            
            # Skip if no valid stop price
            if stop_price <= 0:
                print(f"Skipping {symbol}: No valid stop price computed")
                continue
            
            # Skip stocks/ETFs if desired (uncomment to enable)
            # if contract_details.get('secType') == 'STK':
            #     print(f"Skipping {symbol}: Stock/ETF")
            #     continue
            
            stop_loss_data[symbol] = {
                'stop_price': stop_price,
                'quantity': self.positions_held[i],
                'contract_details': contract_details
            }
            
            logging.info(f"Preparing stop loss for {symbol}: {self.positions_held[i]} @ {stop_price:.2f}")
        
        if not stop_loss_data:
            print("No valid stop loss orders to submit")
            return
        
        # Submit the stop loss orders
        print(f"\nSubmitting {len(stop_loss_data)} stop loss order(s)...")
        results, success = submit_stop_loss_orders(stop_loss_data)
        
        # Update status based on results
        for result in results:
            symbol = result.get('symbol', 'Unknown')
            status = result.get('status', 'unknown')
            
            # Find the row for this symbol and update status
            if symbol in self.symbols:
                idx = self.symbols.index(symbol)
                if status == 'submitted':
                    self.statuses[idx] = f"Stop @ {result.get('stop_price', 0):.2f}"
                elif status == 'error':
                    self.statuses[idx] = f"Error: {result.get('message', 'Unknown')}"
                elif status == 'skipped':
                    self.statuses[idx] = f"Skipped: {result.get('message', '')}"
        
        # Refresh the table to show updated statuses
        self.populate_positions_table()
        
        if success:
            logging.info("Stop loss orders submitted successfully")
        else:
            logging.warning("Some stop loss orders may have failed")

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

    def calculate_atr_for_symbols(self, symbols):
        """Calculate ATR for each symbol in the list"""
        self.atr_symbols.clear()
        self.tr_values.clear()
        self.atr_calculated.clear()
        self.previous_atr_values.clear()
        
        ib = IB()
        try:
            ib.connect('127.0.0.1', 7497, clientId=random.randint(100, 999))
            
            for symbol in symbols:
                try:
                    # Get previous ATR from history
                    previous_atr = self.get_previous_atr(symbol)
                    
                    # Get contract details from position data if available
                    contract_info = self.contract_details_map.get(symbol, {})
                    sec_type = contract_info.get('secType', 'FUT')
                    
                    # Skip stocks/ETFs for ATR calculation (they use different method)
                    if sec_type == 'STK':
                        print(f"Skipping {symbol} - Stock/ETF (ATR calculation not applicable)")
                        continue
                    
                    # Create contract using actual details from position
                    if contract_info.get('conId'):
                        # Use conId for most accurate contract identification
                        contract = Contract(conId=contract_info['conId'])
                        ib.qualifyContracts(contract)
                        logging.info(f"Using conId {contract_info['conId']} for {symbol}")
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
                    
                    # Check if ATR already exists for this completed interval
                    if symbol in self.atr_history and completed_interval_key in self.atr_history[symbol]:
                        # ATR already calculated for this completed interval - use existing value
                        current_atr = self.atr_history[symbol][completed_interval_key]
                        logging.info(f"Symbol: {symbol}, ATR already calculated for completed interval {completed_interval_key}: {current_atr:.2f} (skipping calculation)")
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
                    
                    # Store the values
                    self.atr_symbols.append(symbol)
                    self.tr_values.append(current_tr)
                    self.atr_calculated.append(current_atr)
                    self.previous_atr_values.append(previous_atr)
                
                except Exception as e:
                    print(f"Error calculating ATR for {symbol}: {e}")
                    continue
        
        except Exception as e:
            print(f"Error connecting to IBKR for ATR calculations: {e}")
        finally:
            if ib.isConnected():
                ib.disconnect()
        
        # Update the ATR table
        self.populate_atr_table()
        
        # Update the Positions table to show the new ATR values
        self.populate_positions_table()

    def fetch_ibkr_data(self):
        try:
            positions_data, connection_success = fetch_positions()
            
            # Update connection status
            self.update_status(connection_success)
            
            if connection_success:
                # Update timestamp on successful connection
                self.update_timestamp()
            
            if not positions_data:
                self.raw_data_view.setPlainText("No positions returned from IBKR")
                return

            # Clear old data (but preserve highest_stop_losses - they never reset)
            self.positions.clear()
            self.atr_values.clear()
            self.atr_ratios.clear()
            self.statuses.clear()
            self.symbols.clear()
            self.positions_held.clear()
            self.current_prices.clear()
            self.computed_stop_losses.clear()

            display_text = ""
            self.contract_details_map.clear()
            for p in positions_data:
                self.positions.append(p['position'])
                self.atr_values.append(0)
                self.atr_ratios.append(2.5)  # Default ATR Ratio is now 2.5
                self.statuses.append("Up to date")
                self.symbols.append(p['symbol'])
                self.positions_held.append(p['positions_held'])
                self.current_prices.append(p['current_price'])
                self.computed_stop_losses.append(0)
                # Store contract details for ATR calculations
                if 'contract_details' in p:
                    self.contract_details_map[p['symbol']] = p['contract_details']
                # Build raw line for display
                raw_line = f"{p['symbol']} | Qty: {p['positions_held']} | Avg Cost: ${p['avg_cost']:.2f} | Price: ${p['current_price']:.2f} | P/L: ${p.get('unrealized_pl', 0):.2f} ({p.get('pl_percent', 0):.2f}%)"
                display_text += raw_line + "\n"

            self.raw_data_view.setPlainText(display_text)
            self.populate_positions_table()
            
            # Calculate ATR for all symbols if we have positions
            if self.symbols:
                self.calculate_atr_for_symbols(self.symbols)
            
            # Submit stop loss orders if adaptive stops are enabled
            if self.send_adaptive_stops:
                self.submit_adaptive_stop_losses()

        except Exception as e:
            self.raw_data_view.setPlainText(f"Error fetching data: {e}")
            self.update_status(False)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ATRWindow()
    window.show()
    sys.exit(app.exec())









#_______________________________________________________


#WatchDog




import os
import sys
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess

class ReloadHandler(FileSystemEventHandler):
    def __init__(self, script_path):
        self.script_path = script_path
        self.process = None
        self.start_process()

    def start_process(self):
        """Start the main.py process"""
        if self.process:
            self.process.terminate()
            self.process.wait()
        print(f"Starting {self.script_path}...")
        self.process = subprocess.Popen([sys.executable, self.script_path])

    def on_modified(self, event):
        """Restart process if a Python file is modified"""
        if event.src_path.endswith(".py"):
            print(f"{event.src_path} changed — restarting...")
            self.start_process()

if __name__ == "__main__":
    project_root = os.path.dirname(os.path.abspath(__file__))  # watch current folder
    main_script = os.path.join(project_root, "main.py")

    event_handler = ReloadHandler(main_script)
    observer = Observer()
    observer.schedule(event_handler, path=project_root, recursive=True)
    observer.start()

    print(f"Watching all Python files in {project_root}...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping WatchDog...")
        observer.stop()
        if event_handler.process:
            event_handler.process.terminate()
    observer.join()
