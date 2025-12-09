# main.py
import sys
import random
import json
import os
from datetime import datetime, timedelta
from PyQt6 import QtGui
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QTableWidget, QTableWidgetItem, QVBoxLayout,
    QWidget, QDoubleSpinBox, QTabWidget, QTextEdit, QPushButton, QHeaderView,
    QLabel, QHBoxLayout, QCheckBox
)
from PyQt6.QtCore import Qt, QTimer
from ibkr_api import fetch_positions  # Import our separate IBKR module
from atr_test import parse_ibkr_position, calculate_tr_and_atr
from ib_insync import IB, Future
import pandas as pd


class ATRWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("ATR Adaptive Stop Bot")
        self.setGeometry(100, 100, 1200, 800)

        # Placeholder data lists
        self.positions = []
        self.market_prices = []
        self.pl_values = []
        self.atr_values = []
        self.atr_ratios = []
        self.statuses = []
        self.symbols = []
        self.positions_held = []
        self.avg_costs = []
        self.current_prices = []
        self.monthly_pl_percent = []
        
        # ATR calculation data
        self.atr_symbols = []
        self.tr_values = []
        self.atr_calculated = []
        self.previous_atr_values = []
        self.previous_atr_sources = []  # Track if value is "Calculated" or "User Inputted"
        
        # ATR history file path
        self.atr_history_file = os.path.join(os.path.dirname(__file__), 'atr_history.json')
        
        # Load ATR history
        self.atr_history = self.load_atr_history()
        
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
        toggle_layout = QVBoxLayout()
        toggle_container.setLayout(toggle_layout)
        toggle_layout.setContentsMargins(0, 0, 0, 0)
        
        toggle_label = QLabel("Send Adaptive Stop Losses")
        toggle_label.setStyleSheet("font-weight: bold; font-size: 11px;")
        toggle_layout.addWidget(toggle_label)
        
        self.adaptive_stop_toggle = QCheckBox()
        self.adaptive_stop_toggle.setChecked(False)
        self.adaptive_stop_toggle.stateChanged.connect(self.on_adaptive_stop_toggled)
        # Style the checkbox as a toggle switch
        self.adaptive_stop_toggle.setStyleSheet("""
            QCheckBox {
                spacing: 0px;
            }
            QCheckBox::indicator {
                width: 50px;
                height: 25px;
                border-radius: 12px;
                background-color: #d32f2f;
                border: 2px solid #b71c1c;
            }
            QCheckBox::indicator:checked {
                background-color: #4caf50;
                border: 2px solid #388e3c;
            }
            QCheckBox::indicator:hover {
                border: 2px solid #555;
            }
        """)
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
        self.table.setColumnCount(9)
        self.table.setHorizontalHeaderLabels([
            "Position", "Market Price", "ATR", "ATR Ratio", "Status",
            "Positions Held", "Avg Cost", "Current Price", "Monthly P/L %"
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
        self.atr_table.cellChanged.connect(self.on_previous_atr_changed)
        self.atr_calc_layout.addWidget(self.atr_table)

        # Setup auto-refresh timer (60 seconds = 60000 ms)
        self.refresh_timer = QTimer(self)
        self.refresh_timer.timeout.connect(self.fetch_ibkr_data)
        self.refresh_timer.start(60000)  # Refresh every 60 seconds
        
        # Fetch data immediately on startup
        self.fetch_ibkr_data()

    def populate_positions_table(self):
        self.table.setRowCount(len(self.positions))
        for i, pos in enumerate(self.positions):
            self.table.setItem(i, 0, QTableWidgetItem(pos))
            self.table.setItem(i, 1, QTableWidgetItem(str(self.market_prices[i])))
            
            # Get ATR value from ATR calculations tab
            symbol = self.symbols[i]
            atr_display = "N/A"
            if symbol in self.atr_symbols:
                atr_index = self.atr_symbols.index(symbol)
                if self.atr_calculated[atr_index] is not None:
                    atr_display = f"{self.atr_calculated[atr_index]:.2f}"
            self.table.setItem(i, 2, QTableWidgetItem(atr_display))

            # ATR Ratio editable spin box
            spin = QDoubleSpinBox()
            spin.setMinimum(0.0)
            spin.setValue(self.atr_ratios[i])
            spin.valueChanged.connect(lambda val, row=i: self.update_atr_ratio(row, val))
            self.table.setCellWidget(i, 3, spin)

            self.table.setItem(i, 4, QTableWidgetItem(self.statuses[i]))
            self.table.setItem(i, 5, QTableWidgetItem(str(self.positions_held[i])))
            self.table.setItem(i, 6, QTableWidgetItem(f"{self.avg_costs[i]:.2f}"))
            self.table.setItem(i, 7, QTableWidgetItem(f"{self.current_prices[i]:.2f}"))

            pl_item = QTableWidgetItem(f"{self.monthly_pl_percent[i]:.2f}%")
            if self.monthly_pl_percent[i] >= 0:
                pl_item.setForeground(QtGui.QColor("green"))
            else:
                pl_item.setForeground(QtGui.QColor("red"))
            self.table.setItem(i, 8, pl_item)

    def populate_atr_table(self):
        """Populate the ATR Calculations table"""
        # Temporarily disconnect the cellChanged signal to avoid triggering during population
        self.atr_table.cellChanged.disconnect(self.on_previous_atr_changed)
        
        self.atr_table.setRowCount(len(self.atr_symbols))
        for i in range(len(self.atr_symbols)):
            # Symbol (read-only)
            symbol_item = QTableWidgetItem(self.atr_symbols[i])
            symbol_item.setFlags(symbol_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.atr_table.setItem(i, 0, symbol_item)
            
            # Previous ATR with source indicator
            prev_atr = self.previous_atr_values[i]
            source = self.previous_atr_sources[i] if i < len(self.previous_atr_sources) else None
            
            if prev_atr is None:
                prev_atr_item = QTableWidgetItem("Enter value")
                prev_atr_item.setBackground(QtGui.QColor(255, 200, 200))  # Light red
            else:
                # Create cell with value and source on separate lines
                text = f"{prev_atr:.2f}\n({source})" if source else f"{prev_atr:.2f}"
                prev_atr_item = QTableWidgetItem(text)
                # Make the second line gray using font
                font = prev_atr_item.font()
                font.setPointSize(8)  # Smaller font for the whole cell
            
            self.atr_table.setItem(i, 1, prev_atr_item)
            
            # TR (read-only)
            tr_item = QTableWidgetItem(f"{self.tr_values[i]:.2f}")
            tr_item.setFlags(tr_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.atr_table.setItem(i, 2, tr_item)
            
            # ATR (read-only)
            atr_item = QTableWidgetItem(f"{self.atr_calculated[i]:.2f}" if self.atr_calculated[i] is not None else "N/A")
            atr_item.setFlags(atr_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.atr_table.setItem(i, 3, atr_item)
        
        # Reconnect the signal
        self.atr_table.cellChanged.connect(self.on_previous_atr_changed)

    def load_atr_history(self):
        """Load ATR history from JSON file"""
        if os.path.exists(self.atr_history_file):
            try:
                with open(self.atr_history_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading ATR history: {e}")
                return {}
        return {}
    
    def save_atr_history(self):
        """Save ATR history to JSON file"""
        try:
            with open(self.atr_history_file, 'w') as f:
                json.dump(self.atr_history, f, indent=2)
            print("ATR history saved successfully")
        except Exception as e:
            print(f"Error saving ATR history: {e}")
    
    def get_previous_atr(self, symbol):
        """Get yesterday's ATR for a symbol, or None if not available"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        if symbol in self.atr_history:
            if yesterday in self.atr_history[symbol]:
                return self.atr_history[symbol][yesterday]
        return None
    
    def save_today_atr(self, symbol, atr_value):
        """Save today's ATR for a symbol (overwrites any existing value for today)"""
        today = datetime.now().strftime('%Y-%m-%d')
        if symbol not in self.atr_history:
            self.atr_history[symbol] = {}
        
        # Check if we're overwriting an existing value
        if today in self.atr_history[symbol]:
            old_value = self.atr_history[symbol][today]
            print(f"Overwriting existing ATR for {symbol} on {today}: {old_value:.2f} -> {atr_value:.2f}")
        else:
            print(f"Saving new ATR for {symbol} on {today}: {atr_value:.2f}")
        
        # Save/overwrite today's value
        self.atr_history[symbol][today] = atr_value
        self.save_atr_history()

    def on_previous_atr_changed(self, row, column):
        """Handle when user edits the Previous ATR column"""
        if column == 1:  # Previous ATR column
            try:
                # Extract just the numeric value (ignore the source text if present)
                cell_text = self.atr_table.item(row, column).text()
                # Get first line only (the numeric value)
                numeric_value = cell_text.split('\n')[0].strip()
                new_value = float(numeric_value)
                symbol = self.atr_table.item(row, 0).text()
                
                # Update the previous_atr_values list and mark as user inputted
                self.previous_atr_values[row] = new_value
                self.previous_atr_sources[row] = "User Inputted"
                
                # Remove red background and update cell with source
                self.atr_table.cellChanged.disconnect(self.on_previous_atr_changed)
                new_text = f"{new_value:.2f}\n(User Inputted)"
                self.atr_table.item(row, column).setText(new_text)
                # Clear background color to use default (transparent)
                self.atr_table.item(row, column).setBackground(QtGui.QBrush())
                self.atr_table.cellChanged.connect(self.on_previous_atr_changed)
                
                # Save to history as yesterday's value so it can be used
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                if symbol not in self.atr_history:
                    self.atr_history[symbol] = {}
                self.atr_history[symbol][yesterday] = new_value
                self.save_atr_history()
                
                # Recalculate ATR for this symbol
                print(f"Recalculating ATR for {symbol} with user-inputted previous ATR: {new_value}")
                self.recalculate_single_symbol_atr(row, symbol, new_value)
                
            except ValueError:
                print("Invalid value for Previous ATR. Please enter a number.")
                self.atr_table.item(row, column).setBackground(QtGui.QColor(255, 200, 200))
    
    def recalculate_single_symbol_atr(self, row, symbol, prior_atr):
        """Recalculate ATR for a single symbol with the given prior ATR"""
        ib = IB()
        try:
            ib.connect('127.0.0.1', 7497, clientId=random.randint(100, 999))
            
            contract = Future(symbol=symbol, lastTradeDateOrContractMonth='20251219', exchange='CME', currency='USD')
            
            bars = ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr='14 D',
                barSizeSetting='1 day',
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
                
                tr, atr = calculate_tr_and_atr(df, prior_atr=prior_atr)
                
                if tr is not None and atr is not None:
                    # Update the values
                    self.tr_values[row] = tr
                    self.atr_calculated[row] = atr
                    
                    # Update table cells (disconnect signal first)
                    self.atr_table.cellChanged.disconnect(self.on_previous_atr_changed)
                    self.atr_table.item(row, 2).setText(f"{tr:.2f}")
                    self.atr_table.item(row, 3).setText(f"{atr:.2f}")
                    self.atr_table.cellChanged.connect(self.on_previous_atr_changed)
                    
                    # Save today's ATR
                    self.save_today_atr(symbol, atr)
                    print(f"Updated: Symbol: {symbol}, TR: {tr:.2f}, ATR: {atr:.2f}")
                    
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
        status_text = "ENABLED" if self.send_adaptive_stops else "DISABLED"
        print(f"Adaptive Stop Losses: {status_text}")

    def update_atr_ratio(self, row, value):
        self.atr_ratios[row] = value

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
        self.previous_atr_sources.clear()
        
        ib = IB()
        try:
            ib.connect('127.0.0.1', 7497, clientId=random.randint(100, 999))
            
            for symbol in symbols:
                try:
                    # Get previous ATR from history
                    previous_atr = self.get_previous_atr(symbol)
                    
                    # Define contract
                    contract = Future(symbol=symbol, lastTradeDateOrContractMonth='20251219', exchange='CME', currency='USD')
                    
                    # Get historical bars (14 days for ATR calculation)
                    bars = ib.reqHistoricalData(
                        contract,
                        endDateTime='',
                        durationStr='14 D',
                        barSizeSetting='1 day',
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
                    
                    # Determine source and calculate ATR
                    if previous_atr is not None:
                        current_atr = (previous_atr * 13 + current_tr) / 14
                        source = "Calculated"  # From history, previously calculated
                        print(f"Symbol: {symbol}, Previous ATR: {previous_atr:.2f}, TR: {current_tr:.2f}, ATR: {current_atr:.2f}")
                        
                        # Save today's ATR
                        self.save_today_atr(symbol, current_atr)
                    else:
                        current_atr = None
                        source = None  # No previous value
                        print(f"Symbol: {symbol}, TR: {current_tr:.2f}, ATR: N/A (No previous ATR - please enter value)")
                    
                    # Store the values
                    self.atr_symbols.append(symbol)
                    self.tr_values.append(current_tr)
                    self.atr_calculated.append(current_atr)
                    self.previous_atr_values.append(previous_atr)
                    self.previous_atr_sources.append(source)
                
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

            # Clear old data
            self.positions.clear()
            self.market_prices.clear()
            self.pl_values.clear()
            self.atr_values.clear()
            self.atr_ratios.clear()
            self.statuses.clear()
            self.symbols.clear()
            self.positions_held.clear()
            self.avg_costs.clear()
            self.current_prices.clear()
            self.monthly_pl_percent.clear()

            display_text = ""
            for p in positions_data:
                self.positions.append(p['position'])
                self.market_prices.append(0)
                self.pl_values.append(0)
                self.atr_values.append(0)
                self.atr_ratios.append(1.0)
                self.statuses.append("Up to date")
                self.symbols.append(p['symbol'])
                self.positions_held.append(p['positions_held'])
                self.avg_costs.append(p['avg_cost'])
                self.current_prices.append(p['current_price'])
                self.monthly_pl_percent.append(p['monthly_pl_percent'])
                display_text += p['raw_line'] + "\n"

            self.raw_data_view.setPlainText(display_text)
            self.populate_positions_table()
            
            # Calculate ATR for all symbols if we have positions
            if self.symbols:
                self.calculate_atr_for_symbols(self.symbols)

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
