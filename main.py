# main.py
import sys
from PyQt6 import QtGui
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QTableWidget, QTableWidgetItem, QVBoxLayout,
    QWidget, QDoubleSpinBox, QTabWidget, QTextEdit, QPushButton, QHeaderView
)
from ibkr_api import fetch_positions  # Import our separate IBKR module
from atr_test import parse_ibkr_position, calculate_tr_and_atr


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

        # Central widget & layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout()
        central_widget.setLayout(layout)

        # Tabs
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        # --- Positions Table Tab ---
        self.positions_tab = QWidget()
        self.positions_layout = QVBoxLayout()
        self.positions_tab.setLayout(self.positions_layout)
        self.tabs.addTab(self.positions_tab, "Positions")

        self.table = QTableWidget()
        self.table.setColumnCount(11)
        self.table.setHorizontalHeaderLabels([
            "Position", "Market Price", "P/L", "ATR", "ATR Ratio", "Status",
            "Symbol", "Positions Held", "Avg Cost", "Current Price", "Monthly P/L %"
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

        self.refresh_button = QPushButton("Refresh IBKR Data")
        self.refresh_button.clicked.connect(self.fetch_ibkr_data)
        self.raw_layout.addWidget(self.refresh_button)

    def populate_positions_table(self):
        self.table.setRowCount(len(self.positions))
        for i, pos in enumerate(self.positions):
            self.table.setItem(i, 0, QTableWidgetItem(pos))
            self.table.setItem(i, 1, QTableWidgetItem(str(self.market_prices[i])))
            self.table.setItem(i, 2, QTableWidgetItem(str(self.pl_values[i])))
            self.table.setItem(i, 3, QTableWidgetItem(str(self.atr_values[i])))

            # ATR Ratio editable spin box
            spin = QDoubleSpinBox()
            spin.setMinimum(0.0)
            spin.setValue(self.atr_ratios[i])
            spin.valueChanged.connect(lambda val, row=i: self.update_atr_ratio(row, val))
            self.table.setCellWidget(i, 4, spin)

            self.table.setItem(i, 5, QTableWidgetItem(self.statuses[i]))
            self.table.setItem(i, 6, QTableWidgetItem(self.symbols[i]))
            self.table.setItem(i, 7, QTableWidgetItem(str(self.positions_held[i])))
            self.table.setItem(i, 8, QTableWidgetItem(str(self.avg_costs[i])))
            self.table.setItem(i, 9, QTableWidgetItem(f"{self.current_prices[i]:.2f}"))

            pl_item = QTableWidgetItem(f"{self.monthly_pl_percent[i]:.2f}%")
            if self.monthly_pl_percent[i] >= 0:
                pl_item.setForeground(QtGui.QColor("green"))
            else:
                pl_item.setForeground(QtGui.QColor("red"))
            self.table.setItem(i, 10, pl_item)

    def update_atr_ratio(self, row, value):
        self.atr_ratios[row] = value

    def fetch_ibkr_data(self):
        try:
            positions_data = fetch_positions()
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

        except Exception as e:
            self.raw_data_view.setPlainText(f"Error fetching data: {e}")


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