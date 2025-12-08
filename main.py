#GUI

import sys
from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QDoubleSpinBox,
    QTabWidget,
    QTextEdit,
    QPushButton
)
from ib_insync import IB

class ATRWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("ATR Adaptive Stop Bot")
        self.setGeometry(100, 100, 800, 800)

        # Placeholder data
        self.positions = ["AAA", "BBB", "CCC", "DDD"]
        self.market_prices = [0, 0, 0, 0]
        self.pl_values = [0, 0, 0, 0]
        self.atr_values = [0, 0, 0, 0]
        self.atr_ratios = [1.0, 1.0, 1.0, 1.0]
        self.statuses = ["Up to date"] * 4

        # IBKR connection
        self.ib = IB()

        # Central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout()
        central_widget.setLayout(layout)

        # Tabs
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        # Tab 1: Positions
        self.positions_tab = QWidget()
        self.positions_layout = QVBoxLayout()
        self.positions_tab.setLayout(self.positions_layout)
        self.tabs.addTab(self.positions_tab, "Positions")

        self.table = QTableWidget()
        self.table.setRowCount(len(self.positions))
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(
            ["Position", "Market Price", "P/L", "ATR", "ATR Ratio", "Status"]
        )
        self.positions_layout.addWidget(self.table)
        self.populate_positions_table()

        # Tab 2: Raw Data
        self.raw_tab = QWidget()
        self.raw_layout = QVBoxLayout()
        self.raw_tab.setLayout(self.raw_layout)
        self.tabs.addTab(self.raw_tab, "Raw Data")

        self.raw_data_view = QTextEdit()
        self.raw_data_view.setReadOnly(True)
        self.raw_data_view.setPlaceholderText("IB API raw data will appear here...")
        self.raw_layout.addWidget(self.raw_data_view)

        # Refresh button to fetch IBKR positions
        self.refresh_button = QPushButton("Refresh IBKR Data")
        self.refresh_button.clicked.connect(self.fetch_ibkr_data)
        self.raw_layout.addWidget(self.refresh_button)

    # -------------------------------
    # Populate Positions Table
    # -------------------------------
    def populate_positions_table(self):
        for i, pos in enumerate(self.positions):
            self.table.setItem(i, 0, QTableWidgetItem(pos))
            self.table.setItem(i, 1, QTableWidgetItem(str(self.market_prices[i])))
            self.table.setItem(i, 2, QTableWidgetItem(str(self.pl_values[i])))
            self.table.setItem(i, 3, QTableWidgetItem(str(self.atr_values[i])))

            spin = QDoubleSpinBox()
            spin.setMinimum(0.0)
            spin.setValue(self.atr_ratios[i])
            spin.valueChanged.connect(lambda val, row=i: self.update_atr_ratio(row, val))
            self.table.setCellWidget(i, 4, spin)

            self.table.setItem(i, 5, QTableWidgetItem(self.statuses[i]))

    # -------------------------------
    # Update ATR Ratio
    # -------------------------------
    def update_atr_ratio(self, row, value):
        self.atr_ratios[row] = value

    # -------------------------------
    # Fetch IBKR Data
    # -------------------------------
    def fetch_ibkr_data(self):
        try:
            self.ib.connect('127.0.0.1', 7497, clientId=1)  # Adjust port/clientId if needed
            positions = self.ib.positions()
            display_text = ""
            for pos in positions:
                display_text += f"{pos.account}: {pos.contract.symbol} | {pos.position} | Avg Cost: {pos.avgCost}\n"
            self.raw_data_view.setPlainText(display_text)
            self.ib.disconnect()
        except Exception as e:
            self.raw_data_view.setPlainText(f"Error connecting to IBKR: {e}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ATRWindow()
    window.show()
    sys.exit(app.exec())

#_______________________________________________________