# atr_processor.py
import asyncio
import logging
from datetime import datetime, timedelta
from ib_insync import IB, util, Contract
import json

class ATRProcessor:
    """
    Handles fetching historical data and calculating TR and ATR for symbols concurrently.
    This class is designed to be run in a background thread and is UI-agnostic.
    """
    def __init__(self, atr_history_file, file_lock):
        self.atr_history_file = atr_history_file
        self.file_lock = file_lock
        self.atr_history = self._load_atr_history()

    def _load_atr_history(self):
        """Loads ATR history from the JSON file in a thread-safe manner."""
        with self.file_lock:
            try:
                with open(self.atr_history_file, 'r') as f:
                    return json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                return {}

    def _save_atr_history(self):
        """Saves the current ATR history to the JSON file in a thread-safe manner."""
        with self.file_lock:
            try:
                with open(self.atr_history_file, 'w') as f:
                    json.dump(self.atr_history, f, indent=2)
            except IOError as e:
                logging.error(f"Error saving ATR history: {e}")

    def _cleanup_history(self, current_symbols: list[str]):
        """
        Removes symbols no longer in the portfolio and ATR data older than 4 days.
        """
        cutoff_date = datetime.now() - timedelta(days=4)
        history_changed = False

        # Use a list of keys to iterate, allowing safe deletion from the dictionary
        for symbol in list(self.atr_history.keys()):
            # 1. Remove symbol if it's no longer in the portfolio
            if symbol not in current_symbols:
                del self.atr_history[symbol]
                logging.info(f"ATR History Cleanup: Removed symbol '{symbol}' as it is no longer in the portfolio.")
                history_changed = True
                continue

            # 2. Remove timestamps older than the cutoff
            timestamps_to_remove = []
            for timestamp_str in self.atr_history[symbol]:
                try:
                    timestamp_dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M')
                    if timestamp_dt < cutoff_date:
                        timestamps_to_remove.append(timestamp_str)
                except ValueError:
                    # If format is invalid, mark for removal
                    timestamps_to_remove.append(timestamp_str)
            
            for ts in timestamps_to_remove:
                del self.atr_history[symbol][ts]
                logging.info(f"ATR History Cleanup: Removed old entry for {symbol} at {ts}.")
                history_changed = True

    def _get_interval_key(self, dt: datetime) -> str:
        """Generates a 15-minute interval key (e.g., '2023-10-27 14:30') from a datetime object."""
        minute = (dt.minute // 15) * 15
        interval_time = dt.replace(minute=minute, second=0, microsecond=0)
        return interval_time.strftime('%Y-%m-%d %H:%M')

    def _get_previous_atr(self, symbol: str) -> float:
        """
        Gets the ATR value for a symbol. It first tries the immediately preceding 15-minute interval.
        If that's not available or is zero, it falls back to the most recent ATR value in history.
        """
        now = datetime.now()
        current_interval_dt = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
        previous_interval_dt = current_interval_dt - timedelta(minutes=15)
        previous_interval_key = self._get_interval_key(previous_interval_dt)

        # 1. Try to get ATR from the immediately preceding interval
        previous_atr = self.atr_history.get(symbol, {}).get(previous_interval_key, 0.0)

        # 2. If not found or is zero, find the last available ATR in history
        if previous_atr == 0.0:
            symbol_history = self.atr_history.get(symbol, {})
            if symbol_history:
                # Find the most recent timestamp key that is before the current interval
                current_interval_key = self._get_interval_key(now)
                # The keys are strings like 'YYYY-MM-DD HH:MM', so a reverse-sorted list gives us the latest first.
                for timestamp_key in sorted(symbol_history.keys(), reverse=True):
                    if timestamp_key < current_interval_key:
                        return symbol_history[timestamp_key]

        return previous_atr

    def _calculate_tr(self, df):
        """Calculates True Range from a dataframe of historical bars."""
        if len(df) < 2:
            return 0.0

        prev_close = df['close'].iloc[-2]
        current_high = df['high'].iloc[-1]
        current_low = df['low'].iloc[-1]

        tr1 = current_high - current_low
        tr2 = abs(current_high - prev_close)
        tr3 = abs(current_low - prev_close)
        return max(tr1, tr2, tr3)

    async def _process_symbol(self, ib: IB, symbol: str, contract_details: dict):
        """Fetches data and calculates TR/ATR for a single symbol."""
        try:
            contract = Contract(conId=contract_details['conId'], exchange=contract_details.get('exchange', ''))
            await ib.qualifyContractsAsync(contract)

            # Fetch 1 day of 15-min bars. This is enough to get the last two candles.
            bars = await ib.reqHistoricalDataAsync(
                contract, endDateTime='', durationStr='1 D', barSizeSetting='15 mins',
                whatToShow='TRADES', useRTH=False, formatDate=1
            )

            if not bars or len(bars) < 2:
                logging.warning(f"Not enough historical data for {symbol} to calculate TR.")
                return {'symbol': symbol, 'tr': 0.0, 'atr': None, 'previous_atr': 0.0}

            df = util.df(bars)
            current_tr = self._calculate_tr(df)
            previous_atr = self._get_previous_atr(symbol)

            # Wilder's smoothing formula
            current_atr = ((previous_atr * 13) + current_tr) / 14

            # Save the newly calculated ATR to history for the current interval
            current_interval_key = self._get_interval_key(datetime.now())
            if symbol not in self.atr_history:
                self.atr_history[symbol] = {}
            self.atr_history[symbol][current_interval_key] = current_atr

            logging.info(f"Processed {symbol}: TR={current_tr:.4f}, Prev ATR={previous_atr:.4f}, New ATR={current_atr:.4f} for interval {current_interval_key}")

            return {
                'symbol': symbol,
                'tr': current_tr,
                'atr': current_atr,
                'previous_atr': previous_atr
            }
        except Exception as e:
            logging.error(f"Error processing ATR for {symbol}: {e}")
            return {'symbol': symbol, 'tr': None, 'atr': None, 'previous_atr': None}

    async def run(self, ib: IB, enriched_positions: list) -> tuple[list, dict]:
        """
        Runs the ATR calculation for all positions concurrently.
        Returns a tuple of (results_list, updated_atr_history_dict).
        """
        current_symbols = [p['symbol'] for p in enriched_positions]

        tasks = [
            self._process_symbol(ib, symbol, p['contract_details'])
            for p in enriched_positions if (symbol := p['symbol'])
        ]
        results = await asyncio.gather(*tasks)

        self._cleanup_history(current_symbols)

        self._save_atr_history()  # Save all updates at the end of the run
        return results, self.atr_history