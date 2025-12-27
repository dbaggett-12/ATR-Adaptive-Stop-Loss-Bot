# atr_processor.py
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from ib_insync import IB, util, Contract
import json
import pandas as pd

class ATRProcessor:
    """
    Handles fetching historical data, calculating TR, and deriving ATR for symbols.
    ATR is calculated on-the-fly from a persisted history of True Ranges (TRs).
    This class is designed to be run in a background thread and is UI-agnostic.
    """
    def __init__(self, atr_state_file, atr_history_file, state_file_lock, history_file_lock):
        self.atr_state_file = atr_state_file
        self.atr_history_file = atr_history_file
        self.state_file_lock = state_file_lock
        self.history_file_lock = history_file_lock
        self.atr_state = self._load_atr_state()
        self.atr_history = self._load_atr_history()

    def _load_atr_state(self):
        """Loads ATR state (TR history and last ATR) from the JSON file."""
        with self.state_file_lock:
            try:
                with open(self.atr_state_file, 'r') as f:
                    return json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                return {}

    def _save_atr_state(self):
        """Saves the current ATR state to the JSON file."""
        with self.state_file_lock:
            try:
                with open(self.atr_state_file, 'w') as f:
                    json.dump(self.atr_state, f, indent=2)
            except IOError as e:
                logging.error(f"Error saving ATR state: {e}")

    def _load_atr_history(self):
        """Loads the ATR history for graphing from its JSON file."""
        with self.history_file_lock:
            try:
                with open(self.atr_history_file, 'r') as f:
                    return json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                return {}

    def _save_atr_history(self):
        """Saves the current ATR history to its JSON file."""
        with self.history_file_lock:
            try:
                with open(self.atr_history_file, 'w') as f:
                    json.dump(self.atr_history, f, indent=2)
            except IOError as e:
                logging.error(f"Error saving ATR history: {e}")

    def _cleanup_history(self, current_symbols: list[str], active_candle_sizes: dict):
        """
        Removes symbols no longer in the portfolio and old TR data from the state.
        """
        tr_cutoff_date = datetime.now() - timedelta(days=100)
        atr_cutoff_date = datetime.now() - timedelta(days=3)

        # Use a list of keys to iterate, allowing safe deletion from the dictionary
        for symbol in list(self.atr_state.keys()):
            # 1. Remove symbol if it's no longer in the portfolio
            if symbol not in current_symbols:
                del self.atr_state[symbol]
                logging.info(f"ATR State Cleanup: Removed symbol '{symbol}' as it is no longer in the portfolio.")
                continue

            symbol_state = self.atr_state.get(symbol)
            if not isinstance(symbol_state, dict):
                logging.warning(f"ATR State Cleanup: Invalid state for symbol '{symbol}' (expected dict, got {type(symbol_state)}). Clearing.")
                self.atr_state[symbol] = {}
                continue

            # Check for old format where keys are 'last_atr' and 'tr_history' directly under symbol
            if 'last_atr' in symbol_state or 'tr_history' in symbol_state:
                logging.warning(f"ATR State Cleanup: Detected old state format for '{symbol}' (found 'last_atr'/'tr_history'). Clearing to rebuild.")
                self.atr_state[symbol] = {}
                continue

            # Heuristic to detect old format: keys are timestamps (contain 'T') instead of candle sizes.
            is_old_format = any('T' in k for k in symbol_state.keys())
            if is_old_format:
                logging.warning(f"ATR State Cleanup: Detected old, incompatible state format for '{symbol}'. Clearing to rebuild.")
                self.atr_state[symbol] = {}
                continue

            # Enforce active candle size: Remove data for timeframes that don't match the current setting
            if symbol in active_candle_sizes:
                target_size = active_candle_sizes[symbol]
                for stored_size in list(self.atr_state[symbol].keys()):
                    if stored_size != target_size:
                        del self.atr_state[symbol][stored_size]
                        logging.info(f"ATR State Cleanup: Removed data for '{symbol}' with size '{stored_size}' (current setting: '{target_size}').")

            # The state for a symbol is now a dict of candle sizes
            for candle_size in list(self.atr_state[symbol].keys()):
                # 2. Remove timestamps older than the cutoff
                timestamps_to_remove = []
                symbol_candle_state = self.atr_state[symbol][candle_size]
                symbol_tr_history = symbol_candle_state.get('tr_history', {})
                if not isinstance(symbol_tr_history, dict): # Another sanity check
                    continue
                for timestamp_str in symbol_tr_history:
                    try:
                        # Timestamps are stored as ISO 8601 strings with timezone
                        timestamp_dt = datetime.fromisoformat(timestamp_str)
                        # Handle both aware and naive timestamps (Daily bars are often naive)
                        cutoff_cmp = tr_cutoff_date.astimezone() if timestamp_dt.tzinfo else tr_cutoff_date
                        if timestamp_dt < cutoff_cmp:
                            timestamps_to_remove.append(timestamp_str)
                    except (ValueError, TypeError):
                        # If format is invalid, mark for removal
                        timestamps_to_remove.append(timestamp_str)
                
                for ts in timestamps_to_remove:
                    del self.atr_state[symbol][candle_size]['tr_history'][ts]
                    logging.debug(f"TR History Cleanup: Removed old entry for {symbol} ({candle_size}) at {ts}.")

        # Also clean up the separate ATR history file
        for symbol in list(self.atr_history.keys()):
            if symbol not in current_symbols:
                del self.atr_history[symbol]
                logging.info(f"ATR History Cleanup: Removed symbol '{symbol}'.")
                continue
            
            # Check for old format where values are floats (ATR values) instead of dicts (candle buckets)
            if self.atr_history[symbol] and not isinstance(next(iter(self.atr_history[symbol].values())), dict):
                logging.warning(f"ATR History Cleanup: Detected old history format for '{symbol}'. Clearing.")
                self.atr_history[symbol] = {}
                continue

            # Enforce active candle size for history as well
            if symbol in active_candle_sizes:
                target_size = active_candle_sizes[symbol]
                for stored_size in list(self.atr_history[symbol].keys()):
                    if stored_size != target_size:
                        del self.atr_history[symbol][stored_size]
                        logging.info(f"ATR History Cleanup: Removed history for '{symbol}' with size '{stored_size}' (current setting: '{target_size}').")

            for candle_size in list(self.atr_history[symbol].keys()):
                timestamps_to_remove = []
                symbol_atr_candle_history = self.atr_history[symbol][candle_size]
                for timestamp_str in symbol_atr_candle_history:
                    try:
                        timestamp_dt = datetime.fromisoformat(timestamp_str)
                        cutoff_cmp = atr_cutoff_date.astimezone() if timestamp_dt.tzinfo else atr_cutoff_date
                        if timestamp_dt < cutoff_cmp:
                            timestamps_to_remove.append(timestamp_str)
                    except (ValueError, TypeError):
                        timestamps_to_remove.append(timestamp_str)
                
                for ts in timestamps_to_remove:
                    del self.atr_history[symbol][candle_size][ts]
                    logging.debug(f"ATR History Cleanup: Removed old ATR entry for {symbol} ({candle_size}) at {ts}.")
                
    def _calculate_true_ranges(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates True Range for each row in a dataframe of historical bars."""
        if df.empty or len(df) < 2:
            return pd.DataFrame()

        high = df['high']
        low = df['low']
        prev_close = df['close'].shift(1)

        # Calculate the three components of True Range
        tr1 = high - low
        tr2 = abs(high - prev_close)
        tr3 = abs(low - prev_close)

        # The True Range is the maximum of the three
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        # Use .assign() to create a new DataFrame with the 'tr' column, avoiding SettingWithCopyWarning
        df_with_tr = df.assign(tr=tr)
        return df_with_tr.dropna() # Drop the first row where TR is NaN

    async def _process_symbol(self, ib: IB, symbol: str, contract_details: dict, candle_size: str):
        """Fetches data and calculates TR/ATR for a single symbol."""
        try:
            contract = Contract(conId=contract_details['conId'], exchange=contract_details.get('exchange', ''))
            await ib.qualifyContractsAsync(contract)

            duration_map = {
                '15 mins': '2 D',
                '1 hour': '1 W',
                '1 day': '3 M'
            }
            durationStr = duration_map.get(candle_size, '3 M')  # Default to 3 M for safety

            logging.info(f"Requesting historical data for {symbol}: {durationStr} of {candle_size} bars.")
            bars = await ib.reqHistoricalDataAsync(
                contract, endDateTime='', durationStr=durationStr, barSizeSetting=candle_size,
                whatToShow='TRADES', useRTH=False, formatDate=1, keepUpToDate=False
            )

            if not bars or len(bars) < 2:
                logging.warning(f"Not enough historical data for {symbol} to calculate TR.")
                return {'symbol': symbol, 'tr': 0.0, 'atr': None, 'previous_atr': 0.0}

            df = util.df(bars)
            if df is None or df.empty:
                logging.warning(f"Could not create DataFrame for {symbol}.")
                return {'symbol': symbol, 'tr': 0.0, 'atr': None, 'previous_atr': 0.0}
            
            # Use all bars, including the last one (current interval), to allow live updates.
            df['date'] = pd.to_datetime(df['date'])
            
            if df.empty or len(df) < 2:
                logging.warning(f"Not enough bars for {symbol} to calculate TR.")
                return {'symbol': symbol, 'tr': 0.0, 'atr': None, 'previous_atr': 0.0}

            # Calculate TR for all historical bars
            tr_df = self._calculate_true_ranges(df)

            # --- State Management and Calculation ---
            # The state is now partitioned by symbol, then by candle_size.
            if symbol not in self.atr_state:
                self.atr_state[symbol] = {}
            if candle_size not in self.atr_state[symbol]:
                self.atr_state[symbol][candle_size] = {'last_atr': None, 'tr_history': {}}

            symbol_candle_state = self.atr_state[symbol][candle_size]
            tr_history = symbol_candle_state.get('tr_history', {})

            # Update history with new TRs, overwriting existing keys to ensure the current bar is live
            new_trs_added = 0
            for _, row in tr_df.iterrows():
                timestamp_key = row['date'].isoformat()
                if timestamp_key not in tr_history:
                    new_trs_added += 1
                tr_history[timestamp_key] = row['tr']
            
            if new_trs_added > 0:
                logging.info(f"Added {new_trs_added} new TR values to history for {symbol} ({candle_size}).")

            # --- ATR Calculation: Recalculate from History ---
            sorted_trs = sorted(tr_history.items()) # List of (timestamp_str, tr_value)
            
            current_tr = 0.0
            current_atr = None
            previous_atr = None
            
            if sorted_trs:
                current_ts, current_tr = sorted_trs[-1]
            
            # We track the ATR series to populate history
            atr_values = {}
            
            # 1. Try to calculate chain from the beginning
            # We need at least 15 bars to establish a Previous ATR (based on 14 prior bars)
            if len(sorted_trs) >= 15:
                # Initialize with SMA of the first 14 TRs
                initial_tr_sum = sum(item[1] for item in sorted_trs[:14])
                initial_atr = initial_tr_sum / 14
                
                initial_ts = sorted_trs[13][0]
                atr_values[initial_ts] = initial_atr
                
                prev_atr = initial_atr
                
                # Apply Wilder's Smoothing for subsequent bars, UP TO the one before current
                # We stop before the last one to explicitly identify Previous ATR
                for i in range(14, len(sorted_trs) - 1):
                    ts, tr_val = sorted_trs[i]
                    new_atr = (prev_atr * 13 + tr_val) / 14
                    atr_values[ts] = new_atr
                    prev_atr = new_atr
                
                # The result of the loop is the ATR for the bar immediately preceding the current one
                previous_atr = prev_atr

            # 2. Fallback: If Previous ATR is missing (e.g. not enough history for full chain),
            # use the simple average of the 14 candles BEFORE the current one.
            if previous_atr is None and len(sorted_trs) >= 15:
                # Slice indices -15 to -1 (excludes current at -1)
                fallback_slice = sorted_trs[-15:-1]
                previous_atr = sum(x[1] for x in fallback_slice) / 14
                logging.info(f"Using fallback SMA for Previous ATR for {symbol}")

            # 3. Calculate Current ATR using the specific Previous ATR
            if previous_atr is not None:
                current_atr = (previous_atr * 13 + current_tr) / 14
                # Ensure this calculated value is stored in the map for history
                atr_values[current_ts] = current_atr

            # 4. Update History
            if symbol not in self.atr_history: self.atr_history[symbol] = {}
            if candle_size not in self.atr_history[symbol]: self.atr_history[symbol][candle_size] = {}
            
            # Overwrite/Update history with the calculated series
            self.atr_history[symbol][candle_size].update(atr_values)
            
            # Update persistent state last_atr (optional, but good for consistency)
            symbol_candle_state['last_atr'] = current_atr

            if current_atr is not None:
                logging.info(f"Processed {symbol} ({candle_size}): TR={current_tr:.4f}, Prev ATR={previous_atr if previous_atr else 0:.4f}, Current ATR={current_atr:.4f}")
            else:
                logging.warning(f"ATR for {symbol} ({candle_size}): Not enough data. {len(sorted_trs)}/15 TRs required.")
            
            return {
                'symbol': symbol,
                'candle_size': candle_size,
                'tr': current_tr,
                'atr': current_atr,
                'previous_atr': previous_atr
            }
        except Exception as e:
            logging.error(f"Error processing ATR for {symbol}: {e}", exc_info=True)
            return {'symbol': symbol, 'tr': None, 'atr': None, 'previous_atr': None}

    async def run(self, ib: IB, enriched_positions: list, candle_settings: dict = None) -> tuple[list, dict, dict]:
        """
        Runs the ATR calculation for all positions concurrently.
        Returns a tuple of (results_list, updated_atr_state_dict, updated_atr_history_dict).
        """
        if candle_settings is None:
            candle_settings = {}
            logging.warning("ATRProcessor.run called without candle_settings. Defaulting to '1 day' for all symbols.")

        current_symbols = [p['symbol'] for p in enriched_positions]
        active_candle_sizes = {}

        tasks = []
        for p in enriched_positions:
            if (symbol := p.get('symbol')):
                # Default to '1 day' if not specified in settings
                candle_size = candle_settings.get(symbol, '1 day')
                active_candle_sizes[symbol] = candle_size
                tasks.append(self._process_symbol(ib, symbol, p['contract_details'], candle_size))

        results = await asyncio.gather(*tasks)

        # Cleanup history for symbols no longer in portfolio or old entries
        self._cleanup_history(current_symbols, active_candle_sizes)

        self._save_atr_state()
        self._save_atr_history()
        return results, self.atr_state, self.atr_history