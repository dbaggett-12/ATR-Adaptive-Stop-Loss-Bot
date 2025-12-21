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

    def _cleanup_history(self, current_symbols: list[str]):
        """
        Removes symbols no longer in the portfolio and old TR data from the state.
        """
        cutoff_date = datetime.now() - timedelta(days=100)

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
                        cutoff_cmp = cutoff_date.astimezone() if timestamp_dt.tzinfo else cutoff_date
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

            for candle_size in list(self.atr_history[symbol].keys()):
                timestamps_to_remove = []
                symbol_atr_candle_history = self.atr_history[symbol][candle_size]
                for timestamp_str in symbol_atr_candle_history:
                    try:
                        timestamp_dt = datetime.fromisoformat(timestamp_str)
                        cutoff_cmp = cutoff_date.astimezone() if timestamp_dt.tzinfo else cutoff_date
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
            
            # Exclude the last bar, as it is potentially incomplete.
            df['date'] = pd.to_datetime(df['date'])
            completed_bars_df = df.iloc[:-1]
            
            if completed_bars_df.empty or len(completed_bars_df) < 2:
                logging.warning(f"Not enough *completed* bars for {symbol} to calculate TR.")
                return {'symbol': symbol, 'tr': 0.0, 'atr': None, 'previous_atr': 0.0}

            # Calculate TR for all historical bars
            tr_df = self._calculate_true_ranges(completed_bars_df)

            # --- State Management and Calculation ---
            # The state is now partitioned by symbol, then by candle_size.
            if symbol not in self.atr_state:
                self.atr_state[symbol] = {}
            if candle_size not in self.atr_state[symbol]:
                self.atr_state[symbol][candle_size] = {'last_atr': None, 'tr_history': {}}

            symbol_candle_state = self.atr_state[symbol][candle_size]
            tr_history = symbol_candle_state.get('tr_history', {})
            last_known_ts_str = max(tr_history.keys()) if tr_history else None

            # Append only new TRs to the history
            new_trs_added = 0
            for _, row in tr_df.iterrows():
                timestamp_key = row['date'].isoformat()
                if last_known_ts_str is None or timestamp_key > last_known_ts_str:
                    tr_history[timestamp_key] = row['tr']
                    new_trs_added += 1
            
            if new_trs_added > 0:
                logging.info(f"Added {new_trs_added} new TR values to history for {symbol} ({candle_size}).")

            # --- ATR Calculation: Initialization vs. Update ---
            last_atr = symbol_candle_state.get('last_atr')
            current_tr = tr_df['tr'].iloc[-1]
            previous_atr_for_ui = last_atr # The ATR from the previous complete cycle
            current_atr = None
            current_bar_timestamp = tr_df['date'].iloc[-1].isoformat()

            if last_atr is None:
                # INITIALIZATION PHASE: No ATR exists, try to create the first one.
                # Sort by timestamp (dict key) to get TRs in chronological order.
                all_trs = [v for _, v in sorted(tr_history.items())]
                if len(all_trs) >= 14:
                    # We have enough data to initialize.
                    initial_atr = sum(all_trs[-14:]) / 14
                    symbol_candle_state['last_atr'] = initial_atr
                    current_atr = initial_atr # The first calculated ATR is the simple average
                    logging.info(f"ATR for {symbol} ({candle_size}) INITIALIZED with simple average of 14 TRs: {current_atr:.4f}")
                else:
                    # Not enough history to initialize. Do not publish an ATR.
                    logging.warning(f"ATR for {symbol} ({candle_size}): Cannot initialize. Only {len(all_trs)}/14 TRs available.")
                    current_atr = None
            else:
                # UPDATE PHASE: An ATR already exists. Use Wilder's smoothing.
                # The "previous ATR" is the one we just loaded from state.
                # The "current TR" is from the most recently completed bar.
                current_atr = ((last_atr * 13) + current_tr) / 14
                symbol_candle_state['last_atr'] = current_atr # Persist the new ATR for the next run
                logging.info(f"Processed {symbol} ({candle_size}): TR={current_tr:.4f}, Prev ATR={last_atr:.4f}, New ATR={current_atr:.4f}")

            # --- Save to ATR History for Graphing ---
            if current_atr is not None:
                if symbol not in self.atr_history:
                    self.atr_history[symbol] = {}
                if candle_size not in self.atr_history[symbol]:
                    self.atr_history[symbol][candle_size] = {}

                # Use the timestamp of the bar that generated the TR
                self.atr_history[symbol][candle_size][current_bar_timestamp] = current_atr
            
            return {
                'symbol': symbol,
                'candle_size': candle_size,
                'tr': current_tr,
                'atr': current_atr,
                'previous_atr': previous_atr_for_ui
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

        tasks = []
        for p in enriched_positions:
            if (symbol := p.get('symbol')):
                # Default to '1 day' if not specified in settings
                candle_size = candle_settings.get(symbol, '1 day')
                tasks.append(self._process_symbol(ib, symbol, p['contract_details'], candle_size))

        results = await asyncio.gather(*tasks)

        # Cleanup history for symbols no longer in portfolio or old entries
        self._cleanup_history(current_symbols)

        self._save_atr_state()
        self._save_atr_history()
        return results, self.atr_state, self.atr_history