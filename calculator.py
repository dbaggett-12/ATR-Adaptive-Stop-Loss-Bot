# calculator.py
import logging
from decimal import Decimal, ROUND_DOWN

from utils import get_point_value # Import from the new utils file

class PortfolioCalculator:
    """
    Handles all business logic and calculations for portfolio positions.
    This class is UI-agnostic and runs in the background.
    """
    def __init__(self, atr_history, user_overrides, highest_stop_losses, atr_ratios, market_statuses, log_callback=None):
        self.atr_state = atr_history # This is the full ATR state object.
        self.user_overrides = user_overrides # Kept for signature compatibility, but logic is removed.
        self.highest_stop_losses = highest_stop_losses
        self.atr_ratios = atr_ratios
        self.market_statuses = market_statuses
        self.log_callback = log_callback or (lambda msg: logging.info(msg))

    def compute_stop_loss(self, position_data, current_price, atr_value, atr_ratio, apply_ratchet=True):
        """
        Compute stop loss, applying rounding and ratcheting logic. 
        Returns a tuple of (final_stop, status) where status is 'new' or 'held'.
        """
        symbol = position_data['symbol']

        # If no valid data, return the last known highest stop, and status 'held'
        if atr_value is None or current_price <= 0:
            return self.highest_stop_losses.get(symbol, 0), 'held'

        raw_stop = current_price - (atr_value * atr_ratio)

        contract_details = position_data.get('contract_details', {})
        min_tick = contract_details.get('minTick')
        quantity = position_data.get('positions_held', 0)

        final_stop = float(raw_stop)
        if min_tick and min_tick > 0 and quantity != 0:
            raw_stop_decimal = Decimal(str(raw_stop))
            min_tick_decimal = Decimal(str(min_tick))
            
            # Round down for long positions (SELL stop), which is conservative.
            # For short positions (BUY stop), this also rounds "down" (e.g., 100.7 -> 100.5), 
            # which moves the stop further from the market, also a conservative move.
            rounding_mode = ROUND_DOWN
            
            rounded_stop_decimal = (raw_stop_decimal / min_tick_decimal).quantize(Decimal('1'), rounding=rounding_mode) * min_tick_decimal
            final_stop = float(rounded_stop_decimal)
            logging.debug(f"Rounding for {symbol}: raw={raw_stop}, minTick={min_tick}, final={final_stop}")
        
        # Check if a ratchet value exists for this symbol.
        # The presence of a key in highest_stop_losses indicates an active, tracked position.
        is_new_position = symbol not in self.highest_stop_losses
        
        if apply_ratchet:
            # If this is a brand new position, establish the first stop and treat it as 'new'.
            if is_new_position:
                self.highest_stop_losses[symbol] = final_stop
                return final_stop, 'new'
            
            # If it's an existing position, apply ratcheting logic.
            is_long = quantity > 0
            if is_long:
                prev_highest = self.highest_stop_losses.get(symbol, 0)
                # New stop must be higher than previous highest
                if final_stop > prev_highest:
                    self.highest_stop_losses[symbol] = final_stop
                    return final_stop, 'new'
                else:
                    # The calculated stop is not an improvement, so we hold the previous highest.
                    return prev_highest, 'held'
            else: # is_short
                # For short positions, an "improving" stop is a lower price.
                prev_highest_short = self.highest_stop_losses.get(symbol, float('inf'))
                if final_stop < prev_highest_short:
                    self.highest_stop_losses[symbol] = final_stop
                    return final_stop, 'new'
                else:
                    # The calculated stop is not an improvement, so we hold the previous highest.
                    return prev_highest_short, 'held'
        else:
            # If not ratcheting, it's always considered 'new' for UI feedback purposes
            return final_stop, 'new'

    def calculate_risk(self, position_data, computed_stop):
        """Calculates the dollar and percentage risk for a position."""
        risk_value = 0
        percent_risk = 0.0
        avg_cost = position_data.get('avg_cost', 0)
        quantity = position_data.get('positions_held', 0)

        if computed_stop and avg_cost > 0:
            # Check for "NO RISK" condition (Stop is better than entry)
            is_long = quantity > 0
            if (is_long and computed_stop >= avg_cost) or (not is_long and computed_stop <= avg_cost):
                return "NO RISK", 0.0

            risk_in_points = abs(avg_cost - computed_stop)
            
            point_value = get_point_value(
                position_data['symbol'], 
                position_data.get('contract_details', {}), 
                position_data.get('multiplier', 1.0)
            )
            
            risk_value = risk_in_points * point_value * abs(quantity)
            
            hypothetical_account_value = 6000.0 # This could be a configurable setting
            if hypothetical_account_value > 0:
                percent_risk = (risk_value / hypothetical_account_value) * 100
        
        return risk_value, percent_risk

    def process_positions(self, positions_data, atr_results):
        """
        Takes raw position and ATR data, returns a list of fully calculated position objects for the UI.
        """
        processed_data = []
        atr_map = {res['symbol']: res for res in atr_results}

        for i, p_data in enumerate(positions_data):
            symbol = p_data['symbol']
            atr_data = atr_map.get(symbol, {})
            atr_value = atr_data.get('atr')
            
            # Get ATR ratio from the UI state passed during initialization
            atr_ratio = self.atr_ratios.get(symbol, 1.5)

            # --- Stop Loss Calculation ---
            computed_stop, stop_status = self.compute_stop_loss(
                p_data, p_data['current_price'], atr_value, atr_ratio
            )

            # --- Risk Calculation ---
            risk_value, percent_risk = self.calculate_risk(p_data, computed_stop)

            # --- Assemble final object for UI ---
            p_data['atr_value'] = atr_value
            p_data['atr_ratio'] = atr_ratio
            p_data['previous_atr'] = atr_data.get('previous_atr')
            p_data['tr'] = atr_data.get('tr')
            p_data['computed_stop_loss'] = computed_stop
            p_data['stop_status'] = stop_status # Add the new status field
            p_data['dollar_risk'] = risk_value
            p_data['percent_risk'] = percent_risk
            p_data['status'] = "Ready" # Default status

            processed_data.append(p_data)

        return processed_data