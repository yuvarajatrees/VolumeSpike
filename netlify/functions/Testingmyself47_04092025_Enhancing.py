import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import time
import calendar
import math
import io
import json
from collections import Counter
import threading

# --- PART 1: CONFIGURATION & CONSTANTS ---
CLIENT_ID = "1101908607"
# =========================================================================
# === IMPORTANT: REPLACE WITH YOUR NEW, VALID ACCESS TOKEN EVERY TIME ===
# =========================================================================
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU3MDQzNDE2LCJpYXQiOjE3NTY5NTcwMTYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAxOTA4NjA3In0.pNJWx3Zr7HTa8yk8xr0IU0ytvAwSjN8Dxl5ET_bLOpgDkB7x8VMmuA_XDFdMVNSEoJNiWxu3vGiArbOkZW-GGQ"
# =========================================================================
TELEGRAM_BOT_TOKEN = "7932678610:AAGlx0hCtsV2VG5ghvWsQrY6KxVfwwHQWTc"
TELEGRAM_CHAT_ID = "1296098404"

# --- New Configuration for Monitoring Alerts ---
TELEGRAM_BOT_TOKEN_MONITOR = "7502280238:AAEsPr8FDkbJ8rGMPCcgj0ojbPmg1YSSx7k"
TELEGRAM_CHAT_ID_MONITOR = "1296098404"

SENT_ALERTS_FILE = "sent_alerts.json"

FNO_LIST_PATH = "C:\\Users\\Admin\\Desktop\\Yuvaraja\\28082025_OPTVol10x\\FNOlist.xlsx"
OUTPUT_EXCEL_PATH = "C:\\Users\\Admin\\Desktop\\Yuvaraja\\28082025_OPTVol10x\\Output.xlsx"
SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
LTP_URL = "https://api.dhan.co/v2/marketfeed/ltp"
INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"

headers = {
    "access-token": ACCESS_TOKEN,
    "client-id": CLIENT_ID,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# Global set to track sent alerts to prevent duplicates
sent_alerts = set()
# Global list to hold the details of stocks that have been alerted and need to be monitored
active_alerts = []
# Lock to ensure thread-safe access to the active_alerts list
alerts_lock = threading.Lock()
# Global dictionary to store pre-computed strikes for the day
pre_computed_strikes = {}
# Global dictionary to store pre-fetched previous day's volume data
prev_day_data_cache = {}

# --- PART 2: HELPER FUNCTIONS ---

def throttle_api_calls(api_type):
    """
    Implements a delay to stay within Dhan's API rate limits.
    A delay of 0.5s is used to ensure reliability and avoid '429 Too Many Requests' errors.
    """
    delay = 0.5
    time.sleep(delay)

def get_single_ltp(security_id, exchange_segment):
    """Fetches LTP for a single security ID with throttling."""
    
    throttle_api_calls("ltp")
    
    payload = {
        exchange_segment: [int(security_id)]
    }
    
    try:
        response = requests.post(LTP_URL, json=payload, headers=headers)
        
        # IMPROVED ERROR HANDLING
        if response.status_code == 401:
            print("\nFATAL ERROR: Your Dhan access token is UNAUTHORIZED or EXPIRED.")
            print("Please generate a new token from the Dhan developer console and update your ACCESS_TOKEN variable.")
            print("Exiting script.")
            os._exit(1) # Use os._exit to terminate all threads
        
        response.raise_for_status()
        data = response.json().get('data', {})
        ltp_data = data.get(exchange_segment, {})
        if str(security_id) in ltp_data:
            return ltp_data.get(str(security_id), {}).get('last_price')
        return None
    except requests.exceptions.RequestException as e:
        print(f"API request failed for {exchange_segment} ID {security_id}: {e}")
        return None

def get_strike_interval(scrip_master_df, underlying_symbol, expiry_date):
    """
    Dynamically calculates the strike interval from the scrip master data
    by finding the mode of the differences between adjacent strikes.
    """
    expiry_date_column = 'SM_EXPIRY_DATE' if 'SM_EXPIRY_DATE' in scrip_master_df.columns else 'EXPIRY_DATE'
    
    filtered_df = scrip_master_df[
        (scrip_master_df['UNDERLYING_SYMBOL'] == underlying_symbol) &
        (scrip_master_df['INSTRUMENT'] == 'OPTSTK') &
        (scrip_master_df['EXCH_ID'] == 'NSE') &
        (scrip_master_df[expiry_date_column] == expiry_date)
    ].copy()

    if filtered_df.empty:
        return 50

    unique_strikes = sorted(filtered_df['STRIKE_PRICE'].unique())
    
    if len(unique_strikes) < 2:
        return 50

    differences = [round(unique_strikes[i] - unique_strikes[i-1], 2) for i in range(1, len(unique_strikes))]
    
    if not differences:
        return 50

    # Find the most common difference (the mode)
    mode_difference = Counter(differences).most_common(1)[0][0]
    return mode_difference

def calculate_fixed_strikes(open_price, strike_interval):
    """
    Calculates a fixed set of strikes (1 ATM, 2 OTM for CE/PE) based on the day's open price.
    """
    if open_price is None or strike_interval <= 0:
        return []
    
    # Calculate the ATM strike based on the open price
    atm_strike = round(open_price / strike_interval) * strike_interval
    
    strikes_to_check = []
    
    # 2 OTM Call strikes
    strikes_to_check.append((atm_strike + strike_interval, 'CE'))
    strikes_to_check.append((atm_strike + 2 * strike_interval, 'CE'))
    
    # 2 OTM Put strikes
    strikes_to_check.append((atm_strike - strike_interval, 'PE'))
    strikes_to_check.append((atm_strike - 2 * strike_interval, 'PE'))

    # 1 ATM strike for both Call and Put
    strikes_to_check.append((atm_strike, 'CE'))
    strikes_to_check.append((atm_strike, 'PE'))
    
    # Filter out any non-positive strikes
    return [(strike, option_type) for strike, option_type in strikes_to_check if strike > 0]

def find_futures_security_id_and_expiry(scrip_master_df, underlying_symbol):
    """Finds the futures contract security ID and its expiry date from the scrip master DataFrame."""
    
    # FIX: Corrected the filtering to reference scrip_master_df instead of filtered_df
    filtered_df = scrip_master_df[
        (scrip_master_df['UNDERLYING_SYMBOL'] == underlying_symbol) &
        (scrip_master_df['INSTRUMENT'] == 'FUTSTK') &
        (scrip_master_df['EXCH_ID'] == 'NSE')
    ].copy()
    
    if 'SM_EXPIRY_DATE' in filtered_df.columns:
        filtered_df = filtered_df.sort_values(by='SM_EXPIRY_DATE').reset_index(drop=True)
    elif 'EXPIRY_DATE' in filtered_df.columns:
        filtered_df = filtered_df.sort_values(by='EXPIRY_DATE').reset_index(drop=True)
    
    if not filtered_df.empty:
        expiry_date_column = 'SM_EXPIRY_DATE' if 'SM_EXPIRY_DATE' in filtered_df.columns else 'EXPIRY_DATE'
        expiry_date = filtered_df.iloc[0][expiry_date_column]
        return filtered_df.iloc[0]['SECURITY_ID'], expiry_date
    else:
        return None, None

def find_option_security_id(scrip_master_df, underlying_symbol, strike, option_type, expiry_date):
    """
    Finds the option security ID and lot size for a specific option
    from the scrip master DataFrame.
    """
    strike_price = float(strike)
    expiry_date_column = 'SM_EXPIRY_DATE' if 'SM_EXPIRY_DATE' in scrip_master_df.columns else 'EXPIRY_DATE'
    
    filtered_df = scrip_master_df[
        (scrip_master_df['UNDERLYING_SYMBOL'] == underlying_symbol) &
        (scrip_master_df['OPTION_TYPE'] == option_type) &
        (scrip_master_df['STRIKE_PRICE'] == strike_price) &
        (scrip_master_df['EXCH_ID'] == 'NSE') &
        (scrip_master_df[expiry_date_column] == expiry_date)
    ]
    
    if not filtered_df.empty:
        # Returning both SECURITY_ID and LOT_SIZE
        security_id = filtered_df.iloc[0]['SECURITY_ID']
        lot_size = filtered_df.iloc[0]['LOT_SIZE']
        return security_id, lot_size
    else:
        return None, None

def get_strike_type(strike, option_type, ltp):
    """Determines if the strike is ITM, OTM, or ATM based on LTP."""
    if ltp is None or ltp == 0:
        return "N/A"
    
    if option_type == 'CE': # Call Option
        if strike < ltp:
            return "ITM"
        elif strike > ltp:
            return "OTM"
        else:
            return "ATM"
    elif option_type == 'PE': # Put Option
        if strike > ltp:
            return "ITM"
        elif strike < ltp:
            return "OTM"
        else:
            return "ATM"
    return "N/A"

def check_volume_condition(option_security_id, analysis_date):
    """
    Checks for 10x volume, day high, and close > open conditions,
    using a pre-fetched cache for previous day's data.
    """
    
    # --- Step 1: Get previous day's last candle data from cache ---
    prev_day_data = prev_day_data_cache.get(int(option_security_id), {})
    prev_day_last_volume = prev_day_data.get('volume', 0)
    prev_day_last_high = prev_day_data.get('high', -1)
    prev_day_last_low = prev_day_data.get('low', -1)
    
    # --- Step 2: Get today's data ---
    today_payload = {
        "securityId": int(option_security_id),
        "exchangeSegment": "NSE_FNO",
        "instrument": "OPTSTK",
        "interval": "5",
        "oi": False,
        "fromDate": analysis_date.strftime('%Y-%m-%d'),
        "toDate": analysis_date.strftime('%Y-%m-%d')
    }
    
    found_candles = []
    
    try:
        response = requests.post(INTRADAY_URL, json=today_payload, headers=headers)
        
        # IMPROVED ERROR HANDLING
        if response.status_code == 401:
            print("\nFATAL ERROR: Your Dhan access token is UNAUTHORIZED or EXPIRED.")
            print("Please generate a new token from the Dhan developer console and update your ACCESS_TOKEN variable.")
            print("Exiting script.")
            os._exit(1) # Use os._exit to terminate all threads

        response.raise_for_status()
        data = response.json()
        
        open_list = data.get('open', [])
        high_list = data.get('high', [])
        low_list = data.get('low', [])
        close_list = data.get('close', [])
        volume_list = data.get('volume', [])
        timestamp_list = data.get('timestamp', [])
        
        if not volume_list:
            return []

        # Maintain the highest high seen so far today
        day_high_so_far = -1

        # Check all candles of the current day
        for i in range(len(volume_list)):
            current_volume = volume_list[i]
            current_open = open_list[i]
            current_high = high_list[i]
            current_close = close_list[i]
            
            # Skip if any of the required data is missing for the current candle
            if not all([current_volume, current_open, current_high, current_close]):
                continue

            # Update day high
            day_high_so_far = max(day_high_so_far, current_high)
            
            # Get the correct previous volume and price data to compare against
            if i == 0:
                previous_volume = prev_day_last_volume
                previous_high = prev_day_last_high
                previous_low = prev_day_last_low
            else:
                previous_volume = volume_list[i-1]
                previous_high = high_list[i-1]
                previous_low = low_list[i-1]
            
            # Condition 1: Check for volume spike
            volume_spike_condition = (previous_volume > 500 and 
                                      current_volume > 5000 and 
                                      current_volume > (previous_volume * 10))
            
            # Condition 2: Current candle high is the day high so far
            day_high_condition = (current_high == day_high_so_far)
            
            # Condition 3: Close > Open for ALL candles (updated logic)
            close_open_condition = (current_close > current_open)
            
            # Condition 4: Previous candle was not "flat" (i.e., had price movement)
            prev_candle_is_liquid = (previous_high != previous_low)
            
            # Check for all conditions
            if (volume_spike_condition and 
                day_high_condition and 
                close_open_condition and 
                prev_candle_is_liquid):
                
                # The caller will populate SYMBOL, STRIKE, TYPE
                found_candles.append({
                    "TIMESTAMP": datetime.fromtimestamp(timestamp_list[i]).strftime('%Y-%m-%d %H:%M:%S'),
                    "OPEN": current_open,
                    "HIGH": current_high,
                    "LOW": low_list[i],
                    "CLOSE": current_close,
                    "PREV VOL": previous_volume,
                    "VOLUME": current_volume,
                })
        
        throttle_api_calls("intraday")
        return found_candles
        
    except requests.exceptions.RequestException as e:
        return []
    except ValueError as e:
        return []

def send_telegram_alert(candle, ltp, lot_size, is_backtest=False):
    """Sends a formatted message to the specified Telegram chat with a custom emoji."""
    
    alert_type = "Backtest Alert" if is_backtest else "Live Alert"
    
    # Determine the strike type (ITM, OTM, ATM)
    strike_type = get_strike_type(candle['STRIKE'], candle['TYPE'], ltp)
    
    # Parse the timestamp string from the candle dict to a datetime object
    alert_time = datetime.strptime(candle['TIMESTAMP'], '%Y-%m-%d %H:%M:%S')
    formatted_time = alert_time.strftime('%d-%b-%Y %I:%M %p')
    
    # Construct the formatted message
    message = (
        f"🔥🔥🔥 **Volume Spike {alert_type}** 🔥🔥🔥\n\n"
        f⚡️ Condition: Volume 10x Done\n"
        f"📊 Stock: **{candle['SYMBOL']}**\n"
        f"💰 Strike: **{int(candle['STRIKE'])} {candle['TYPE']} ({strike_type})**\n"
        f"📦 Lot Size: {int(lot_size)}\n"
        f"🎯 Volume: {int(candle['VOLUME'])} (Prev: {int(candle['PREV VOL'])})\n"
        f"🕒 Time: {formatted_time}"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        print(f"Telegram {alert_type} sent successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send Telegram alert: {e}")

def send_telegram_monitor_alert(message):
    """Sends a single, comprehensive message to the monitoring Telegram channel."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN_MONITOR}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID_MONITOR,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        print(f"Telegram Monitor Alert sent successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send Telegram monitor alert: {e}")

def load_sent_alerts():
    """Loads previously sent alerts from a JSON file and converts them to tuples."""
    global sent_alerts
    try:
        if os.path.exists(SENT_ALERTS_FILE):
            with open(SENT_ALERTS_FILE, 'r') as f:
                loaded_list = json.load(f)
                sent_alerts = set(tuple(item) for item in loaded_list)
        else:
            sent_alerts = set()
    except (IOError, json.JSONDecodeError) as e:
        print(f"Error loading sent alerts file: {e}. Starting with a fresh list.")
        sent_alerts = set()

def save_sent_alerts():
    """Saves the current set of sent alerts to a JSON file."""
    try:
        with open(SENT_ALERTS_FILE, 'w') as f:
            json.dump(list(sent_alerts), f)
    except IOError as e:
        print(f"Error saving sent alerts file: {e}")

def check_market_hours():
    """Checks if the current time is within Indian market hours (9:15 AM - 3:30 PM) on a weekday."""
    now = datetime.now()
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    
    # Check for weekday (Monday=0, Friday=4) and time
    is_weekday = now.weekday() < 5
    is_market_open_now = market_open <= now <= market_close
    
    return is_weekday and is_market_open_now

def monitor_alerted_strikes():
    """
    Monitors active alerts and sends a single, ranked alert for top movers.
    Runs in a separate thread every 5 minutes.
    """
    global active_alerts
    print("Starting monitoring thread...")
    while True:
        if check_market_hours():
            
            # Use a lock to ensure thread-safe access to the list
            with alerts_lock:
                # Create a copy to iterate over, in case the original list is modified
                alerts_to_check = active_alerts[:] 
            
            gaining_alerts = []
            for alert in alerts_to_check:
                ltp = get_single_ltp(alert['security_id'], "NSE_FNO")
                
                if ltp is None or alert['start_price'] == 0:
                    continue

                percent_change = ((ltp - alert['start_price']) / alert['start_price']) * 100
                
                # Only consider stocks that have made a positive move
                if percent_change > 0:
                    gaining_alerts.append({
                        "symbol": alert['symbol'],
                        "strike": alert['strike'],
                        "type": alert['type'],
                        "timestamp": alert['timestamp'],
                        "percent_change": percent_change
                    })
            
            # Sort the list in descending order by percentage change
            gaining_alerts.sort(key=lambda x: x['percent_change'], reverse=True)

            if gaining_alerts:
                message_lines = ["📈 **Top Gainers Since Alert** 📈\n"]
                for i, alert in enumerate(gaining_alerts):
                    # Format the initial time to display only hours and minutes
                    alert_time = datetime.strptime(alert['timestamp'], '%Y-%m-%d %H:%M:%S').strftime('%H:%M %p')
                    message_lines.append(
                        f"{i+1}. **{alert['symbol']}** {int(alert['strike'])} {alert['type']} @ {alert_time} ({alert['percent_change']:.2f}% up)"
                    )
                
                full_message = "\n".join(message_lines)
                send_telegram_monitor_alert(full_message)
            else:
                print("No stocks have gained since the last check. No alert sent.")
                
            # Sleep for 5 minutes before the next check
            time.sleep(300) 
        else:
            print("Monitoring thread: Market is closed. Sleeping...")
            time.sleep(600) # Sleep for 10 minutes when market is closed

def get_day_open_price(security_id, exchange_segment, analysis_date):
    """
    Fetches the open price of the first candle of the day for a given security.
    """
    payload = {
        "securityId": int(security_id),
        "exchangeSegment": exchange_segment,
        "instrument": "FUTSTK" if exchange_segment == "NSE_FNO" else "EQ",
        "interval": "5",
        "oi": False,
        "fromDate": analysis_date.strftime('%Y-%m-%d'),
        "toDate": analysis_date.strftime('%Y-%m-%d')
    }
    
    try:
        response = requests.post(INTRADAY_URL, json=payload, headers=headers)
        
        # --- FIX: Add throttling here to prevent 429 errors ---
        throttle_api_calls("intraday")
        
        response.raise_for_status()
        data = response.json()
        open_list = data.get('open', [])
        
        if open_list:
            return open_list[0] # Return the first candle's open price
        return None
    except requests.exceptions.RequestException as e:
        print(f"API request failed for Open Price {exchange_segment} ID {security_id}: {e}")
        return None
    except ValueError as e:
        print(f"Error parsing data for {security_id}: {e}")
        return None

def pre_fetch_previous_day_data(security_id, prev_trading_day):
    """
    Fetches and caches the last candle's data for the previous trading day.
    """
    global prev_day_data_cache
    payload = {
        "securityId": int(security_id),
        "exchangeSegment": "NSE_FNO",
        "instrument": "OPTSTK",
        "interval": "5",
        "oi": False,
        "fromDate": prev_trading_day.strftime('%Y-%m-%d'),
        "toDate": prev_trading_day.strftime('%Y-%m-%d')
    }
    
    try:
        response = requests.post(INTRADAY_URL, json=payload, headers=headers)
        
        # --- FIX: Add throttling here to prevent 429 errors ---
        throttle_api_calls("intraday")

        response.raise_for_status()
        data = response.json()
        
        volume_list = data.get('volume', [])
        high_list = data.get('high', [])
        low_list = data.get('low', [])
        
        if volume_list:
            prev_day_data_cache[int(security_id)] = {
                'volume': volume_list[-1],
                'high': high_list[-1],
                'low': low_list[-1]
            }
    except requests.exceptions.RequestException as e:
        print(f"Failed to pre-fetch prev day data for {security_id}: {e}")
    except (IndexError, KeyError) as e:
        # Handles cases where there's no data for the previous day
        print(f"No previous day data found for {security_id}.")
        prev_day_data_cache[int(security_id)] = {'volume': 0, 'high': -1, 'low': -1}

def _convert_pandas_types(data):
    """
    Recursively converts pandas-specific data types to standard Python types.
    """
    if isinstance(data, dict):
        return {key: _convert_pandas_types(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [_convert_pandas_types(item) for item in data]
    # Check for pandas/numpy numeric types
    elif pd.api.types.is_numeric_dtype(type(data)):
        if pd.isna(data):
            return None
        return data.item()
    # Check for pandas timestamp types
    elif pd.api.types.is_datetime64_any_dtype(type(data)):
        if pd.isna(data):
            return None
        return data.isoformat()
    # Handle other pandas types
    elif isinstance(data, pd.Series):
        return _convert_pandas_types(data.to_dict())
    elif isinstance(data, pd.DataFrame):
        return _convert_pandas_types(data.to_dict('records'))
    # Fallback for any other custom types
    elif isinstance(data, (int, float, str, bool, type(None))):
        return data
    else:
        try:
            return json.loads(json.dumps(data))
        except (TypeError, json.JSONDecodeError):
            return str(data)

def ensure_cache_file_valid(file_path):
    """
    Checks if a JSON file is valid. If not, it deletes the file.
    This is useful for handling corrupted cache files.
    """
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                json.load(f)
            return True
        except (IOError, json.JSONDecodeError) as e:
            print(f"Detected corrupted cache file: {e}. Deleting to force re-computation.")
            os.remove(file_path)
    return False

def pre_compute_fixed_strikes():
    """
    Function to pre-compute fixed strikes for all symbols and cache the result.
    This replaces the pre-computation loop in main_live.
    """
    print("\n--- Pre-computing fixed strikes for all symbols ---")
    start_pre_comp = time.time()
    computed_strikes = {}

    try:
        fno_df = pd.read_excel(FNO_LIST_PATH)
        
        print("Downloading latest scrip master from Dhan API...")
        scrip_master_response = requests.get(SCRIP_MASTER_URL)
        scrip_master_response.raise_for_status()
        scrip_master_data = io.StringIO(scrip_master_response.text)
        scrip_master_df = pd.read_csv(scrip_master_data, low_memory=False)
        
        expiry_date_column = 'SM_EXPIRY_DATE' if 'SM_EXPIRY_DATE' in scrip_master_df.columns else 'EXPIRY_DATE'
        scrip_master_df[expiry_date_column] = scrip_master_df[expiry_date_column].astype(str)
        
        if fno_df.empty:
            print("Error: FNOlist.xlsx is empty.")
            return {}
    except FileNotFoundError as e:
        print(f"Error: Required file not found - {e}")
        return {}
    except requests.exceptions.RequestException as e:
        print(f"Error downloading scrip master file: {e}")
        return {}

    current_date = datetime.now().date()
    if current_date.weekday() >= 5:
        analysis_date = current_date - timedelta(days=current_date.weekday() - 4)
    else:
        analysis_date = current_date

    for index, row in fno_df.iterrows():
        underlying_symbol = row['UNDERLYING_SYMBOL']
        underlying_security_id = row['UNDERLYING_SECURITY_ID']
        
        futures_security_id, expiry_date_str = find_futures_security_id_and_expiry(scrip_master_df, underlying_symbol)
        
        if futures_security_id is None:
            print(f"Skipping {underlying_symbol}: No futures contract found.")
            continue
            
        open_price = get_day_open_price(futures_security_id, "NSE_FNO", analysis_date)
        
        if open_price is None:
            print(f"Skipping {underlying_symbol}: Could not fetch open price.")
            continue
            
        strike_interval = get_strike_interval(scrip_master_df, underlying_symbol, expiry_date_str)
        fixed_strikes_and_types = calculate_fixed_strikes(open_price, strike_interval)
        
        strikes_for_symbol = []
        for strike, option_type in fixed_strikes_and_types:
            option_security_id, lot_size = find_option_security_id(
                scrip_master_df,
                underlying_symbol,
                strike,
                option_type,
                expiry_date_str
            )
            
            if option_security_id:
                strikes_for_symbol.append({
                    'strike': strike,
                    'type': option_type,
                    'security_id': option_security_id,
                    'lot_size': lot_size
                })
            else:
                print(f"  > Warning: Could not find security ID for {underlying_symbol} {strike} {option_type}. Skipping.")
        
        computed_strikes[underlying_symbol] = {
            'expiry': expiry_date_str,
            'strikes': strikes_for_symbol
        }

    end_pre_comp = time.time()
    print(f"--- Pre-computation complete. Took {end_pre_comp - start_pre_comp:.2f} seconds. ---")
    
    # FIX: Convert pandas types before returning
    return _convert_pandas_types(computed_strikes)


def get_previous_day_data(fixed_strikes_file_path):
    """
    Fetches and caches the previous day's data for all pre-computed strikes.
    """
    print("\n--- Caching previous day's data for all pre-computed strikes ---")
    cached_prev_day_data = {}
    
    try:
        fno_df = pd.read_excel(FNO_LIST_PATH)
        
        scrip_master_response = requests.get(SCRIP_MASTER_URL)
        scrip_master_response.raise_for_status()
        scrip_master_data = io.StringIO(scrip_master_response.text)
        scrip_master_df = pd.read_csv(scrip_master_data, low_memory=False)
    except Exception as e:
        print(f"Error fetching data for caching: {e}")
        return {}

    current_date = datetime.now().date()
    if current_date.weekday() == 0:
        prev_trading_day = current_date - timedelta(days=3)
    else:
        prev_trading_day = current_date - timedelta(days=1)

    for underlying_symbol, data in pre_computed_strikes.items():
        for option_data in data['strikes']:
            security_id = option_data['security_id']
            pre_fetch_previous_day_data(security_id, prev_trading_day)
    
    print("--- Caching complete. ---")
    return prev_day_data_cache

def main_live():
    global active_alerts, pre_computed_strikes, prev_day_data_cache
    load_sent_alerts()
    print("--- FNO Options Volume Scanner - Live Mode ---")

    fixed_strikes_file_path = "fixed_strikes_cache.json"

    # Use the new function to validate the cache file
    if ensure_cache_file_valid(fixed_strikes_file_path):
        print("Cached data found.")
        use_cached = input("Do you want to use the cached pre-computed data? (yes/no): ").strip().lower()
        if use_cached == 'yes':
            try:
                with open(fixed_strikes_file_path, "r") as f:
                    pre_computed_strikes = json.load(f)
                print("Using cached pre-computed fixed strikes.")
            except (IOError, json.JSONDecodeError) as e:
                # This block should now be rarely hit due to the check above
                print(f"Error loading cached data: {e}. Re-computing fixed strikes.")
                pre_computed_strikes = pre_compute_fixed_strikes()
                with open(fixed_strikes_file_path, "w") as f:
                    json.dump(pre_computed_strikes, f)
        else:
            print("Re-computing fixed strikes.")
            pre_computed_strikes = pre_compute_fixed_strikes()
            with open(fixed_strikes_file_path, "w") as f:
                json.dump(pre_computed_strikes, f)
    else:
        print("No cached data found or file was corrupted. Pre-computing fixed strikes...")
        pre_computed_strikes = pre_compute_fixed_strikes()
        with open(fixed_strikes_file_path, "w") as f:
            json.dump(pre_computed_strikes, f)

    prev_day_data_cache = get_previous_day_data(fixed_strikes_file_path)

    # Start the monitoring thread
    monitoring_thread = threading.Thread(target=monitor_alerted_strikes, daemon=True)
    monitoring_thread.start()

    try:
        fno_df = pd.read_excel(FNO_LIST_PATH)
        scrip_master_response = requests.get(SCRIP_MASTER_URL)
        scrip_master_response.raise_for_status()
        scrip_master_data = io.StringIO(scrip_master_response.text)
        scrip_master_df = pd.read_csv(scrip_master_data, low_memory=False)
        
        expiry_date_column = 'SM_EXPIRY_DATE' if 'SM_EXPIRY_DATE' in scrip_master_df.columns else 'EXPIRY_DATE'
        scrip_master_df[expiry_date_column] = scrip_master_df[expiry_date_column].astype(str)
        
        if fno_df.empty:
            print("Error: FNOlist.xlsx is empty.")
            return
    except FileNotFoundError as e:
        print(f"Error: Required file not found - {e}")
        return
    except requests.exceptions.RequestException as e:
        print(f"Error downloading scrip master file: {e}")
        return
    
    current_date = datetime.now().date()
    if current_date.weekday() >= 5:
        analysis_date = current_date - timedelta(days=current_date.weekday() - 4)
    else:
        analysis_date = current_date
    
    main_loop_analysis_date = analysis_date

    while True:
        if check_market_hours():
            start_time = time.time()
            print(f"--- Starting new scan at {datetime.now().strftime('%H:%M:%S')} ---")
            
            all_found_candles = []
            
            print(f"\nScanning for High Volume Candles on: {main_loop_analysis_date.strftime('%Y-%m-%d')}\n")

            for underlying_symbol, data in pre_computed_strikes.items():
                print(f"\n--- PROCESSING: {underlying_symbol} ---")
                
                # Correct way to get the underlying security ID from the DataFrame
                try:
                    current_underlying_security_id = fno_df[fno_df['UNDERLYING_SYMBOL'] == underlying_symbol]['UNDERLYING_SECURITY_ID'].iloc[0]
                except IndexError:
                    print(f"Warning: Could not find UNDERLYING_SECURITY_ID for {underlying_symbol}. Skipping.")
                    continue
                
                futures_security_id, _ = find_futures_security_id_and_expiry(scrip_master_df, underlying_symbol)
                
                ltp = get_single_ltp(futures_security_id, "NSE_FNO")
                if ltp is None:
                    # Fallback to the equity LTP if futures LTP is not available
                    ltp = get_single_ltp(current_underlying_security_id, "NSE_EQ")
                
                for option_data in data['strikes']:
                    # Call the optimized function
                    found_candles = check_volume_condition(
                        option_data['security_id'],
                        main_loop_analysis_date
                    )
                    
                    for candle in found_candles:
                        alert_id = (
                            underlying_symbol,
                            str(option_data['strike']),
                            option_data['type'],
                            candle['TIMESTAMP']
                        )
                        
                        if alert_id not in sent_alerts:
                            candle['SYMBOL'] = underlying_symbol
                            candle['STRIKE'] = option_data['strike']
                            candle['TYPE'] = option_data['type']
                            candle['LOT_SIZE'] = option_data['lot_size']
                            
                            all_found_candles.append(candle)
                            sent_alerts.add(alert_id)
                            
                            send_telegram_alert(candle, ltp, option_data['lot_size'])
                            
                            new_alert = {
                                "symbol": candle['SYMBOL'],
                                "strike": candle['STRIKE'],
                                "type": candle['TYPE'],
                                "timestamp": candle['TIMESTAMP'],
                                "start_price": candle['CLOSE'],
                                "security_id": option_data['security_id'],
                            }
                            with alerts_lock:
                                active_alerts.append(new_alert)
                        else:
                            print(f"Alert already sent for {alert_id}. Skipping.")
            
            end_time = time.time()
            print(f"\n--- Scan finished in {end_time - start_time:.2f} seconds ---")
            
            save_sent_alerts()
            
            if all_found_candles:
                try:
                    results_df = pd.DataFrame(all_found_candles)
                    results_df.insert(0, 'S.No.', range(1, 1 + len(results_df)))
                    results_df.to_excel(OUTPUT_EXCEL_PATH, index=False)
                    print(f"Successfully saved new results to: {OUTPUT_EXCEL_PATH}")
                except Exception as e:
                    print(f"Error saving Excel file: {e}")
            else:
                print("\nNo new high volume candles were found on this scan.")
            
            current_minute = datetime.now().minute
            wait_time = (5 - (current_minute % 5)) * 60 - datetime.now().second
            if wait_time > 0:
                print(f"Sleeping for {wait_time} seconds until the next 5-minute candle.")
                time.sleep(wait_time)
            else:
                print("Proceeding to next scan immediately as the 5-minute interval has passed.")
                time.sleep(10)

        else:
            print(f"Market is closed. Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}. Waiting...")
            time.sleep(600)

def main_backtest():
    """
    Backtesting function to scan a historical date for alerts.
    """
    print("--- FNO Options Volume Scanner - Backtest Mode ---")

    try:
        fno_df = pd.read_excel(FNO_LIST_PATH)
        
        print("Downloading latest scrip master from Dhan API...")
        scrip_master_response = requests.get(SCRIP_MASTER_URL)
        scrip_master_response.raise_for_status()
        scrip_master_data = io.StringIO(scrip_master_response.text)
        scrip_master_df = pd.read_csv(scrip_master_data, low_memory=False)
        
        expiry_date_column = 'SM_EXPIRY_DATE' if 'SM_EXPIRY_DATE' in scrip_master_df.columns else 'EXPIRY_DATE'
        scrip_master_df[expiry_date_column] = scrip_master_df[expiry_date_column].astype(str)
        
        if fno_df.empty:
            print("Error: FNOlist.xlsx is empty.")
            return
    except FileNotFoundError as e:
        print(f"Error: Required file not found - {e}")
        return
    except requests.exceptions.RequestException as e:
        print(f"Error downloading scrip master file: {e}")
        return

    date_str = input("Enter the date to backtest (YYYY-MM-DD): ").strip()
    try:
        analysis_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        return

    if analysis_date.weekday() == 0:
        prev_trading_day = analysis_date - timedelta(days=3)
    else:
        prev_trading_day = analysis_date - timedelta(days=1)
    
    print(f"\nScanning for High Volume Candles on: {analysis_date.strftime('%Y-%m-%d')} (Previous trading day: {prev_trading_day.strftime('%Y-%m-%d')})\n")

    all_backtest_alerts = []
    
    for index, row in fno_df.iterrows():
        underlying_symbol = row['UNDERLYING_SYMBOL']
        underlying_security_id = row['UNDERLYING_SECURITY_ID']

        print(f"\n--- PROCESSING: {underlying_symbol} ---")

        futures_security_id, expiry_date_str = find_futures_security_id_and_expiry(scrip_master_df, underlying_symbol)
        
        if futures_security_id is None:
            print(f"No futures contract found for {underlying_symbol}. Skipping.")
            continue
        
        # --- FIX: Use open price for strike calculation, not LTP ---
        open_price = get_day_open_price(futures_security_id, "NSE_FNO", analysis_date)
        if open_price is None:
            # Fallback to the underlying's open price if futures is unavailable
            open_price = get_day_open_price(underlying_security_id, "NSE_EQ", analysis_date)

        if open_price is None:
            print(f"Skipping {underlying_symbol} due to inability to fetch open price.")
            continue
        
        strike_interval = get_strike_interval(scrip_master_df, underlying_symbol, expiry_date_str)
        strikes_to_check = calculate_fixed_strikes(open_price, strike_interval)
        
        for strike, option_type in strikes_to_check:
            option_security_id, lot_size = find_option_security_id(
                scrip_master_df,
                underlying_symbol,
                strike,
                option_type,
                expiry_date_str
            )
            
            if option_security_id:
                # Need to run a full check with both API calls for backtesting
                found_candles = check_volume_condition_backtest(
                    option_security_id,
                    "OPTSTK",
                    analysis_date,
                    prev_trading_day
                )
                
                for candle in found_candles:
                    candle['SYMBOL'] = underlying_symbol
                    candle['STRIKE'] = strike
                    candle['TYPE'] = option_type
                    candle['LOT_SIZE'] = lot_size
                    candle['STRIKE_TYPE'] = get_strike_type(strike, option_type, open_price)
                    
                    send_telegram_alert(candle, open_price, lot_size, is_backtest=True)
                    
                    all_backtest_alerts.append(candle)
            else:
                print(f"  > Error: Could not find security ID in CSV for {underlying_symbol} {strike} {option_type}.")
    
    if all_backtest_alerts:
        try:
            results_df = pd.DataFrame(all_backtest_alerts)
            results_df.insert(0, 'S.No.', range(1, 1 + len(results_df)))
            results_df.to_excel(OUTPUT_EXCEL_PATH, index=False)
            print(f"\nSuccessfully saved all backtest results to: {OUTPUT_EXCEL_PATH}")
        except Exception as e:
            print(f"\nError saving Excel file: {e}")
    else:
        print("\nNo high volume candles were found on this backtest day.")

def check_volume_condition_backtest(option_security_id, instrument_type, analysis_date, prev_trading_day):
    """
    A separate, full version of the volume condition check for backtesting.
    This version includes both API calls as it's a one-time process.
    """
    
    # --- Step 1: Get previous day's last candle data ---
    prev_day_payload = {
        "securityId": int(option_security_id),
        "exchangeSegment": "NSE_FNO",
        "instrument": instrument_type,
        "interval": "5",
        "oi": False,
        "fromDate": prev_trading_day.strftime('%Y-%m-%d'),
        "toDate": prev_trading_day.strftime('%Y-%m-%d')
    }
    
    prev_day_last_volume = 0
    prev_day_last_high = -1
    prev_day_last_low = -1
    
    try:
        response = requests.post(INTRADAY_URL, json=prev_day_payload, headers=headers)
        response.raise_for_status()
        prev_data = response.json()
        prev_volume_list = prev_data.get('volume', [])
        prev_high_list = prev_data.get('high', [])
        prev_low_list = prev_data.get('low', [])
        
        if prev_volume_list and prev_high_list and prev_low_list:
            prev_day_last_volume = prev_volume_list[-1]
            prev_day_last_high = prev_high_list[-1]
            prev_day_last_low = prev_low_list[-1]
            
        throttle_api_calls("intraday")
    except requests.exceptions.RequestException as e:
        return []

    # --- Step 2: Get today's data ---
    today_payload = {
        "securityId": int(option_security_id),
        "exchangeSegment": "NSE_FNO",
        "instrument": instrument_type,
        "interval": "5",
        "oi": False,
        "fromDate": analysis_date.strftime('%Y-%m-%d'),
        "toDate": analysis_date.strftime('%Y-%m-%d')
    }
    
    found_candles = []
    
    try:
        response = requests.post(INTRADAY_URL, json=today_payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        open_list = data.get('open', [])
        high_list = data.get('high', [])
        low_list = data.get('low', [])
        close_list = data.get('close', [])
        volume_list = data.get('volume', [])
        timestamp_list = data.get('timestamp', [])
        
        if not volume_list:
            return []

        day_high_so_far = -1

        for i in range(len(volume_list)):
            current_volume = volume_list[i]
            current_open = open_list[i]
            current_high = high_list[i]
            current_close = close_list[i]
            
            if not all([current_volume, current_open, current_high, current_close]):
                continue

            day_high_so_far = max(day_high_so_far, current_high)
            
            if i == 0:
                previous_volume = prev_day_last_volume
                previous_high = prev_day_last_high
                previous_low = prev_day_last_low
            else:
                previous_volume = volume_list[i-1]
                previous_high = high_list[i-1]
                previous_low = low_list[i-1]
            
            volume_spike_condition = (previous_volume > 500 and 
                                      current_volume > 5000 and 
                                      current_volume > (previous_volume * 10))
            
            day_high_condition = (current_high == day_high_so_far)
            close_open_condition = (current_close > current_open)
            prev_candle_is_liquid = (previous_high != previous_low)
            
            if (volume_spike_condition and 
                day_high_condition and 
                close_open_condition and 
                prev_candle_is_liquid):
                
                found_candles.append({
                    "TIMESTAMP": datetime.fromtimestamp(timestamp_list[i]).strftime('%Y-%m-%d %H:%M:%S'),
                    "OPEN": current_open,
                    "HIGH": current_high,
                    "LOW": low_list[i],
                    "CLOSE": current_close,
                    "PREV VOL": previous_volume,
                    "VOLUME": current_volume,
                })
        
        throttle_api_calls("intraday")
        return found_candles
        
    except requests.exceptions.RequestException as e:
        return []
    except ValueError as e:
        return []

def handler(event, context):
    print("Starting Netlify Function")
    # Call your existing main function
    main_live()
    return {
        "statusCode": 200,
        "body": "Function executed successfully!"
    }

if __name__ == "__main__":
    mode = input("Enter 'live' for live scanning or 'backtest' for historical backtesting: ").strip().lower()
    if mode == 'live':
        main_live()
    elif mode == 'backtest':
        main_backtest()
    else:
        print("Invalid mode. Please enter 'live' or 'backtest'.")