import asyncio
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
def get_prev_day_high_low(symbol):
    today = datetime.now().date()
    prev_day = today - timedelta(days=1)

    params = {
        "symbol": f"NSE:{symbol}",
        "resolution": "D",          # Daily candle
        "date_format": "1",
        "range_from": prev_day.strftime("%Y-%m-%d"),
        "range_to": prev_day.strftime("%Y-%m-%d"),
        "cont_flag": "1"
    }

    resp = fyers.history(params)
    if resp and "candles" in resp and len(resp["candles"]) > 0:
        c = resp["candles"][0]
        return {"high": c[2], "low": c[3]}   # index: [time, open, high, low, close, volume]
    return {"high": None, "low": None}
from fyers_apiv3 import fyersModel
from fyers_api.Websocket import ws

# ====== Fyers API Setup ======
# Load credentials from files in C:\API CONNECTION\Fyers
def load_cred(file_name):
    path = r"C:\API CONNECTION\Fyers\\" + file_name
    with open(path, "r") as file:
        return file.read().strip()

client_id = load_cred("client_id.txt")
secret_key = load_cred("secret_key.txt")
access_token = load_cred("access_token.txt")

fyers = fyersModel.FyersModel(client_id=client_id, token=access_token)

# ====== Nifty 50 List ======
nifty_50 = [
    "ADANIENT-EQ", "ADANIPORTS-EQ", "APOLLOHOSP-EQ", "ASIANPAINT-EQ", "AXISBANK-EQ",
    "BAJAJ-AUTO-EQ", "BAJAJFINSV-EQ", "BAJFINANCE-EQ", "BEL-EQ", "BHARTIARTL-EQ",
    "CIPLA-EQ", "COALINDIA-EQ", "DRREDDY-EQ", "EICHERMOT-EQ", "ETERNAL-EQ",
    "GRASIM-EQ", "HCLTECH-EQ", "HDFCBANK-EQ", "HDFCLIFE-EQ", "HEROMOTOCO-EQ",
    "HINDALCO-EQ", "HINDUNILVR-EQ", "ICICIBANK-EQ", "INDUSINDBK-EQ", "INFY-EQ",
    "ITC-EQ", "JIOFIN-EQ", "JSWSTEEL-EQ", "KOTAKBANK-EQ", "LT-EQ",
    "M&M-EQ", "MARUTI-EQ", "NESTLEIND-EQ", "NTPC-EQ", "ONGC-EQ",
    "POWERGRID-EQ", "RELIANCE-EQ", "SBILIFE-EQ", "SBIN-EQ", "SHRIRAMFIN-EQ",
    "SUNPHARMA-EQ", "TATACONSUM-EQ", "TATAMOTORS-EQ", "TATASTEEL-EQ", "TCS-EQ",
    "TECHM-EQ", "TITAN-EQ", "TRENT-EQ", "ULTRACEMCO-EQ", "WIPRO-EQ"
]

# ====== State ======
candles_1m = {}
prev_day_levels = {}
buffer_ticks = {}
signals = []

# ====== VWAP ======
def calculate_vwap(df):
    tp = (df['high'] + df['low'] + df['close']) / 3
    return (tp * df['volume']).cumsum() / df['volume'].cumsum()

def generate_signals(symbol, df, prev_high, prev_low):
    latest = df.iloc[-1]
    open_ = df.iloc[0]['open']
    close = latest['close']
    vwap = calculate_vwap(df).iloc[-1]

    if open_ > prev_high and close > open_ and close > vwap:
        return f"📈 {symbol}: GAP UP + Bullish + Above VWAP"
    elif open_ < prev_low and close < open_ and close < vwap:
        return f"📉 {symbol}: GAP DOWN + Bearish + Below VWAP"
    return None

# ====== WebSocket Handler ======
def on_message(msg):
    symbol_data = msg.get("symbolData", [])[0]
    symbol = symbol_data["symbol"].split(":")[-1]

    ts = datetime.fromtimestamp(symbol_data["timestamp"])
    minute = ts.replace(second=0, microsecond=0)

    tick = {
        "timestamp": ts,
        "open": symbol_data["open_price"],
        "high": symbol_data["high_price"],
        "low": symbol_data["low_price"],
        "close": symbol_data["close_price"],
        "volume": symbol_data["min_volume"]
    }

    if symbol not in buffer_ticks:
        buffer_ticks[symbol] = {}
    if symbol not in candles_1m:
        candles_1m[symbol] = []

    if minute not in buffer_ticks[symbol]:
        buffer_ticks[symbol][minute] = tick
    else:
        buf = buffer_ticks[symbol][minute]
        buf["high"] = max(buf["high"], tick["high"])
        buf["low"] = min(buf["low"], tick["low"])
        buf["close"] = tick["close"]
        buf["volume"] += tick["volume"]

# ====== Streamlit UI ======
st.title("📊 Nifty 50 Live Screener")
signal_box = st.empty()

async def candle_loop():
    while True:
        await asyncio.sleep(60)
        now = datetime.now().replace(second=0, microsecond=0)

        for symbol in nifty_50:
            if symbol not in buffer_ticks or now not in buffer_ticks[symbol]:
                continue

            candle = buffer_ticks[symbol].pop(now)
            candles_1m[symbol].append(candle)

            if len(candles_1m[symbol]) > 30:
                candles_1m[symbol] = candles_1m[symbol][-30:]

            if len(candles_1m[symbol]) >= 5 and len(candles_1m[symbol]) % 5 == 0:
                df = pd.DataFrame(candles_1m[symbol][-5:])
                prev = prev_day_levels[symbol]
                signal = generate_signals(symbol, df, prev["high"], prev["low"])
                if signal:
                    signals.append({"time": datetime.now().strftime("%H:%M:%S"), "signal": signal})

        # Update UI
        if signals:
            signal_box.table(pd.DataFrame(signals))

async def main():
    # Load previous day levels
    for symbol in nifty_50:
        prev_day_levels[symbol] = get_prev_day_high_low(symbol)

    # Setup WebSocket
    fyers_ws = ws.FyersSocket(access_token=access_token, log_path=".")
    fyers_ws.websocket_data = on_message
    fyers_ws.subscribe(symbol=[f"NSE:{s}" for s in nifty_50], data_type="symbolUpdate")

    # Run socket (no background arg in this version)
    fyers_ws.keep_running()

    # Start candle loop
    await candle_loop()


if __name__ == "__main__":
    asyncio.run(main())
