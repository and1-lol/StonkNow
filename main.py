import os
import time
import json
import queue
import threading
import asyncio
import yfinance as yf
from flask import Flask, render_template, Response, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Allows your frontend to talk seamlessly with the backend API

# Shared Global State
STATE = {
    "sst": "NVDA",
    "sst2": "BOXX",
    "p": 0.05,  # 5% change threshold
    "browser_notifications_enabled": True
}

# Thread-safe queue for push notifications to the browser frontend
notification_queue = queue.Queue()

def log_event(message):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    with open('log.txt', 'a') as m:
        m.write(f"[{timestamp}] {message}\n")

async def monitor_ticker(ticker_symbol):
    """Background worker that continuously pulls asset info via yfinance fast_info."""
    if not ticker_symbol:
        return
    try:
        stock = yf.Ticker(ticker_symbol)
        info = stock.fast_info
        ncv = info.get("day_change")  # Fractional day change (e.g. 0.02 for 2%)
        current_price = info.get("last_price")

        if ncv is not None and current_price is not None:
            threshold = STATE["p"]
            if ncv > threshold:
                msg = f"🚀 {ticker_symbol} is UP by {(ncv*100):.2f}% (Price: ${current_price:.2f})"
                if STATE["browser_notifications_enabled"]:
                    notification_queue.put(msg)
                    log_event(msg)
            elif ncv < -threshold:
                msg = f"📉 {ticker_symbol} is DOWN by {(ncv*100):.2f}% (Price: ${current_price:.2f})"
                if STATE["browser_notifications_enabled"]:
                    notification_queue.put(msg)
                    log_event(msg)
    except Exception as e:
        # Fail silently to avoid crashing the background engine loop
        pass

async def background_loop():
    """Infinite loop orchestration task for processing active tickers."""
    while True:
        # Create dynamic tasks based on current user inputs
        tasks = []
        if STATE["sst"]:
            tasks.append(monitor_ticker(STATE["sst"]))
        if STATE["sst2"]:
            tasks.append(monitor_ticker(STATE["sst2"]))
        
        if tasks:
            await asyncio.gather(*tasks)
        
        # Pull stock changes every 20 seconds
        await asyncio.sleep(20)

def start_async_loop():
    """Starts the asyncio engine cleanly inside a dedicated background thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(background_loop())

# --- FLASK API ROUTING ENGINE ---

@app.route('/api/state', methods=['GET', 'POST'])
def handle_state():
    """Gets current engine config or modifies tracker targets and percentages."""
    if request.method == 'POST':
        data = request.json
        if "sst" in data: STATE["sst"] = data["sst"].upper().strip() or None
        if "sst2" in data: STATE["sst2"] = data["sst2"].upper().strip() or None
        if "p" in data: STATE["p"] = float(data["p"]) / 100  # Converts '5' UI input to 0.05
        if "enabled" in data: STATE["browser_notifications_enabled"] = bool(data["enabled"])
        return jsonify({"status": "success", "config": STATE})
    
    # Return human-readable standard percentage format back out to the client UI
    return jsonify({
        "sst": STATE["sst"],
        "sst2": STATE["sst2"],
        "p": STATE["p"] * 100,
        "enabled": STATE["browser_notifications_enabled"]
    })

@app.route('/api/price/<ticker>', methods=['GET'])
def get_instant_price(ticker):
    """Pulls current live single market asset metric explicitly on user click request."""
    try:
        stock = yf.Ticker(ticker.upper())
        info = stock.fast_info
        return jsonify({
            "ticker": ticker.upper(),
            "price": round(info.get("last_price", 0), 2),
            "change": round((info.get("day_change", 0) * 100), 2)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/logs', methods=['GET'])
def get_logs():
    """Reads transactional event notification log contents safely from disk."""
    if not os.path.exists('log.txt'):
        return jsonify({"logs": []})
    with open('log.txt', 'r') as file:
        lines = file.readlines()
        return jsonify({"logs": [line.strip() for line in lines[::-1][:30]]}) # Last 30 events

@app.route('/api/stream')
def stream_events():
    """SSE endpoint streaming instant price movement triggers straight to UI context."""
    def event_generator():
        while True:
            try:
                # Wait up to 5 seconds for a notification payload
                message = notification_queue.get(timeout=5.0)
                yield f"data: {json.dumps({'message': message})}\n\n"
            except queue.Empty:
                # Keep client connection explicitly warm using empty string keep-alives
                yield ": keep-alive\n\n"
    return Response(event_generator(), mimetype="text/event-stream")

if __name__ == '__main__':
    # Initialize monitoring thread background execution sequence separate from web process
    t = threading.Thread(target=start_async_loop, daemon=True)
    t.start()
    # Run application endpoint listener locally
    app.run(host='127.0.0.1', port=5000, debug=False)
