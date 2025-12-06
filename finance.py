# finance_backend.py - Optimized Finance Markets Backend with Proper Market Formatting
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import time
import threading
import gzip
import os
import json
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)
CORS(app)

# Add response compression
@app.after_request
def compress_response(response):
    accept_encoding = request.headers.get('Accept-Encoding', '')
    if 'gzip' not in accept_encoding.lower():
        return response
    if response.status_code < 200 or response.status_code >= 300:
        return response
    if len(response.data) < 1000:
        return response
    gzip_buffer = gzip.compress(response.data)
    response.data = gzip_buffer
    response.headers['Content-Encoding'] = 'gzip'
    response.headers['Content-Length'] = len(response.data)
    return response

# Global cache
cached_finance_events = []
last_update = 0
UPDATE_INTERVAL = 300
update_lock = threading.Lock()
is_updating = False
initialized = False
init_lock = threading.Lock()

gamma_api_url = "https://gamma-api.polymarket.com"

def get_all_finance_events():
    """Fetch all finance events with parallel requests and proper market formatting"""
    try:
        print(f"\n[PID {os.getpid()}] 💰 Fetching finance markets...", flush=True)
        start_time = time.time()
        
        all_events = []
        limit = 100
        offsets = list(range(0, 5000, limit))
        
        def fetch_batch(offset):
            params = {
                "active": True,
                "closed": False,
                "limit": limit,
                "offset": offset,
                "order": "volume",
                "ascending": False
            }
            try:
                response = requests.get(f"{gamma_api_url}/events", params=params, timeout=15)
                response.raise_for_status()
                return response.json() if response.json() else []
            except Exception as e:
                print(f"[PID {os.getpid()}] ❌ Error at offset {offset}: {e}", flush=True)
                return []
        
        # Parallel fetch with ordering preservation
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_offset = {executor.submit(fetch_batch, offset): offset for offset in offsets}
            results = {}
            for future in future_to_offset:
                offset = future_to_offset[future]
                batch = future.result()
                if batch:
                    results[offset] = batch
        
        # Reconstruct in order
        for offset in sorted(results.keys()):
            all_events.extend(results[offset])
            if len(results[offset]) < limit:
                break
        
        print(f"[PID {os.getpid()}] 📦 Fetched {len(all_events)} total events", flush=True)
        
        # Filter for finance
        finance_keywords = ['finance', 'financial', 'stock', 'stocks', 'market',
                          'trading', 'investment', 'investor', 'wall street',
                          'banking', 'bank', 'credit', 'loan', 'mortgage',
                          'bond', 'securities', 'etf', 'mutual fund', 'hedge fund',
                          'forex', 'currency', 'exchange', 'commodity', 'gold',
                          'silver', 'oil', 'futures', 'options', 'derivatives',
                          'portfolio', 'dividend', 'yield', 'interest rate',
                          'fed', 'federal reserve', 'treasury', 'debt', 'inflation',
                          'sp500', 's&p', 'dow', 'nasdaq', 'nyse', 'ipo',
                          'merger', 'acquisition', 'earnings', 'revenue', 'profit']
        
        finance_events = []
        for event in all_events:
            tags_list = event.get('tags', [])
            tag_labels = [tag.get('label', '').lower() for tag in tags_list]
            
            is_finance = any(
                any(keyword in tag_label for keyword in finance_keywords)
                for tag_label in tag_labels
            )
            
            if is_finance:
                finance_events.append(event)
        
        # Format events with proper market structure
        formatted_events = []
        for i, event in enumerate(finance_events, 1):
            tags_list = event.get('tags', [])
            markets = event.get('markets', [])
            
            volume = float(event.get('volume', 0)) if isinstance(event.get('volume'), (int, float, str)) else 0
            volume_24hr = float(event.get('volume24hr', 0)) if isinstance(event.get('volume24hr'), (int, float, str)) else 0
            liquidity = float(event.get('liquidity', 0)) if isinstance(event.get('liquidity'), (int, float, str)) else 0
            
            # Filter event if both volume and liquidity are 0
            if volume == 0 and liquidity == 0:
                continue
            
            # Process markets with their outcome prices
            formatted_markets = []
            for market in markets:
                # Get market liquidity and volume
                market_liquidity = float(market.get('liquidity', 0)) if isinstance(market.get('liquidity'), (int, float, str)) else 0
                market_volume = float(market.get('volume', 0)) if isinstance(market.get('volume'), (int, float, str)) else 0
                
                # Filter market if both are 0
                if market_volume == 0 and market_liquidity == 0:
                    continue
                
                # Parse outcome prices
                outcome_prices_raw = market.get('outcomePrices', [])
                outcome_prices = []
                
                if isinstance(outcome_prices_raw, str):
                    try:
                        outcome_prices = json.loads(outcome_prices_raw)
                    except json.JSONDecodeError:
                        outcome_prices = []
                elif isinstance(outcome_prices_raw, list):
                    outcome_prices = outcome_prices_raw
                
                # Get market status
                market_closed = market.get('closed', False)
                market_active = market.get('active', True)
                is_live = market_active and not market_closed
                
                formatted_market = {
                    'groupItemTitle': market.get('groupItemTitle', ''),
                    'image': market.get('image') or market.get('icon') or event.get('image') or event.get('icon') or 'https://via.placeholder.com/40',
                    'outcomePrices': outcome_prices,
                    'outcomes': market.get('outcomes', []),
                    'is_live': is_live,
                    'closed': market_closed,
                    'active': market_active,
                    'liquidity': market_liquidity,
                    'volume': market_volume
                }
                formatted_markets.append(formatted_market)
            
            # Skip event if no valid markets after filtering
            if len(formatted_markets) == 0:
                continue
            
            formatted_events.append({
                'rank': len(formatted_events) + 1,
                'id': event.get('id'),
                'title': event.get('title'),
                'slug': event.get('slug'),
                'link': f"https://polymarket.com/event/{event.get('slug')}",
                'image': event.get('image') or event.get('icon') or 'https://via.placeholder.com/150',
                'tags': [{'id': t.get('id'), 'label': t.get('label'), 'slug': t.get('slug')} for t in tags_list],
                'tag_labels': [t.get('label') for t in tags_list],
                'volume': volume,
                'volume_24hr': volume_24hr,
                'liquidity': liquidity,
                'description': event.get('description', ''),
                'end_date': event.get('endDate'),
                'market_count': len(formatted_markets),
                'category': event.get('category'),
                'markets': formatted_markets,
                'is_live': event.get('active', True) and not event.get('closed', False)
            })
        
        elapsed = time.time() - start_time
        print(f"[PID {os.getpid()}] ✅ Found {len(formatted_events)} finance markets in {elapsed:.1f}s", flush=True)
        return formatted_events
    
    except Exception as e:
        print(f"[PID {os.getpid()}] ❌ Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return []

def update_finance_events():
    """Update cache without blocking"""
    global cached_finance_events, last_update, is_updating
    
    with update_lock:
        if is_updating:
            print(f"[PID {os.getpid()}] ⏭️  Already updating, skipping", flush=True)
            return
        is_updating = True
    
    try:
        events = get_all_finance_events()
        if events:
            with update_lock:
                cached_finance_events = events
                last_update = time.time()
                is_updating = False
            print(f"[PID {os.getpid()}] ✅ Cache updated: {len(events)} events", flush=True)
    except Exception as e:
        print(f"[PID {os.getpid()}] ❌ Update error: {e}", flush=True)
        with update_lock:
            is_updating = False

def background_updater():
    """Background thread for cache updates"""
    print(f"[PID {os.getpid()}] 🔄 Background updater started", flush=True)
    while True:
        time.sleep(UPDATE_INTERVAL)
        print(f"[PID {os.getpid()}] 🔄 Background refresh triggered", flush=True)
        update_finance_events()

def ensure_initialized():
    """Initialize worker on first request (lazy initialization)"""
    global initialized, cached_finance_events, last_update
    
    if initialized:
        return
    
    with init_lock:
        if initialized:
            return
        
        print(f"\n{'='*60}", flush=True)
        print(f"🚀 INITIALIZING FINANCE BACKEND (PID: {os.getpid()})", flush=True)
        print(f"{'='*60}", flush=True)
        
        update_finance_events()
        
        bg_thread = threading.Thread(target=background_updater, daemon=True, name=f"updater-{os.getpid()}")
        bg_thread.start()
        
        initialized = True
        print(f"✅ Worker {os.getpid()} ready with {len(cached_finance_events)} events", flush=True)
        print(f"{'='*60}\n", flush=True)

@app.before_request
def before_request():
    """Initialize on first request"""
    ensure_initialized()

@app.route('/api/finance', methods=['GET'])
def get_finance_events():
    if not initialized:
        return jsonify({
            'success': True,
            'data': [],
            'count': 0,
            'message': 'Loading events, please try again in a moment...',
            'initializing': True
        })
    
    return jsonify({
        'success': True,
        'data': cached_finance_events,
        'count': len(cached_finance_events),
        'last_update': last_update,
        'timestamp': time.time()
    })

@app.route('/api/finance/paginated', methods=['GET'])
def get_paginated_finance_markets():
    try:
        if not initialized:
            return jsonify({
                'success': True,
                'data': [],
                'message': 'Loading events...',
                'initializing': True
            })
        
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', 100))
        
        if offset < 0:
            offset = 0
        if limit < 1 or limit > 200:
            limit = 100
        
        start_idx = offset
        end_idx = offset + limit
        
        paginated_events = cached_finance_events[start_idx:end_idx]
        has_more = end_idx < len(cached_finance_events)
        
        return jsonify({
            'success': True,
            'data': paginated_events,
            'count': len(paginated_events),
            'offset': offset,
            'limit': limit,
            'total': len(cached_finance_events),
            'has_more': has_more,
            'timestamp': time.time()
        })
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    with update_lock:
        cache_age = time.time() - last_update if last_update else 0
        is_updating_now = is_updating
        cache_size = len(cached_finance_events)
    
    return jsonify({
        'success': True,
        'status': 'healthy' if initialized else 'initializing',
        'initialized': initialized,
        'service': 'Finance Markets Backend',
        'events_count': cache_size,
        'last_update': last_update,
        'cache_age_seconds': cache_age,
        'is_updating': is_updating_now,
        'worker_pid': os.getpid()
    })

@app.route('/api/finance/refresh', methods=['POST'])
def force_refresh():
    """Force refresh finance events"""
    thread = threading.Thread(target=update_finance_events, daemon=True)
    thread.start()
    return jsonify({'success': True, 'message': 'Cache refresh initiated'})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8204)