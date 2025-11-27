# finance_backend.py - Standalone Finance Markets Backend
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import time
import threading
import gzip
import os
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

gamma_api_url = "https://gamma-api.polymarket.com"

def get_all_finance_events():
    """Fetch all finance events with parallel requests and proper ordering"""
    try:
        print(f"\n💰 Fetching finance markets...", flush=True)
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
                print(f"❌ Error at offset {offset}: {e}", flush=True)
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
        
        print(f"📦 Fetched {len(all_events)} total events", flush=True)
        
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
        
        # Format events
        formatted_events = []
        for i, event in enumerate(finance_events, 1):
            tags_list = event.get('tags', [])
            markets = event.get('markets', [])
            
            volume = float(event.get('volume', 0)) if isinstance(event.get('volume'), (int, float, str)) else 0
            volume_24hr = float(event.get('volume24hr', 0)) if isinstance(event.get('volume24hr'), (int, float, str)) else 0
            liquidity = float(event.get('liquidity', 0)) if isinstance(event.get('liquidity'), (int, float, str)) else 0
            
            formatted_events.append({
                'rank': i,
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
                'market_count': len(markets),
                'category': event.get('category'),
                'markets': markets
            })
        
        elapsed = time.time() - start_time
        print(f"✅ Found {len(formatted_events)} finance markets in {elapsed:.1f}s", flush=True)
        return formatted_events
    
    except Exception as e:
        print(f"❌ Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return []

def update_finance_events():
    """Update cache without blocking"""
    global cached_finance_events, last_update, is_updating
    
    with update_lock:
        if is_updating:
            print("⏭️  Already updating, skipping", flush=True)
            return
        is_updating = True
    
    try:
        events = get_all_finance_events()
        if events:
            with update_lock:
                cached_finance_events = events
                last_update = time.time()
                is_updating = False
            print(f"✅ Cache updated: {len(events)} events", flush=True)
    except Exception as e:
        print(f"❌ Update error: {e}", flush=True)
        with update_lock:
            is_updating = False

def background_updater():
    """Background thread for cache updates"""
    print("🔄 Background updater started", flush=True)
    while True:
        time.sleep(UPDATE_INTERVAL)
        print("🔄 Background refresh triggered", flush=True)
        update_finance_events()

def initialize_app():
    """Initialize the application cache and background thread"""
    global initialized
    
    if initialized:
        return
    
    print(f"\n{'='*60}", flush=True)
    print("🚀 INITIALIZING FINANCE BACKEND", flush=True)
    print(f"{'='*60}", flush=True)
    
    # Initial data load
    update_finance_events()
    
    # Start background thread
    bg_thread = threading.Thread(target=background_updater, daemon=True, name="cache-updater")
    bg_thread.start()
    
    initialized = True
    print(f"✅ Server ready with {len(cached_finance_events)} events", flush=True)
    print(f"{'='*60}\n", flush=True)

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
        include_markets = request.args.get('include_markets', 'false').lower() == 'true'
        
        if offset < 0:
            offset = 0
        if limit < 1 or limit > 200:
            limit = 100
        
        start_idx = offset
        end_idx = offset + limit
        
        paginated_events = cached_finance_events[start_idx:end_idx]
        has_more = end_idx < len(cached_finance_events)
        
        # Strip markets by default for performance
        if not include_markets:
            paginated_events = [
                {
                    'rank': e['rank'],
                    'id': e['id'],
                    'title': e['title'],
                    'slug': e['slug'],
                    'link': e['link'],
                    'image': e['image'],
                    'tags': e['tags'],
                    'tag_labels': e['tag_labels'],
                    'volume': e['volume'],
                    'volume_24hr': e['volume_24hr'],
                    'liquidity': e['liquidity'],
                    'market_count': e['market_count'],
                    'category': e.get('category'),
                    'description': e.get('description', '')[:200] if e.get('description') else ''
                }
                for e in paginated_events
            ]
        
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
        'is_updating': is_updating_now
    })

@app.route('/api/finance/refresh', methods=['POST'])
def force_refresh():
    """Force refresh finance events"""
    thread = threading.Thread(target=update_finance_events, daemon=True)
    thread.start()
    return jsonify({'success': True, 'message': 'Cache refresh initiated'})

if __name__ == '__main__':
    # Initialize before starting the server
    initialize_app()
    
    # Run with Flask's built-in server
    # For production, consider using waitress or another WSGI server
    port = int(os.getenv('PORT', 8204))
    
    print(f"\n🌐 Starting server on http://0.0.0.0:{port}")
    print("Press CTRL+C to quit\n")
    
    app.run(
        host='0.0.0.0',
        port=port,
        debug=False,  # Set to False for production
        threaded=True,  # Enable threading for concurrent requests
        use_reloader=False  # Disable reloader to prevent double initialization
    )