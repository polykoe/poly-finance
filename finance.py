# ============================================
# finance_backend.py - Flask Backend for Finance Markets Only
# Save as: finance_backend.py
# Run: python finance_backend.py
# Port: 8204
# ============================================

from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import time
import threading

app = Flask(__name__)
CORS(app)

cached_finance_events = []
last_update = 0
UPDATE_INTERVAL = 300

gamma_api_url = "https://gamma-api.polymarket.com"

def get_all_finance_events():
    """Get ALL finance events by filtering for finance-related tags"""
    try:
        all_events = []
        offset = 0
        limit = 100
        
        print("\n" + "="*80)
        print("💰 FETCHING ALL FINANCE MARKETS")
        print("="*80)
        
        while True:
            url = f"{gamma_api_url}/events"
            params = {
                "active": True,
                "closed": False,
                "limit": limit,
                "offset": offset,
                "order": "volume",
                "ascending": False
            }
            
            try:
                response = requests.get(url, params=params, timeout=15)
                response.raise_for_status()
                batch = response.json()
                
                if not batch or len(batch) == 0:
                    break
                
                print(f"  📦 Fetched batch of {len(batch)} events at offset {offset}")
                all_events.extend(batch)
                
                if len(batch) < limit:
                    break
                
                offset += limit
                
                if offset >= 10000:
                    break
                
                time.sleep(0.1)
                
            except Exception as e:
                print(f"  ❌ Error fetching batch at offset {offset}: {e}")
                break
        
        print(f"\n  ✅ Total events fetched: {len(all_events)}")
        
        print("\n  🔍 Filtering for finance markets...")
        finance_events = []
        
        for event in all_events:
            tags_list = event.get('tags', [])
            tag_labels = [tag.get('label', '').lower() for tag in tags_list]
            
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
            
            is_finance = any(
                any(keyword in tag_label for keyword in finance_keywords)
                for tag_label in tag_labels
            )
            
            if is_finance:
                finance_events.append(event)
        
        print(f"  ✅ Found {len(finance_events)} finance markets")
        
        formatted_events = []
        
        for i, event in enumerate(finance_events, 1):
            tags_list = event.get('tags', [])
            markets = event.get('markets', [])
            
            volume = event.get('volume', 0)
            if isinstance(volume, str):
                try:
                    volume = float(volume)
                except (ValueError, TypeError):
                    volume = 0
            
            volume_24hr = event.get('volume24hr', 0)
            if isinstance(volume_24hr, str):
                try:
                    volume_24hr = float(volume_24hr)
                except (ValueError, TypeError):
                    volume_24hr = 0
            
            liquidity = event.get('liquidity', 0)
            if isinstance(liquidity, str):
                try:
                    liquidity = float(liquidity)
                except (ValueError, TypeError):
                    liquidity = 0
            
            formatted_event = {
                'rank': i,
                'id': event.get('id'),
                'title': event.get('title'),
                'slug': event.get('slug'),
                'link': f"https://polymarket.com/event/{event.get('slug')}",
                'image': event.get('image') or event.get('icon') or 'https://via.placeholder.com/150',
                'tags': [
                    {
                        'id': tag.get('id'),
                        'label': tag.get('label'),
                        'slug': tag.get('slug')
                    }
                    for tag in tags_list
                ],
                'tag_labels': [tag.get('label') for tag in tags_list],
                'volume': volume,
                'volume_24hr': volume_24hr,
                'liquidity': liquidity,
                'description': event.get('description', ''),
                'end_date': event.get('endDate'),
                'market_count': len(markets),
                'category': event.get('category'),
                'markets': markets[:5] if markets else []
            }
            
            formatted_events.append(formatted_event)
        
        print(f"\n  ✅ Formatted {len(formatted_events)} finance events")
        print("="*80 + "\n")
        
        return formatted_events
    
    except Exception as e:
        print(f"❌ Error fetching finance events: {e}")
        return []


def update_finance_events_background():
    global cached_finance_events, last_update
    
    while True:
        try:
            print("\n🔄 Background update: Fetching latest finance markets...")
            events = get_all_finance_events()
            
            if events:
                cached_finance_events = events
                last_update = time.time()
                print(f"✅ Updated {len(events)} finance markets at {time.ctime()}")
            else:
                print("⚠️  No finance events fetched, keeping cached data")
        
        except Exception as e:
            print(f"❌ Error in background update: {e}")
        
        time.sleep(UPDATE_INTERVAL)


@app.route('/api/finance', methods=['GET'])
def get_finance_events():
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
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', 100))
        
        if offset < 0:
            offset = 0
        if limit < 1 or limit > 500:
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
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': time.time()
        }), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'service': 'Finance Markets Backend',
        'finance_events_count': len(cached_finance_events),
        'last_update': last_update,
        'last_update_time': time.ctime(last_update) if last_update else 'Never',
        'timestamp': time.time()
    })


def initialize_app():
    global cached_finance_events, last_update
    
    print("\n" + "="*80)
    print("🚀 INITIALIZING FINANCE BACKEND SERVER")
    print("="*80)
    
    events = get_all_finance_events()
    
    if events:
        cached_finance_events = events
        last_update = time.time()
        print(f"\n✅ Successfully loaded {len(events)} finance markets!")
    else:
        print("\n⚠️  Failed to load initial finance data")
    
    print("\n🔄 Starting background update thread...")
    update_thread = threading.Thread(target=update_finance_events_background, daemon=True)
    update_thread.start()
    print("✅ Background updates started")
    print("="*80 + "\n")


if __name__ == '__main__':
    initialize_app()
    
    print("="*80)
    print("💰 FINANCE MARKETS BACKEND API SERVER")
    print("="*80)
    print("\n📋 Available Endpoints:")
    print("  GET  /api/finance               - Get all finance events")
    print("  GET  /api/finance/paginated     - Get paginated finance markets")
    print("  GET  /api/health                - Health check")
    print("\n🌐 Running on: http://localhost:8204")
    print("="*80 + "\n")
    
    app.run(debug=True, host='0.0.0.0', port=8204)