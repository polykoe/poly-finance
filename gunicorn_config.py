# gunicorn_trending.py - Config for Trending Server (Port 8100)
import multiprocessing
import os

bind = f"0.0.0.0:{os.getenv('PORT', '8204')}"
backlog = 2048

# Reduce workers - fewer workers = less memory and faster startup
workers = int(os.getenv('GUNICORN_WORKERS', 2))  # Changed from 4 to 2
worker_class = 'sync'
worker_connections = 1000
timeout = 180  # Increased for initial data load
keepalive = 5
graceful_timeout = 90

max_requests = 1000
max_requests_jitter = 50

accesslog = '-'
errorlog = '-'
loglevel = 'info'
capture_output = True

# CRITICAL FIX: Preload the app to initialize cache once before forking
preload_app = True

def on_starting(server):
    print("="*80)
    print("🚀 TRENDING SERVER STARTING")
    print(f"   Workers: {workers}")
    print(f"   Timeout: {timeout}s")
    print(f"   Port: {os.getenv('PORT', '8100')}")
    print(f"   Preload: {preload_app}")
    print("="*80)

def when_ready(server):
    print("✅ Trending server ready - cache preloaded and shared across workers")

def worker_int(worker):
    print(f"⚠️  Worker {worker.pid} shutting down gracefully")

def post_worker_init(worker):
    print(f"👷 Worker {worker.pid} initialized and using shared cache")