"""FastAPI orchestrator for META Ads automation on Zeabur.

Endpoints (all require x-api-key header):
  POST /run/s1            — kick off S1 weekly report (background)
  POST /run/s2            — kick off S2 competitor monitoring (background)
  POST /run/console-poll  — single console poll cycle (synchronous, ~5-10 min)
  GET  /health            — liveness check
"""
import os, sys, traceback, threading, time
from fastapi import FastAPI, HTTPException, Header

import cloud_config  # validates required env vars on import

app = FastAPI(title='meta-ads-cloud', version='1.0')


def _auth(key: str | None):
    if not key or key != cloud_config.API_KEY:
        raise HTTPException(403, 'invalid api key')


def _spawn(target_name: str, fn, *args, **kwargs):
    """Run fn in a daemon thread, log any exception."""
    def _wrap():
        try:
            print(f'[{target_name}] starting', flush=True)
            fn(*args, **kwargs)
            print(f'[{target_name}] done', flush=True)
        except Exception:
            traceback.print_exc()
            print(f'[{target_name}] failed', flush=True)
    t = threading.Thread(target=_wrap, name=target_name, daemon=True)
    t.start()
    return t


@app.get('/health')
def health():
    return {'ok': True, 'ts': time.time()}


@app.post('/run/s1')
def run_s1(x_api_key: str = Header(None), fake_today: str | None = None):
    _auth(x_api_key)
    if fake_today:
        os.environ['META_FAKE_TODAY'] = fake_today
    from meta_ads_s1_weekly import main as s1_main
    _spawn('s1', s1_main)
    return {'started': 's1', 'fake_today': fake_today}


@app.post('/run/s2')
def run_s2(x_api_key: str = Header(None), fake_today: str | None = None):
    _auth(x_api_key)
    if fake_today:
        os.environ['META_FAKE_TODAY'] = fake_today
    from meta_ads_s2_weekly import main as s2_main
    _spawn('s2', s2_main)
    return {'started': 's2', 'fake_today': fake_today}


@app.post('/run/console-poll')
def run_console_poll(x_api_key: str = Header(None)):
    """Synchronous: console poll is short and we want the caller to know status."""
    _auth(x_api_key)
    from meta_ads_console_poll import main as poll_main
    try:
        poll_main()
        return {'ok': True}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f'console-poll failed: {e}')


@app.get('/')
def root():
    return {
        'service': 'meta-ads-cloud',
        'endpoints': ['/health', '/run/s1', '/run/s2', '/run/console-poll'],
        'skip_adlib_images': cloud_config.SKIP_ADLIB_IMAGES,
    }
