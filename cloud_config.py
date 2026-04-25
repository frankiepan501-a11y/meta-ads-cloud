"""Centralized configuration loaded from environment variables.

All secrets and environment-specific values come from env. No hardcoded credentials.
Local-dev fallback: set HTTP_PROXY=http://127.0.0.1:7890 plus the credential vars.
"""
import os
import urllib.request


def _required(key: str) -> str:
    v = os.environ.get(key)
    if not v:
        raise RuntimeError(f'Required env var {key!r} is not set')
    return v


def _make_proxy() -> urllib.request.ProxyHandler:
    proxy_url = (os.environ.get('HTTP_PROXY') or '').strip()
    if proxy_url:
        return urllib.request.ProxyHandler({'http': proxy_url, 'https': proxy_url})
    return urllib.request.ProxyHandler({})


PROXY = _make_proxy()
PLAYWRIGHT_PROXY = (os.environ.get('PLAYWRIGHT_PROXY') or '').strip() or None

# Feishu (Lark)
IM_APP_ID = _required('IM_APP_ID')
IM_APP_SECRET = _required('IM_APP_SECRET')
BT_APP_ID = _required('BT_APP_ID')
BT_APP_SECRET = _required('BT_APP_SECRET')

# AI
DEEPSEEK_KEY = _required('DEEPSEEK_KEY')

# Facebook Ads
PK_TOKEN = _required('PK_TOKEN')
FL_TOKEN = _required('FL_TOKEN')
PK_ACCOUNT = os.environ.get('PK_ACCOUNT', 'act_1498442934673297')
FL_ACCOUNT = os.environ.get('FL_ACCOUNT', 'act_1705425610151698')

# Feishu users / wiki anchors
BOSS_OPEN_ID = os.environ.get('BOSS_OPEN_ID', 'ou_629ce01f4bc31de078e10fcb038dbf78')
WIKI_SPACE_ID = os.environ.get('WIKI_SPACE_ID', '7271064748154650628')
WIKI_PARENT_NODE_S1 = os.environ.get('WIKI_PARENT_NODE_S1', 'DLlkwhBymiHJfjk5mC6cuqNnnDy')
WIKI_PARENT_NODE_S2 = os.environ.get('WIKI_PARENT_NODE_S2', 'Z41Awia1yiSTKAkUH8ScmBSHn6b')
WIKI_PARENT_NODE_S4 = os.environ.get('WIKI_PARENT_NODE_S4', 'MDQewVLuAiDyPmkBmf3cHz45nGh')

# Bitable apps
META_ADS_CONSOLE_APP = os.environ.get('META_ADS_CONSOLE_APP', 'DLD5b93HLaWHVxs7NFjcisdgnuE')

# v1 toggle: skip embedding Ad Library HD images in docx (cloud headless can't render
# video posters reliably; awaiting Ad Library API approval to switch to Graph API).
SKIP_ADLIB_IMAGES = (os.environ.get('SKIP_ADLIB_IMAGES', '1') == '1')

# FastAPI auth
API_KEY = _required('API_KEY')
