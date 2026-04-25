#!/bin/bash
set -e

# Start Xvfb so Playwright non-headless can render videos (used by S2 ad library scrape).
# Headless mode also still works; Xvfb is a safety net for future toggle.
Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp >/tmp/xvfb.log 2>&1 &
sleep 1
export DISPLAY=:99

exec uvicorn app:app --host 0.0.0.0 --port 8080
