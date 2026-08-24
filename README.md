# meta-ads-cloud

META Ads automation suite (S1 weekly, S2 competitor, S4 VSL generator, control console poll, creative planner) packaged as a FastAPI service for Zeabur.

## Endpoints

All endpoints require header `x-api-key: <API_KEY>`.

- `POST /run/s1` — kick off S1 weekly report (background)
- `POST /run/s2` — kick off S2 competitor monitoring (background)
- `POST /run/console-poll` — single console poll cycle (sync, ~5-10 min)
- `GET /health` — liveness check

## Required env vars

```
API_KEY
IM_APP_ID, IM_APP_SECRET
BT_APP_ID, BT_APP_SECRET
PK_TOKEN, FL_TOKEN
```

## Optional AI mode

The service starts safely without these variables and uses deterministic templates. Before enabling the model, configure a durable shared budget that survives container restarts.

```
META_ADS_DEEPSEEK_API_KEY
META_ADS_MODEL_DAILY_LIMIT=12
META_ADS_MODEL_FAILURE_THRESHOLD=2
```

## v1 vs v2

v1 (current): SKIP_ADLIB_IMAGES=1 — Ad Library HD images not embedded in docx (cloud Chromium can't render video posters reliably; Ad Library API approval pending).

v2 (after FB approval): SKIP_ADLIB_IMAGES=0 + switch to Graph API `/ads_archive` endpoint, drop Playwright.
