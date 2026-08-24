"""Persistent soft budget and failure circuit for Meta Ads model calls."""
from __future__ import annotations

import datetime
import json
import os
from pathlib import Path
import threading


_STATE_LOCK = threading.Lock()


class ModelCallGuard:
    def __init__(self, state_path: Path, *, daily_limit: int, failure_threshold: int):
        self.state_path = Path(state_path)
        self.daily_limit = max(0, int(daily_limit))
        self.failure_threshold = max(1, int(failure_threshold))

    def _today(self) -> str:
        return datetime.datetime.now(datetime.timezone.utc).date().isoformat()

    def _load(self) -> dict:
        base = {"date": self._today(), "calls": 0, "consecutive_failures": 0}
        try:
            loaded = json.loads(self.state_path.read_text(encoding="utf-8"))
            if loaded.get("date") == base["date"]:
                base.update(loaded)
        except (FileNotFoundError, OSError, ValueError, TypeError):
            pass
        return base

    def _save(self, state: dict) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        temp_path.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")
        temp_path.replace(self.state_path)

    def reserve(self) -> tuple[bool, str]:
        with _STATE_LOCK:
            state = self._load()
            if int(state["consecutive_failures"]) >= self.failure_threshold:
                return False, "circuit_open"
            if int(state["calls"]) >= self.daily_limit:
                return False, "daily_budget_exhausted"
            state["calls"] = int(state["calls"]) + 1
            self._save(state)
            return True, "ok"

    def record_success(self) -> None:
        with _STATE_LOCK:
            state = self._load()
            state["consecutive_failures"] = 0
            self._save(state)

    def record_failure(self) -> None:
        with _STATE_LOCK:
            state = self._load()
            state["consecutive_failures"] = int(state["consecutive_failures"]) + 1
            self._save(state)

    def snapshot(self) -> dict:
        with _STATE_LOCK:
            state = self._load()
        return {
            "date": state["date"],
            "calls": int(state["calls"]),
            "daily_limit": self.daily_limit,
            "consecutive_failures": int(state["consecutive_failures"]),
            "failure_threshold": self.failure_threshold,
            "circuit_open": int(state["consecutive_failures"]) >= self.failure_threshold,
        }


META_ADS_MODEL_GUARD = ModelCallGuard(
    Path(os.environ.get("META_ADS_MODEL_STATE_PATH", "/tmp/meta_ads_model_budget.json")),
    daily_limit=int(os.environ.get("META_ADS_MODEL_DAILY_LIMIT", "12")),
    failure_threshold=int(os.environ.get("META_ADS_MODEL_FAILURE_THRESHOLD", "2")),
)
