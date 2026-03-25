import json
import subprocess
import sys
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException

router = APIRouter(prefix="/backfill", tags=["backfill"])

STATE_FILE = Path("data/backfill_state.json")


@router.get("/status")
async def get_status():
    if not STATE_FILE.exists():
        return {"sources": {}, "updated_at": None, "initialized": False}
    return json.loads(STATE_FILE.read_text())


@router.post("/run/{source}")
async def trigger_run(source: str, background_tasks: BackgroundTasks):
    valid = {"polygon", "alpha_vantage", "coingecko", "yahoo", "all"}
    if source not in valid:
        raise HTTPException(status_code=400, detail=f"Unknown source. Valid: {valid}")
    background_tasks.add_task(_run_backfill, source)
    return {"status": "started", "source": source}


@router.post("/reset")
async def reset_state():
    if STATE_FILE.exists():
        STATE_FILE.unlink()
    return {"status": "reset"}


def _run_backfill(source: str):
    args = [sys.executable, "scripts/backfill_manager.py", "--run"]
    if source != "all":
        args += ["--source", source]
    subprocess.Popen(args)
