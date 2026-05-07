"""Auto-publish: commits the SQLite insights DB to git and pushes.
Lets the team see the latest agent output via `git pull` even when they
cannot reach localhost. Runs as a background loop inside the FastAPI app.

Only commits ga4_intel.db (small, contains insights/health/briefings/agent activity).
Skips ga4_warehouse.duckdb (large raw GA4 dump - regeneratable via sync).
"""
import logging
import subprocess
from pathlib import Path
from datetime import datetime

from config import ROOT, SQLITE_DB_PATH

logger = logging.getLogger("ga4.publish")

PUBLISHABLE_FILES = [
    str(SQLITE_DB_PATH.relative_to(ROOT)),
]


def _run(cmd: list[str], cwd: Path = ROOT, check: bool = True) -> tuple[int, str, str]:
    """Run a command, return (returncode, stdout, stderr)."""
    try:
        r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=120)
        if check and r.returncode != 0:
            logger.warning(f"  cmd {' '.join(cmd)} exited {r.returncode}: {r.stderr.strip()}")
        return r.returncode, r.stdout, r.stderr
    except Exception as e:
        logger.warning(f"  cmd {' '.join(cmd)} raised: {e}")
        return 1, "", str(e)


def _has_remote() -> bool:
    rc, out, _ = _run(["git", "remote"], check=False)
    return rc == 0 and "origin" in out


def has_changes() -> bool:
    rc, out, _ = _run(["git", "status", "--porcelain", "--"] + PUBLISHABLE_FILES, check=False)
    return rc == 0 and out.strip() != ""


def publish() -> dict:
    """Stage publishable files, commit if anything changed, push to origin."""
    if not (ROOT / ".git").exists():
        return {"status": "skipped", "reason": "not a git repo"}

    if not _has_remote():
        return {"status": "skipped", "reason": "no remote"}

    if not has_changes():
        return {"status": "noop", "reason": "no changes"}

    # Stage only the safe files
    rc, _, err = _run(["git", "add"] + PUBLISHABLE_FILES, check=False)
    if rc != 0:
        return {"status": "error", "step": "add", "error": err}

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    rc, _, err = _run([
        "git", "commit",
        "-m", f"data: agent insights snapshot {ts}",
        "--no-verify",
    ], check=False)
    if rc != 0:
        # Commit can fail if nothing to commit (race) — treat as noop
        if "nothing to commit" in err.lower() or "no changes" in err.lower():
            return {"status": "noop"}
        return {"status": "error", "step": "commit", "error": err}

    rc, _, err = _run(["git", "push", "origin", "main"], check=False)
    if rc != 0:
        return {"status": "error", "step": "push", "error": err}

    return {"status": "ok", "ts": ts}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(publish())
