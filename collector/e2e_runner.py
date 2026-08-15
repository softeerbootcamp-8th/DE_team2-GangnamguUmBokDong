import asyncio
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from config.loader import load

KST = ZoneInfo("Asia/Seoul")
END_TIME = datetime(2026, 8, 15, 11, 30, tzinfo=KST)

def get_window_start(interval_seconds: int) -> str:
    now = datetime.now(KST)
    ts = int(now.timestamp())
    window_ts = ts - (ts % interval_seconds)
    return datetime.fromtimestamp(window_ts, tz=KST).isoformat()

async def run_source(source_id: str, interval_seconds: int):
    last_window = None
    while datetime.now(KST) < END_TIME:
        window_start = get_window_start(interval_seconds)
        if window_start != last_window:
            print(f"[{source_id}] Starting collection for {window_start}")
            cmd = [
                "uv", "run", "python", "main.py",
                "--source", source_id,
                "--window-start", window_start
            ]
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=sys.stdout,
                stderr=sys.stderr
            )
            await process.wait()
            last_window = window_start
        
        # Sleep a short duration before checking for the next window
        await asyncio.sleep(10)

async def main():
    print(f"Starting E2E runner. Will run until {END_TIME}")
    tasks = []
    sources_dir = Path("sources")
    for path in sources_dir.glob("*.yaml"):
        source_id = path.stem
        config = load(source_id)
        interval_seconds = int(config.schedule.interval.total_seconds())
        tasks.append(asyncio.create_task(run_source(source_id, interval_seconds)))
    
    await asyncio.gather(*tasks)
    print("Reached END_TIME. E2E runner exiting.")

if __name__ == "__main__":
    asyncio.run(main())
