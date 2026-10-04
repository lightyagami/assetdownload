#!/usr/bin/env python3
"""Aether Gazer offline asset downloader (resumable, size-verified).

Requires: pip install requests
"""
import argparse
import concurrent.futures
import json
import shutil
import sys
import threading
import time
from pathlib import Path

import requests

STREAMING_ASSETS = Path("/AetherGazerLauncher/AetherGazer/AetherGazer_Data/StreamingAssets")
CDN_BASE_URL = "https://skzy-us-download-ali.yongshigame.com/pc/resources"
USER_AGENT = "AetherGazer/293 (PC Windows)"
CHUNK_SIZE = 256 * 1024
MAX_ATTEMPTS = 4
PERMANENT_STATUS = {400, 403, 404, 410}  # don't retry these

_local = threading.local()
_stop = threading.Event()


class PermanentError(Exception):
    pass


def get_session():
    s = getattr(_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers["User-Agent"] = USER_AGENT
        adapter = requests.adapters.HTTPAdapter(pool_connections=1, pool_maxsize=1)
        s.mount("https://", adapter)
        _local.session = s
    return s


def parse_manifest(manifest: Path, base_dir: Path, label: str):
    """Yield (hash, size, target_path, label) for entries that are missing or wrong size."""
    with open(manifest, "r", encoding="utf-8") as f:
        data = json.load(f)
    for item in data.get("assetHashList", []):
        parts = item.split("|")
        if len(parts) < 2:
            continue
        h = parts[1]
        try:
            size = int(parts[2]) if len(parts) >= 3 else 0
        except ValueError:
            size = 0
        target = base_dir / h[0] / h[1] / f"{h}.ys"
        if not target.exists() or (size > 0 and target.stat().st_size != size):
            yield (h, size, target, f"{label}:{parts[0]}")


def collect_missing_tasks(voice_langs):
    tasks = {}  # dedupe by target path

    win_dir = STREAMING_ASSETS / "Windows"
    win_info = win_dir / "AssetHash_Info.bytes"
    if win_info.exists():
        for t in parse_manifest(win_info, win_dir, "win"):
            tasks[t[2]] = t
    else:
        print(f"[warn] missing {win_info}")

    for lang in voice_langs:
        vdir = STREAMING_ASSETS / "Voice" / lang
        manifests = sorted(vdir.glob(f"voice_hash_{lang}_*_temp.bytes"))
        if not manifests:
            print(f"[warn] no temp voice manifest found for '{lang}' in {vdir}")
        for m in manifests:
            for t in parse_manifest(m, vdir, f"voice-{lang}"):
                tasks[t[2]] = t

    return list(tasks.values())


def download_file(task):
    h, expected_size, target, label = task
    url = f"{CDN_BASE_URL}/{h}.ys"
    tmp = target.with_suffix(".ys.tmp")
    target.parent.mkdir(parents=True, exist_ok=True)
    last_err = None

    for attempt in range(MAX_ATTEMPTS):
        if _stop.is_set():
            return False, 0, f"Cancelled {h} ({label})"
        try:
            with get_session().get(url, stream=True, timeout=(10, 30)) as resp:
                if resp.status_code in PERMANENT_STATUS:
                    raise PermanentError(f"HTTP {resp.status_code}")
                resp.raise_for_status()
                with open(tmp, "wb") as out:
                    for chunk in resp.iter_content(CHUNK_SIZE):
                        if _stop.is_set():
                            raise KeyboardInterrupt
                        out.write(chunk)

            got = tmp.stat().st_size
            if expected_size > 0 and got != expected_size:
                raise IOError(f"size mismatch: got {got}, expected {expected_size}")

            tmp.replace(target)
            return True, got, None
        except PermanentError as e:
            last_err = e
            break
        except KeyboardInterrupt:
            last_err = "cancelled"
            break
        except Exception as e:
            last_err = e
            time.sleep(1 + attempt * 2)
        finally:
            if tmp.exists() and not target.exists():
                try:
                    tmp.unlink()
                except OSError:
                    pass

    return False, 0, f"{h}\t{label}\t{last_err}"


def finalize_voice(voice_langs):
    for lang in voice_langs:
        vdir = STREAMING_ASSETS / "Voice" / lang
        for temp in vdir.glob(f"voice_hash_{lang}_*_temp.bytes"):
            final = temp.with_name(temp.name.replace("_temp", ""))
            if not final.exists():
                shutil.copy2(temp, final)
                print(f"Created {final.name} to activate '{lang}' voice pack")


def fmt_eta(seconds):
    if seconds <= 0 or seconds == float("inf"):
        return "--"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workers", type=int, default=24)
    ap.add_argument("--voice", nargs="*", default=["en"], help="voice languages (default: en)")
    ap.add_argument("--failed-file", default="failed.txt")
    ap.add_argument("--force-finalize", action="store_true",
                    help="create final voice manifest even if some downloads failed")
    args = ap.parse_args()

    print("=== Aether Gazer Offline Asset Downloader ===")
    tasks = collect_missing_tasks(args.voice)
    total_files = len(tasks)
    total_bytes = sum(t[1] for t in tasks)
    print(f"Missing files: {total_files}")
    print(f"Download size: {total_bytes / 1024**3:.2f} GB")

    if total_files == 0:
        print("Everything already downloaded and verified.")
        finalize_voice(args.voice)
        return 0

    done = 0
    done_bytes = 0
    failed = []
    start = last_print = time.time()

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=args.workers)
    try:
        futures = [executor.submit(download_file, t) for t in tasks]
        for fut in concurrent.futures.as_completed(futures):
            ok, nbytes, err = fut.result()
            if ok:
                done += 1
                done_bytes += nbytes
            else:
                failed.append(err)

            now = time.time()
            finished = done + len(failed)
            if now - last_print >= 3.0 or finished == total_files:
                elapsed = now - start
                speed = done_bytes / 1024**2 / elapsed if elapsed > 0 else 0
                remaining = total_bytes - done_bytes
                eta = remaining / (done_bytes / elapsed) if done_bytes else 0
                print(f"[{finished}/{total_files} | {finished / total_files * 100:.1f}%] "
                      f"{done_bytes / 1024**3:.2f}/{total_bytes / 1024**3:.2f} GB "
                      f"({speed:.1f} MB/s, ETA {fmt_eta(eta)}) - Failed: {len(failed)}",
                      flush=True)
                last_print = now
        executor.shutdown(wait=True)
    except KeyboardInterrupt:
        print("\nInterrupted, stopping workers (re-run to resume)...")
        _stop.set()
        executor.shutdown(wait=False, cancel_futures=True)
        return 130

    print("\nDownload pass complete.")
    if failed:
        Path(args.failed_file).write_text("\n".join(failed) + "\n", encoding="utf-8")
        print(f"{len(failed)} failures written to {args.failed_file} (hash\\tlabel\\terror)")
        for e in failed[:5]:
            print(f"  {e}")
        if args.force_finalize:
            finalize_voice(args.voice)
        else:
            print("Re-run to retry, or use --force-finalize to activate the voice pack anyway.")
        return 1

    print("All assets downloaded successfully.")
    finalize_voice(args.voice)
    return 0


if __name__ == "__main__":
    sys.exit(main())
