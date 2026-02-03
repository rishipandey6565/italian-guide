#!/usr/bin/env python3
"""
Updated: 
1. Source folder changed to "schedule/"
2. JSON key changed to "programs"
3. Fallback URL used if image download fails
"""

import argparse
import concurrent.futures
import json
import logging
import os
import re
import sys
import time
from io import BytesIO
from pathlib import Path
from typing import Dict, List

import requests
from PIL import Image, UnidentifiedImageError

# -----------------------
# Config
# -----------------------
DEFAULT_SCHEDULES_DIR = "schedule"  # Updated from "schedules"
DEFAULT_OUT_DIR = "downloaded-images"
DEFAULT_BASE_URL = "https://tv-programma.it/wp-content/uploads"
FALLBACK_LOGO_URL = "https://tv-programma.it/wp-content/uploads/2026/02/sample-image.webp"

DEFAULT_WORKERS = 32
REQUEST_TIMEOUT = 20
RETRY_COUNT = 3
RETRY_BACKOFF = 1.2
WEBP_QUALITY = 80

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger("logo_downloader")

# Optimization: Global Session for Keep-Alive
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=DEFAULT_WORKERS, 
    pool_maxsize=DEFAULT_WORKERS
)
session.mount('https://', adapter)
session.mount('http://', adapter)


# -----------------------
# Helpers
# -----------------------
def slugify(name: str) -> str:
    """Convert show_name to south-park.webp style."""
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "-", name)
    name = re.sub(r"-+", "-", name).strip("-")
    return name or "unknown"


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def download_with_retries(url: str) -> bytes:
    last_err = None
    backoff = 1
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            # Use global session
            resp = session.get(url, timeout=REQUEST_TIMEOUT, stream=True)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            last_err = e
            if attempt < RETRY_COUNT:
                time.sleep(backoff)
                backoff *= RETRY_BACKOFF
    raise last_err


def convert_to_webp(image_bytes: bytes, out_path: Path):
    try:
        img = Image.open(BytesIO(image_bytes))
    except UnidentifiedImageError:
        raise RuntimeError("Invalid image data")

    if img.mode not in ("RGB", "RGBA"):
        img = img.convert("RGBA" if "A" in img.getbands() else "RGB")

    img.save(out_path, "WEBP", quality=WEBP_QUALITY, method=6)


# -----------------------
# Core per-JSON processing
# -----------------------
def process_json_file(
    json_path: Path, day: str, out_root: Path, base_url: str, workers: int
):
    logger.info(f"Processing {json_path}")

    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Invalid JSON {json_path} : {e}")
        return

    # Updated: changed 'schedule' to 'programs'
    programs = data.get("programs", [])
    
    channel_folder = json_path.stem
    target_dir = out_root / channel_folder / day
    ensure_dir(target_dir)

    # RULE: One image per show_name
    show_to_url: Dict[str, str] = {}      # show_name -> first URL
    show_to_indexes: Dict[str, List[int]] = {}  # show_name -> row indexes

    for idx, row in enumerate(programs):
        show = row.get("show_name", "").strip()
        url = row.get("show_logo", "").strip()

        # If empty show name, skip
        if not show:
            continue
        
        # NOTE: Even if URL is empty or invalid, we track the index
        # so we can inject the fallback URL later if needed.
        if show not in show_to_url and url.startswith("http"):
            show_to_url[show] = url
        
        show_to_indexes.setdefault(show, []).append(idx)

    tasks = []

    # Prepare tasks only for shows that actually have a URL
    for show_name, url in show_to_url.items():
        slug = slugify(show_name)
        out_path = target_dir / f"{slug}.webp"
        tasks.append((show_name, url, out_path))

    # Multi-threaded downloading
    def worker(task):
        show_name, url, out_path = task

        if out_path.exists():
            return show_name, out_path, None

        try:
            content = download_with_retries(url)
            convert_to_webp(content, out_path)
            logger.info(f"Saved {out_path.name}")
            return show_name, out_path, None
        except Exception as e:
            logger.error(f"Failed {show_name}: {e}")
            return show_name, out_path, e

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as exe:
        results = list(exe.map(worker, tasks))

    # Update JSON
    # We loop through results to handle successes and failures
    results_map = {r[0]: (r[1], r[2]) for r in results} # name -> (path, err)

    for show_name, indexes in show_to_indexes.items():
        # Check if we tried to download this show
        if show_name in results_map:
            out_path, err = results_map[show_name]
            
            if err:
                # Case 1: Download failed -> Use Fallback URL
                final_url = FALLBACK_LOGO_URL
            else:
                # Case 2: Success -> Use local WebP URL
                slug = slugify(show_name)
                # Ensure we use out_root.name to handle custom --out-dir names correctly
                new_rel = f"{out_root.name}/{channel_folder}/{day}/{slug}.webp"
                final_url = f"{base_url}/{new_rel}"
        else:
            # Case 3: No URL was present originally -> Use Fallback URL
            # (Optional: assuming you want fallback even if source was empty)
            final_url = FALLBACK_LOGO_URL

        # Apply the URL to all occurrences of this show
        for idx in indexes:
            programs[idx]["show_logo"] = final_url

    # Atomic Save
    temp_path = json_path.with_suffix(".tmp")
    try:
        temp_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(temp_path, json_path)
    except Exception as e:
        logger.error(f"Failed to save JSON {json_path}: {e}")
        if temp_path.exists():
            temp_path.unlink()


# -----------------------
# Main
# -----------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schedules-dir", default=DEFAULT_SCHEDULES_DIR)
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    args = parser.parse_args()

    schedules_dir = Path(args.schedules_dir)
    out_root = Path(args.out_dir)
    base_url = args.base_url.rstrip("/")
    workers = args.workers

    # Process 'today' and 'tomorrow' inside 'schedule/'
    for day in ("today", "tomorrow"):
        folder = schedules_dir / day
        if not folder.exists():
            continue

        for json_path in sorted(folder.glob("*.json")):
            process_json_file(json_path, day, out_root, base_url, workers)


if __name__ == "__main__":
    main()
