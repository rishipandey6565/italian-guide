#!/usr/bin/env python3
"""
Logo Downloader and Schedule Updater
1. Reads JSON files from "schedule/"
2. Downloads show logos concurrently, converts to WebP, and saves to "downloaded-images/<channel-folder>/"
3. Reuses already downloaded images between individual channel schedules and the master "all-channel.json"
4. Preserves JSON minification (separators=(',', ':')) by default to save storage and CDN bandwidth
5. Atomic write with .tmp files to prevent corruption
"""

import argparse
import concurrent.futures
import json
import logging
import os
import re
import time
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from PIL import Image, UnidentifiedImageError

# -----------------------
# Config
# -----------------------
DEFAULT_SCHEDULES_DIR = "schedule"  
DEFAULT_OUT_DIR = "downloaded-images"
DEFAULT_BASE_URL = "https://cdn.tv-programma.it/wp-content/uploads"
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
def sanitize_filename(name: str) -> str:
    """Converts 'Sky Serie' to 'Sky-Serie' and removes illegal chars, matching epg_ripper.py"""
    clean_name = re.sub(r'[^a-zA-Z0-9]', '-', name).strip('-')
    clean_name = re.sub(r'-+', '-', clean_name)
    return clean_name or "unknown"


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
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
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


def extract_program_dicts(node) -> List[dict]:
    """Recursively search for all dictionaries inside any 'programs' list."""
    found = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "programs" and isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        found.append(item)
            else:
                found.extend(extract_program_dicts(value))
    elif isinstance(node, list):
        for item in node:
            found.extend(extract_program_dicts(item))
    return found


def extract_programs_by_channel(data, default_folder: str) -> List[Tuple[str, dict]]:
    """
    Returns a list of (channel_folder, program_dict) tuples.
    Correctly associates programs with their channel folder,
    including master list format (all-channel.json) and individual channel format.
    """
    results = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and "channel_name" in item and "programs" in item:
                ch_name = item.get("channel_name", "")
                ch_folder = sanitize_filename(ch_name) if ch_name else default_folder
                progs = item.get("programs", [])
                if isinstance(progs, list):
                    for p in progs:
                        if isinstance(p, dict):
                            results.append((ch_folder, p))
            else:
                for p in extract_program_dicts(item):
                    results.append((default_folder, p))
    elif isinstance(data, dict):
        ch_name = data.get("channel_name", "")
        ch_folder = sanitize_filename(ch_name) if ch_name else default_folder
        for p in extract_program_dicts(data):
            results.append((ch_folder, p))
    return results


# -----------------------
# Core per-JSON processing
# -----------------------
def process_json_file(json_path: Path, out_root: Path, base_url: str, workers: int, minify: bool = True):
    logger.info(f"Processing {json_path}")

    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Invalid JSON {json_path} : {e}")
        return

    default_folder = json_path.stem
    items = extract_programs_by_channel(data, default_folder)
    if not items:
        logger.warning(f"No programs found in {json_path}")
        return

    # Track unique download tasks and program dict references by (channel_folder, show_name)
    channel_show_to_url: Dict[Tuple[str, str], str] = {}
    channel_show_to_programs: Dict[Tuple[str, str], List[dict]] = {}

    for ch_folder, row in items:
        show = row.get("name", "").strip()
        url = row.get("logo", "").strip()

        if not show:
            continue

        key = (ch_folder, show)
        if key not in channel_show_to_url and url.startswith("http"):
            channel_show_to_url[key] = url

        channel_show_to_programs.setdefault(key, []).append(row)

    tasks = []
    for (ch_folder, show_name), url in channel_show_to_url.items():
        target_dir = out_root / ch_folder
        ensure_dir(target_dir)
        slug = slugify(show_name)
        out_path = target_dir / f"{slug}.webp"
        tasks.append((ch_folder, show_name, url, out_path))

    # Multi-threaded downloading
    def worker(task):
        ch_folder, show_name, url, out_path = task

        # Skip if file exists AND is not empty
        if out_path.exists() and out_path.stat().st_size > 0:
            return (ch_folder, show_name), out_path, None

        try:
            content = download_with_retries(url)
            convert_to_webp(content, out_path)
            logger.info(f"Saved {ch_folder}/{out_path.name}")
            return (ch_folder, show_name), out_path, None
        except Exception as e:
            logger.error(f"Failed [{ch_folder}] {show_name}: {e}")
            return (ch_folder, show_name), out_path, e

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as exe:
        results = list(exe.map(worker, tasks))

    results_map = {r[0]: (r[1], r[2]) for r in results}

    for (ch_folder, show_name), program_dicts in channel_show_to_programs.items():
        if (ch_folder, show_name) in results_map:
            out_path, err = results_map[(ch_folder, show_name)]
            if err:
                final_url = FALLBACK_LOGO_URL
            else:
                slug = slugify(show_name)
                new_rel = f"{out_root.name}/{ch_folder}/{slug}.webp"
                final_url = f"{base_url}/{new_rel}"
        else:
            final_url = FALLBACK_LOGO_URL

        # Update JSON references in memory
        for p_dict in program_dicts:
            p_dict["logo"] = final_url

    # Atomic Save
    temp_path = json_path.with_suffix(".tmp")
    try:
        if minify:
            content = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
        else:
            content = json.dumps(data, indent=4, ensure_ascii=False)
        temp_path.write_text(content, encoding="utf-8")
        os.replace(temp_path, json_path)
    except Exception as e:
        logger.error(f"Failed to save JSON {json_path}: {e}")
        if temp_path.exists():
            temp_path.unlink()


# -----------------------
# Main
# -----------------------
def main():
    parser = argparse.ArgumentParser(description="Download TV show logos and update schedule JSON files.")
    parser.add_argument("--schedules-dir", default=DEFAULT_SCHEDULES_DIR, help="Path to schedules directory")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Path to output images directory")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base CDN URL for uploaded images")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Number of concurrent download workers")
    parser.add_argument("--pretty", action="store_true", help="Output indented JSON instead of minified JSON")
    args = parser.parse_args()

    schedules_dir = Path(args.schedules_dir)
    out_root = Path(args.out_dir)
    base_url = args.base_url.rstrip("/")
    workers = args.workers
    minify = not args.pretty

    if not schedules_dir.exists():
        logger.error(f"Schedules directory '{schedules_dir}' does not exist.")
        return

    # Process all channel files first, then all-channel.json last
    # This ensures images are already downloaded and can be reused instantly
    all_json_files = sorted(schedules_dir.glob("*.json"))
    channel_files = [p for p in all_json_files if p.name != "all-channel.json"]
    master_files = [p for p in all_json_files if p.name == "all-channel.json"]

    for json_path in channel_files + master_files:
        process_json_file(json_path, out_root, base_url, workers, minify=minify)


if __name__ == "__main__":
    main()
