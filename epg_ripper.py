import os
import gzip
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, time
import pytz
import re

# --- Configuration ---
EPG_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz",
    # Add more URLs here if needed
]

OUTPUT_DIR = "schedule"
FILTER_FILE = "filter.txt"
LOG_FILE = "scrape.log"
TZ_ITALY = pytz.timezone('Europe/Rome')

def load_filter(filepath):
    """
    Reads filter.txt and returns a dictionary mapping channel IDs to channel names.
    Expects format: Channel.ID, Channel Name
    """
    allowed_channels = {}
    if not os.path.exists(filepath):
        print(f"Error: {filepath} not found in the current directory.")
        return allowed_channels
        
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                parts = line.split(',', 1)
                if len(parts) == 2:
                    c_id = parts[0].strip()
                    c_name = parts[1].strip()
                    allowed_channels[c_id] = c_name
    return allowed_channels

def get_xml_root(url):
    """
    Downloads and parses XML from a URL (handles .gz and raw .xml).
    """
    try:
        print(f"Downloading: {url}")
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        content = response.content
        
        if url.endswith('.gz') or content[:2] == b'\x1f\x8b':
            try:
                content = gzip.decompress(content)
            except OSError:
                print("Warning: Failed to decompress. Trying as plain text.")
        
        return ET.fromstring(content)
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return None

def parse_xmltv_date(date_str):
    """
    Parses XMLTV date format: YYYYMMDDHHMMSS +0000
    Returns a datetime object in UTC.
    """
    if not date_str:
        return None
    date_str = date_str.replace(" +", "+") 
    try:
        dt = datetime.strptime(date_str, "%Y%m%d%H%M%S%z")
        return dt
    except ValueError:
        return None

def sanitize_filename(name):
    """Converts 'Sky Serie' to 'Sky-Serie' and removes illegal chars"""
    clean_name = re.sub(r'[^a-zA-Z0-9]', '-', name).strip('-')
    clean_name = re.sub(r'-+', '-', clean_name)
    return clean_name

def extract_schedule():
    allowed_channels = load_filter(FILTER_FILE)
    if not allowed_channels:
        print(f"No channels loaded from {FILTER_FILE}. Exiting.")
        return

    all_extracted_data = {}
    
    for url in EPG_URLS:
        root = get_xml_root(url)
        if root is None:
            continue
            
        print("Parsing XML data...")

        channel_id_map = {} 
        for channel in root.findall('channel'):
            c_id = channel.get('id')
            if c_id not in allowed_channels:
                continue
            channel_id_map[c_id] = allowed_channels[c_id]

        print(f"Found {len(channel_id_map)} filtered channels in XML.")

        count_progs = 0
        for prog in root.findall('programme'):
            channel_id = prog.get('channel')
            
            if channel_id in channel_id_map:
                channel_name_clean = channel_id_map[channel_id]
                
                start_utc = parse_xmltv_date(prog.get('start'))
                stop_utc = parse_xmltv_date(prog.get('stop'))
                
                if not start_utc or not stop_utc:
                    continue

                start_it = start_utc.astimezone(TZ_ITALY)
                stop_it = stop_utc.astimezone(TZ_ITALY)
                
                title_el = prog.find('title')
                cat_el = prog.find('category')
                icon_el = prog.find('icon')
                ep_el = prog.find('episode-num')
                
                program_data = {
                    "show_name": title_el.text if title_el is not None else "No Title",
                    "category": cat_el.text if cat_el is not None else "",
                    "start_dt": start_it, 
                    "end_dt": stop_it,
                    "logo_url": icon_el.get('src') if icon_el is not None else "",
                    "episode": ep_el.text if ep_el is not None else ""
                }
                
                if channel_name_clean not in all_extracted_data:
                    all_extracted_data[channel_name_clean] = []
                all_extracted_data[channel_name_clean].append(program_data)
                count_progs += 1
        
        print(f"Extracted {count_progs} programs for filtered channels.")

    # 3. Process and Save Data
    now_italy = datetime.now(TZ_ITALY)
    today_date = now_italy.date()
    tomorrow_date = today_date + timedelta(days=1)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"Saving schedules and generating master JSON for {today_date}...")

    files_saved = 0
    master_today_data = [] # Stores all channels for 'Today'
    
    with open(LOG_FILE, 'w', encoding='utf-8') as log_f:
        log_f.write(f"--- Scrape Log: {now_italy.strftime('%Y-%m-%d %H:%M:%S')} ---\n\n")
        
        for c_id, ch_name in allowed_channels.items():
            log_f.write(f"[{ch_name} (ID: {c_id})]\n")
            
            programs = all_extracted_data.get(ch_name, [])
            if programs:
                programs.sort(key=lambda x: x['start_dt'])
            
            channel_schedule = {
                "channel_name": ch_name,
                "days": []
            }
            
            has_data = False

            for target_date, day_label in [(today_date, "Today"), (tomorrow_date, "Tomorrow")]:
                daily_schedule = []
                
                day_start = TZ_ITALY.localize(datetime.combine(target_date, time.min))
                day_end = TZ_ITALY.localize(datetime.combine(target_date, time.max))
                
                for p in programs:
                    p_start = p['start_dt']
                    p_end = p['end_dt']
                    
                    if p_start <= day_end and p_end >= day_start:
                        display_start = p_start if p_start >= day_start else day_start
                        
                        # HH:MM format - dropping the seconds
                        time_fmt = "%H:%M"
                        
                        # Baseline entry
                        entry = {
                            "name": p['show_name'],
                            "start": display_start.strftime(time_fmt),
                            "end": p_end.strftime(time_fmt)
                        }
                        
                        # Size Optimization: Only append keys if the data isn't empty
                        if p['logo_url']: entry["logo"] = p['logo_url']
                        if p['episode']: entry["episode"] = p['episode']
                        if p['category']: entry["category"] = p['category']
                        
                        daily_schedule.append(entry)
                
                if daily_schedule:
                    channel_schedule["days"].append({
                        "date": str(target_date),
                        "programs": daily_schedule
                    })
                    has_data = True
                    
                    # If this is 'Today', push it to the master dictionary
                    if target_date == today_date:
                        master_today_data.append({
                            "channel_name": ch_name,
                            "programs": daily_schedule
                        })
                        
                    log_f.write(f"  -> {day_label} ({target_date}): SUCCESS - Processed {len(daily_schedule)} programs.\n")
                else:
                    log_f.write(f"  -> {day_label} ({target_date}): FAILED - No programs scheduled for this date.\n")

            if has_data:
                filename = f"{sanitize_filename(ch_name)}.json"
                file_path = os.path.join(OUTPUT_DIR, filename)
                
                try:
                    # Using separators to remove whitespace and minify the JSON
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(channel_schedule, f, separators=(',', ':'), ensure_ascii=False)
                    files_saved += 1
                    log_f.write(f"  -> Merged File Saved: {file_path}\n")
                except OSError as e:
                    log_f.write(f"  -> FAILED - OS Error while saving file: {e}\n")
                    print(f"Error saving file for {ch_name}: {e}")
            else:
                log_f.write(f"  -> FAILED - No data to save for both days.\n")
            
            log_f.write("\n")

    # Save the Master JSON file for Today
    if master_today_data:
        master_file_path = os.path.join(OUTPUT_DIR, "all-channel.json")
        try:
            with open(master_file_path, 'w', encoding='utf-8') as f:
                json.dump(master_today_data, f, separators=(',', ':'), ensure_ascii=False)
            print(f"Successfully saved Master JSON: {master_file_path}")
        except OSError as e:
            print(f"Error saving master file: {e}")

    print(f"Done! Saved {files_saved} individual channel files and 1 master file in /{OUTPUT_DIR}/. Check {LOG_FILE} for full details.")

if __name__ == "__main__":
    extract_schedule()
