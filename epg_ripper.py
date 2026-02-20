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

OUTPUT_DIR_TODAY = "schedule/today"
OUTPUT_DIR_TOMORROW = "schedule/tomorrow"
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
            # Ignore empty lines and comments
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
        
        # Check if it is gzipped (Magic number 1f 8b) or URL ends in .gz
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
    # Remove space before timezone if present
    date_str = date_str.replace(" +", "+") 
    try:
        dt = datetime.strptime(date_str, "%Y%m%d%H%M%S%z")
        return dt
    except ValueError:
        return None

def sanitize_filename(name):
    """Converts 'Sky Serie' to 'Sky-Serie' and removes illegal chars"""
    clean_name = re.sub(r'[^a-zA-Z0-9]', '-', name).strip('-')
    # Collapse multiple hyphens into one
    clean_name = re.sub(r'-+', '-', clean_name)
    return clean_name

def extract_schedule():
    # Load the filter list
    allowed_channels = load_filter(FILTER_FILE)
    if not allowed_channels:
        print(f"No channels loaded from {FILTER_FILE}. Exiting.")
        return

    # Prepare data structure: { 'Channel Name': [list of programs] }
    all_extracted_data = {}
    
    # Iterate over all URLs
    for url in EPG_URLS:
        root = get_xml_root(url)
        if root is None:
            continue
            
        print("Parsing XML data...")

        # 1. Map Channel IDs to Display Names from the XML itself
        channel_id_map = {} 
        
        for channel in root.findall('channel'):
            c_id = channel.get('id')
            
            # Skip channels that are not in our filter.txt
            if c_id not in allowed_channels:
                continue
            
            # Use the preferred display name from filter.txt
            c_name = allowed_channels[c_id]
            channel_id_map[c_id] = c_name

        print(f"Found {len(channel_id_map)} filtered channels in XML.")

        # 2. Parse Programmes
        count_progs = 0
        for prog in root.findall('programme'):
            channel_id = prog.get('channel')
            
            # Only process if we know the channel name (which is now pre-filtered)
            if channel_id in channel_id_map:
                channel_name_clean = channel_id_map[channel_id]
                
                # Times
                start_utc = parse_xmltv_date(prog.get('start'))
                stop_utc = parse_xmltv_date(prog.get('stop'))
                
                if not start_utc or not stop_utc:
                    continue

                # Convert to Italy Time
                start_it = start_utc.astimezone(TZ_ITALY)
                stop_it = stop_utc.astimezone(TZ_ITALY)
                
                # Extract Metadata
                title_el = prog.find('title')
                desc_el = prog.find('desc')
                cat_el = prog.find('category')
                icon_el = prog.find('icon')
                ep_el = prog.find('episode-num')
                
                program_data = {
                    "show_name": title_el.text if title_el is not None else "No Title",
                    "description": desc_el.text if desc_el is not None else "",
                    "category": cat_el.text if cat_el is not None else "",
                    "start_dt": start_it, # Keep as object for sorting/filtering
                    "end_dt": stop_it,
                    "logo_url": icon_el.get('src') if icon_el is not None else "",
                    "episode": ep_el.text if ep_el is not None else ""
                }
                
                if channel_name_clean not in all_extracted_data:
                    all_extracted_data[channel_name_clean] = []
                all_extracted_data[channel_name_clean].append(program_data)
                count_progs += 1
        
        print(f"Extracted {count_progs} programs for filtered channels.")

    # 3. Process, Save Data, and Write Log (Today/Tomorrow Split)
    now_italy = datetime.now(TZ_ITALY)
    today_date = now_italy.date()
    tomorrow_date = today_date + timedelta(days=1)
    
    # Create directories
    os.makedirs(OUTPUT_DIR_TODAY, exist_ok=True)
    os.makedirs(OUTPUT_DIR_TOMORROW, exist_ok=True)
    
    print(f"Saving schedules for {today_date} and {tomorrow_date} and writing log...")

    files_saved = 0
    
    # Open log file to write results
    with open(LOG_FILE, 'w', encoding='utf-8') as log_f:
        log_f.write(f"--- Scrape Log: {now_italy.strftime('%Y-%m-%d %H:%M:%S')} ---\n\n")
        
        # Iterate over ALL allowed channels from filter.txt to catch missing ones
        for c_id, ch_name in allowed_channels.items():
            log_f.write(f"[{ch_name} (ID: {c_id})]\n")
            
            programs = all_extracted_data.get(ch_name, [])
            if programs:
                programs.sort(key=lambda x: x['start_dt'])
            
            # Check for both Today and Tomorrow
            for target_date, folder, day_label in [(today_date, OUTPUT_DIR_TODAY, "Today"), (tomorrow_date, OUTPUT_DIR_TOMORROW, "Tomorrow")]:
                if not programs:
                    log_f.write(f"  -> {day_label} ({target_date}): FAILED - No data found in XML source.\n")
                    continue

                daily_schedule = []
                
                # Define Day Start and End in Italy time
                day_start = TZ_ITALY.localize(datetime.combine(target_date, time.min))
                day_end = TZ_ITALY.localize(datetime.combine(target_date, time.max))
                
                for p in programs:
                    p_start = p['start_dt']
                    p_end = p['end_dt']
                    
                    # Check overlap (Start is before day end AND End is after day start)
                    if p_start <= day_end and p_end >= day_start:
                        
                        # Clip start time for display if it starts before today
                        display_start = p_start if p_start >= day_start else day_start
                        fmt = "%Y-%m-%d %H:%M:%S"
                        
                        entry = {
                            "show_name": p['show_name'],
                            "show_logo": p['logo_url'],
                            "start_time": display_start.strftime(fmt),
                            "end_time": p_end.strftime(fmt),
                            "episode_number": p['episode'],
                            "show_category": p['category'],
                            "show_description": p['description']
                        }
                        daily_schedule.append(entry)
                
                if not daily_schedule:
                    log_f.write(f"  -> {day_label} ({target_date}): FAILED - No programs scheduled for this specific date.\n")
                else:
                    json_output = {
                        "channel_name": ch_name,
                        "date": str(target_date),
                        "programs": daily_schedule
                    }
                    
                    filename = f"{sanitize_filename(ch_name)}.json"
                    file_path = os.path.join(folder, filename)
                    
                    try:
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(json_output, f, indent=2, ensure_ascii=False)
                        files_saved += 1
                        log_f.write(f"  -> {day_label} ({target_date}): SUCCESS - Saved {len(daily_schedule)} programs.\n")
                    except OSError as e:
                        log_f.write(f"  -> {day_label} ({target_date}): FAILED - OS Error while saving file: {e}\n")
                        print(f"Error saving file for {ch_name}: {e}")
            
            log_f.write("\n") # Add a blank line between channels in the log for readability

    print(f"Done! Saved {files_saved} JSON files. Check {LOG_FILE} for full details.")

if __name__ == "__main__":
    extract_schedule()
