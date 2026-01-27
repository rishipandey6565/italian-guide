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
]

CHANNEL_FILE = "channel.txt"
OUTPUT_DIR_TODAY = "schedule/today"
OUTPUT_DIR_TOMORROW = "schedule/tomorrow"
TZ_ITALY = pytz.timezone('Europe/Rome')

def load_channels_to_track():
    channels = []
    if not os.path.exists(CHANNEL_FILE):
        print(f"Error: {CHANNEL_FILE} not found.")
        return []
    with open(CHANNEL_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            parts = [p.strip() for p in line.split(',')]
            if len(parts) >= 2:
                channels.append({"user_id": parts[0], "user_name": parts[1], "found_xml_id": None})
    print(f"Loaded {len(channels)} channels from txt file.")
    return channels

def get_xml_root(url):
    try:
        print(f"Downloading: {url}")
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        content = response.content
        if url.endswith('.gz') or content[:2] == b'\x1f\x8b':
            content = gzip.decompress(content)
        return ET.fromstring(content)
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return None

def parse_xmltv_date(date_str):
    date_str = date_str.replace(" +", "+") 
    try:
        return datetime.strptime(date_str, "%Y%m%d%H%M%S%z")
    except ValueError:
        return None

def sanitize_filename(name):
    return re.sub(r'[^a-zA-Z0-9]', '-', name).strip('-')

def extract_schedule():
    channels_to_track = load_channels_to_track()
    if not channels_to_track:
        return

    all_extracted_data = {} 
    
    # Create directories immediately to prevent Git error
    os.makedirs(OUTPUT_DIR_TODAY, exist_ok=True)
    os.makedirs(OUTPUT_DIR_TOMORROW, exist_ok=True)

    for url in EPG_URLS:
        root = get_xml_root(url)
        if root is None: continue
        
        # DEBUG: Print first 5 channels found in XML to verify IDs
        print("--- DEBUG: Sample Channels in XML ---")
        xml_channels = root.findall('channel')
        for i, ch in enumerate(xml_channels[:5]):
            print(f"XML ID: {ch.get('id')} | Name: {ch.find('display-name').text if ch.find('display-name') is not None else 'N/A'}")
        print("-------------------------------------")

        # Map IDs
        for channel in xml_channels:
            c_id = channel.get('id')
            display_name = channel.find('display-name').text or ""
            for track in channels_to_track:
                if track['user_id'] == c_id:
                    track['found_xml_id'] = c_id
                elif track['user_name'].lower() == display_name.lower() and track['found_xml_id'] is None:
                    track['found_xml_id'] = c_id
        
        active_ids = {c['found_xml_id']: c for c in channels_to_track if c['found_xml_id']}
        print(f"Found {len(active_ids)} matching channels out of {len(channels_to_track)} requested.")

        # Parse Programmes
        count_progs = 0
        for prog in root.findall('programme'):
            channel_id = prog.get('channel')
            if channel_id in active_ids:
                count_progs += 1
                start_utc = parse_xmltv_date(prog.get('start'))
                stop_utc = parse_xmltv_date(prog.get('stop'))
                
                if not start_utc or not stop_utc: continue

                start_it = start_utc.astimezone(TZ_ITALY)
                stop_it = stop_utc.astimezone(TZ_ITALY)
                
                # ... (Data Extraction Logic) ...
                user_channel_info = active_ids[channel_id]
                ch_name_clean = user_channel_info['user_name']
                
                # EXTRACT METADATA
                title = prog.find('title').text if prog.find('title') is not None else "No Title"
                desc = prog.find('desc').text if prog.find('desc') is not None else ""
                cat = prog.find('category').text if prog.find('category') is not None else ""
                icon = prog.find('icon').get('src') if prog.find('icon') is not None else ""
                ep = prog.find('episode-num').text if prog.find('episode-num') is not None else ""

                program_data = {
                    "show_name": title, "description": desc, "category": cat,
                    "start_dt": start_it, "end_dt": stop_it,
                    "logo_url": icon, "episode": ep
                }
                
                if ch_name_clean not in all_extracted_data:
                    all_extracted_data[ch_name_clean] = []
                all_extracted_data[ch_name_clean].append(program_data)
        
        print(f"Extracted {count_progs} programs for matched channels.")

    # Save Data
    now_italy = datetime.now(TZ_ITALY)
    today_date = now_italy.date()
    tomorrow_date = today_date + timedelta(days=1)
    
    print(f"Filtering for Today ({today_date}) and Tomorrow ({tomorrow_date})...")

    files_saved = 0
    for ch_name, programs in all_extracted_data.items():
        programs.sort(key=lambda x: x['start_dt'])
        
        for target_date, folder in [(today_date, OUTPUT_DIR_TODAY), (tomorrow_date, OUTPUT_DIR_TOMORROW)]:
            daily_schedule = []
            day_start = TZ_ITALY.localize(datetime.combine(target_date, time.min))
            day_end = TZ_ITALY.localize(datetime.combine(target_date, time.max))
            
            for p in programs:
                # Check overlap with the day
                if p['start_dt'] <= day_end and p['end_dt'] >= day_start:
                    display_start = p['start_dt'] if p['start_dt'] >= day_start else day_start
                    
                    daily_schedule.append({
                        "show_name": p['show_name'],
                        "show_logo": p['logo_url'],
                        "start_time": display_start.strftime("%Y-%m-%d %H:%M:%S"),
                        "end_time": p['end_dt'].strftime("%Y-%m-%d %H:%M:%S"),
                        "episode_number": p['episode'],
                        "show_category": p['category'],
                        "show_description": p['description']
                    })
            
            if daily_schedule:
                filename = f"{sanitize_filename(ch_name)}.json"
                file_path = os.path.join(folder, filename)
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump({"channel_name": ch_name, "date": str(target_date), "programs": daily_schedule}, f, indent=2, ensure_ascii=False)
                files_saved += 1
                
    print(f"Successfully saved {files_saved} JSON files.")

if __name__ == "__main__":
    extract_schedule()
