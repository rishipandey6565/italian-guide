import os
import gzip
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, time
import pytz
import io
import re

# --- Configuration ---
# Add your EPG URLs here. The script detects .gz automatically.
EPG_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz",
    # "https://example.com/another_schedule.xml", 
]

CHANNEL_FILE = "channel.txt"
OUTPUT_DIR_TODAY = "schedule/today"
OUTPUT_DIR_TOMORROW = "schedule/tomorrow"
TZ_ITALY = pytz.timezone('Europe/Rome')
TZ_UTC = pytz.timezone('UTC')

def load_channels_to_track():
    """
    Reads channel.txt and returns a list of dictionaries.
    Format expected: ChannelID, ChannelName
    """
    channels = []
    if not os.path.exists(CHANNEL_FILE):
        print(f"Error: {CHANNEL_FILE} not found.")
        return []
    
    with open(CHANNEL_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            parts = [p.strip() for p in line.split(',')]
            if len(parts) >= 2:
                # Store both ID and Name provided by user
                channels.append({
                    "user_id": parts[0],
                    "user_name": parts[1],
                    "found_xml_id": None # Will be populated later
                })
    return channels

def get_xml_root(url):
    """
    Downloads and parses XML from a URL (handles .gz and raw .xml).
    """
    try:
        print(f"Downloading: {url}")
        response = requests.get(url, timeout=30)
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
    # Remove space before timezone if present
    date_str = date_str.replace(" +", "+") 
    try:
        # Format usually: 20260126050000+0000
        dt = datetime.strptime(date_str, "%Y%m%d%H%M%S%z")
        return dt
    except ValueError:
        return None

def sanitize_filename(name):
    """Converts 'Sky Serie' to 'Sky-Serie'"""
    return re.sub(r'[^a-zA-Z0-9]', '-', name).strip('-')

def extract_schedule():
    channels_to_track = load_channels_to_track()
    if not channels_to_track:
        print("No channels to track found.")
        return

    # Prepare data structure: { 'Channel Name': [list of programs] }
    all_extracted_data = {} 
    
    # Iterate over all URLs
    for url in EPG_URLS:
        root = get_xml_root(url)
        if root is None:
            continue
            
        # 1. Map User Channel Names/IDs to XML IDs
        # We need to find the correct 'id' used in the XML <programme> tags
        xml_channel_map = {} # Map xml_id -> display_name
        
        for channel in root.findall('channel'):
            c_id = channel.get('id')
            display_name = channel.find('display-name')
            c_name = display_name.text if display_name is not None else ""
            
            # Check against our list
            for track in channels_to_track:
                # Match Logic: Try ID match first, then Name match
                if track['user_id'] == c_id:
                    track['found_xml_id'] = c_id
                elif track['user_name'].lower() == c_name.lower() and track['found_xml_id'] is None:
                    track['found_xml_id'] = c_id
        
        # Filter only channels we found in this XML
        active_ids = {c['found_xml_id']: c for c in channels_to_track if c['found_xml_id']}

        # 2. Parse Programmes
        for prog in root.findall('programme'):
            channel_id = prog.get('channel')
            
            if channel_id in active_ids:
                user_channel_info = active_ids[channel_id]
                channel_name_clean = user_channel_info['user_name']
                
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

    # 3. Process and Save Data (Today/Tomorrow Split)
    now_italy = datetime.now(TZ_ITALY)
    today_date = now_italy.date()
    tomorrow_date = today_date + timedelta(days=1)
    
    # Create directories
    os.makedirs(OUTPUT_DIR_TODAY, exist_ok=True)
    os.makedirs(OUTPUT_DIR_TOMORROW, exist_ok=True)

    for ch_name, programs in all_extracted_data.items():
        # Sort programs by start time
        programs.sort(key=lambda x: x['start_dt'])
        
        for target_date, folder in [(today_date, OUTPUT_DIR_TODAY), (tomorrow_date, OUTPUT_DIR_TOMORROW)]:
            daily_schedule = []
            
            # Define Day Start and End in Italy time
            day_start = TZ_ITALY.localize(datetime.combine(target_date, time.min))
            day_end = TZ_ITALY.localize(datetime.combine(target_date, time.max))
            
            for p in programs:
                p_start = p['start_dt']
                p_end = p['end_dt']
                
                # Check overlap
                # (Start is before day end AND End is after day start)
                if p_start <= day_end and p_end >= day_start:
                    
                    # Logic: "if a show start 11:40 PM and end 12:40 AM then save it as 12:00 Am to 12:40 am"
                    # We clip the start time if it starts before the current day
                    display_start = p_start
                    if p_start < day_start:
                        display_start = day_start
                    
                    # We usually do NOT clip the end time for the viewer context, 
                    # but if you want strictly 24h files, we can clip end too.
                    # Based on your request, I will only clip the start to show it starts at 00:00 for that day.
                    
                    fmt = "%Y-%m-%d %H:%M:%S"
                    
                    entry = {
                        "show_name": p['show_name'],
                        "show_logo": p['logo_url'],
                        "start_time": display_start.strftime(fmt),
                        "end_time": p_end.strftime(fmt), # Keeping original end time
                        "episode_number": p['episode'],
                        "show_category": p['category'],
                        "show_description": p['description']
                    }
                    daily_schedule.append(entry)
            
            if daily_schedule:
                json_output = {
                    "channel_name": ch_name,
                    "date": str(target_date),
                    "programs": daily_schedule
                }
                
                filename = f"{sanitize_filename(ch_name)}.json"
                file_path = os.path.join(folder, filename)
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(json_output, f, indent=2, ensure_ascii=False)
                print(f"Saved {file_path}")

if __name__ == "__main__":
    extract_schedule()
