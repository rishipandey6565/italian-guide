#!/usr/bin/env python3
"""
EPG Schedule Extractor
Extracts TV channel schedules from XML/XML.GZ files and saves them as JSON
"""

import gzip
import json
import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from urllib.request import urlopen, Request
from io import BytesIO
import pytz


class EPGExtractor:
    def __init__(self, urls: List[str], channels_file: str = "channels.txt"):
        """
        Initialize EPG Extractor
        
        Args:
            urls: List of EPG source URLs (can be .xml or .xml.gz)
            channels_file: Path to file containing channel IDs and names
        """
        self.urls = urls
        self.channels_file = channels_file
        self.italian_tz = pytz.timezone('Europe/Rome')
        self.utc_tz = pytz.UTC
        
        # Load channel mappings
        self.channel_mappings = self.load_channel_mappings()
        
    def load_channel_mappings(self) -> List[Tuple[str, str]]:
        """
        Load channel mappings from channels.txt
        Format: channel_id, channel_name
        
        Returns:
            List of tuples (channel_id, channel_name)
        """
        mappings = []
        
        if not os.path.exists(self.channels_file):
            print(f"Warning: {self.channels_file} not found!")
            return mappings
        
        with open(self.channels_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                    
                parts = [p.strip() for p in line.split(',')]
                if len(parts) >= 2:
                    channel_id, channel_name = parts[0], parts[1]
                    mappings.append((channel_id, channel_name))
                    print(f"Loaded channel: {channel_name} ({channel_id})")
        
        return mappings
    
    def download_and_extract(self, url: str) -> str:
        """
        Download and extract XML content from URL
        
        Args:
            url: URL to download from
            
        Returns:
            XML content as string
        """
        print(f"Downloading from: {url}")
        
        # Create request with headers to avoid blocks
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        request = Request(url, headers=headers)
        
        with urlopen(request, timeout=60) as response:
            content = response.read()
        
        # Check if content is gzipped
        if url.endswith('.gz') or content[:2] == b'\x1f\x8b':
            print("Extracting gzip content...")
            with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                xml_content = gz.read().decode('utf-8')
        else:
            xml_content = content.decode('utf-8')
        
        print(f"Downloaded and extracted {len(xml_content)} bytes")
        return xml_content
    
    def parse_datetime(self, dt_str: str) -> datetime:
        """
        Parse XMLTV datetime format: 20260126050000 +0000
        
        Args:
            dt_str: DateTime string from XML
            
        Returns:
            datetime object in UTC
        """
        # Format: YYYYMMDDHHmmss +offset
        dt_part = dt_str.split()[0]
        dt = datetime.strptime(dt_part, '%Y%m%d%H%M%S')
        return self.utc_tz.localize(dt)
    
    def format_time(self, dt: datetime) -> str:
        """
        Format datetime to HH:MM AM/PM format
        
        Args:
            dt: datetime object
            
        Returns:
            Formatted time string
        """
        return dt.strftime('%I:%M %p').lstrip('0')
    
    def get_italian_date_range(self, day_offset: int = 0) -> Tuple[datetime, datetime]:
        """
        Get start and end of day in Italian timezone
        
        Args:
            day_offset: 0 for today, 1 for tomorrow
            
        Returns:
            Tuple of (start_datetime, end_datetime) in UTC
        """
        # Get current date in Italian timezone
        now_italian = datetime.now(self.italian_tz)
        target_date = now_italian.date() + timedelta(days=day_offset)
        
        # Create start and end of day in Italian timezone
        start_italian = self.italian_tz.localize(
            datetime.combine(target_date, datetime.min.time())
        )
        end_italian = self.italian_tz.localize(
            datetime.combine(target_date, datetime.max.time())
        )
        
        # Convert to UTC
        start_utc = start_italian.astimezone(self.utc_tz)
        end_utc = end_italian.astimezone(self.utc_tz)
        
        return start_utc, end_utc
    
    def extract_channel_data(self, root: ET.Element) -> Dict[str, Dict]:
        """
        Extract channel information from XML
        
        Args:
            root: XML root element
            
        Returns:
            Dictionary mapping channel_id to channel info
        """
        channels = {}
        
        for channel_elem in root.findall('channel'):
            channel_id = channel_elem.get('id')
            display_name_elem = channel_elem.find('display-name')
            
            if channel_id and display_name_elem is not None:
                channels[channel_id] = {
                    'id': channel_id,
                    'name': display_name_elem.text
                }
        
        return channels
    
    def extract_programmes(self, root: ET.Element, channel_id: str, 
                          start_range: datetime, end_range: datetime) -> List[Dict]:
        """
        Extract programme data for a specific channel and time range
        
        Args:
            root: XML root element
            channel_id: Channel ID to filter
            start_range: Start of time range (UTC)
            end_range: End of time range (UTC)
            
        Returns:
            List of programme dictionaries
        """
        programmes = []
        
        for prog in root.findall('programme'):
            if prog.get('channel') != channel_id:
                continue
            
            start_str = prog.get('start')
            stop_str = prog.get('stop')
            
            if not start_str or not stop_str:
                continue
            
            try:
                start_utc = self.parse_datetime(start_str)
                stop_utc = self.parse_datetime(stop_str)
                
                # Check if programme overlaps with our time range
                if stop_utc < start_range or start_utc > end_range:
                    continue
                
                # Adjust times if they cross boundaries
                adjusted_start = max(start_utc, start_range)
                adjusted_stop = min(stop_utc, end_range)
                
                # Convert to Italian timezone for display
                start_italian = adjusted_start.astimezone(self.italian_tz)
                stop_italian = adjusted_stop.astimezone(self.italian_tz)
                
                # Extract programme details
                title_elem = prog.find('title')
                desc_elem = prog.find('desc')
                icon_elem = prog.find('icon')
                episode_elem = prog.find('episode-num')
                
                # Get all categories
                categories = [cat.text for cat in prog.findall('category') if cat.text]
                
                programme_data = {
                    'title': title_elem.text if title_elem is not None else 'Unknown',
                    'start_time': self.format_time(start_italian),
                    'end_time': self.format_time(stop_italian),
                    'description': desc_elem.text if desc_elem is not None else '',
                    'category': categories[0] if categories else '',
                    'logo': icon_elem.get('src') if icon_elem is not None else '',
                    'episode': episode_elem.text if episode_elem is not None else ''
                }
                
                programmes.append(programme_data)
                
            except Exception as e:
                print(f"Error parsing programme: {e}")
                continue
        
        return programmes
    
    def save_schedule(self, channel_name: str, date_label: str, 
                     programmes: List[Dict], date_str: str):
        """
        Save schedule to JSON file
        
        Args:
            channel_name: Name of the channel
            date_label: 'today' or 'tomorrow'
            programmes: List of programme data
            date_str: Date string for the schedule
        """
        # Create directory
        output_dir = Path('schedule') / date_label
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Sanitize channel name for filename
        safe_name = channel_name.replace(' ', '-').replace('/', '-')
        output_file = output_dir / f"{safe_name}.json"
        
        # Prepare JSON data
        schedule_data = {
            'channel_name': channel_name,
            'date': date_str,
            'programmes': programmes
        }
        
        # Save to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(schedule_data, f, ensure_ascii=False, indent=2)
        
        print(f"Saved {len(programmes)} programmes to {output_file}")
    
    def process(self):
        """
        Main processing function
        """
        if not self.channel_mappings:
            print("No channels to process!")
            return
        
        # Download and parse all XML sources
        all_roots = []
        for url in self.urls:
            try:
                xml_content = self.download_and_extract(url)
                root = ET.fromstring(xml_content)
                all_roots.append(root)
            except Exception as e:
                print(f"Error processing {url}: {e}")
                continue
        
        if not all_roots:
            print("No XML data loaded!")
            return
        
        # Process for today and tomorrow
        for day_offset, day_label in [(0, 'today'), (1, 'tomorrow')]:
            start_range, end_range = self.get_italian_date_range(day_offset)
            date_str = start_range.astimezone(self.italian_tz).strftime('%Y-%m-%d')
            
            print(f"\n{'='*60}")
            print(f"Processing {day_label.upper()} ({date_str})")
            print(f"{'='*60}")
            
            # Process each channel
            for channel_id, channel_name in self.channel_mappings:
                print(f"\nProcessing: {channel_name}")
                
                all_programmes = []
                found = False
                
                # Search in all XML sources
                for root in all_roots:
                    # Extract channel data to verify existence
                    channels = self.extract_channel_data(root)
                    
                    # Try to find by ID first, then by name
                    if channel_id in channels:
                        found = True
                        programmes = self.extract_programmes(
                            root, channel_id, start_range, end_range
                        )
                        all_programmes.extend(programmes)
                    else:
                        # Search by name
                        for cid, cdata in channels.items():
                            if cdata['name'] == channel_name:
                                found = True
                                programmes = self.extract_programmes(
                                    root, cid, start_range, end_range
                                )
                                all_programmes.extend(programmes)
                                break
                
                if found and all_programmes:
                    # Sort by start time
                    all_programmes.sort(key=lambda x: x['start_time'])
                    self.save_schedule(channel_name, day_label, all_programmes, date_str)
                else:
                    print(f"  ⚠️  No schedule found for {channel_name}")


def main():
    """
    Main entry point
    """
    # Configuration
    URLS = [
        'https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz',
        # Add more URLs here if needed
        # 'https://example.com/epg.xml',  # Direct XML
    ]
    
    CHANNELS_FILE = 'channels.txt'
    
    print("EPG Schedule Extractor")
    print("=" * 60)
    
    extractor = EPGExtractor(URLS, CHANNELS_FILE)
    extractor.process()
    
    print("\n" + "=" * 60)
    print("Processing complete!")


if __name__ == '__main__':
    main()
