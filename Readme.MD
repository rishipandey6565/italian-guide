# EPG Schedule Extractor

Automated tool to extract TV channel schedules from EPG (Electronic Program Guide) XML files and save them as JSON.

## Features

- ✅ Download EPG data from multiple URLs
- ✅ Support for both `.xml` and `.xml.gz` formats
- ✅ Automatic timezone conversion (UTC → Italian time)
- ✅ Filter schedules by date (today/tomorrow)
- ✅ Handle shows that cross midnight
- ✅ Save as organized JSON files
- ✅ Automated GitHub Actions workflow

## Setup

### 1. Repository Setup

1. Create a new GitHub repository
2. Add all files from this project to your repository
3. Enable GitHub Actions in repository settings

### 2. Configure Channels

Edit `channels.txt` to add the channels you want to track:

```txt
# Format: channel_id, channel_name
Sky.Serie.it, Sky Serie
Rai1.it, Rai 1
Canale5.it, Canale 5
```

**Format Rules:**
- One channel per line
- Format: `channel_id, channel_name`
- Lines starting with `#` are comments
- The script will first search by `channel_id`, then by `channel_name` if not found

### 3. Configure EPG Sources

Edit `epg_schedule_extractor.py` and update the `URLS` list:

```python
URLS = [
    'https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz',
    'https://example.com/another_epg.xml',  # Direct XML
    # Add more URLs as needed
]
```

### 4. GitHub Actions Permissions

The workflow needs write permissions to commit schedule files:

1. Go to **Settings** → **Actions** → **General**
2. Scroll to **Workflow permissions**
3. Select **Read and write permissions**
4. Click **Save**

## Usage

### Automatic Execution

The workflow runs automatically:
- **Every day at 00:30 UTC** (01:30/02:30 Italian time)
- **When you push changes** to `channels.txt`, the Python script, or the workflow file

### Manual Execution

1. Go to **Actions** tab in your repository
2. Click **EPG Schedule Extractor**
3. Click **Run workflow**
4. Select branch and click **Run workflow**

### Local Testing

```bash
# Install dependencies
pip install -r requirements.txt

# Run the extractor
python epg_schedule_extractor.py
```

## Output Structure

Schedules are saved in the following structure:

```
schedule/
├── today/
│   ├── Sky-Serie.json
│   ├── Rai-1.json
│   └── Canale-5.json
└── tomorrow/
    ├── Sky-Serie.json
    ├── Rai-1.json
    └── Canale-5.json
```

### JSON Format

Each JSON file contains:

```json
{
  "channel_name": "Sky Serie",
  "date": "2026-01-28",
  "programmes": [
    {
      "title": "The Paper 1^TV",
      "start_time": "6:00 AM",
      "end_time": "6:35 AM",
      "description": "Una troupe di documentaristi...",
      "category": "Intrattenimento",
      "logo": "https://guidatv.sky.it/uuid/...",
      "episode": "S1 E1"
    }
  ]
}
```

## How It Works

1. **Download**: Fetches EPG data from configured URLs
2. **Extract**: Decompresses `.gz` files if needed
3. **Parse**: Extracts channel and programme data from XML
4. **Filter**: Filters programmes by date/time in Italian timezone
5. **Adjust**: Handles shows crossing midnight (splits appropriately)
6. **Save**: Saves schedules as JSON files
7. **Commit**: GitHub Actions commits changes back to repository

## Midnight Handling

When a show crosses midnight (e.g., 11:40 PM - 12:40 AM):

- **Today's file**: Contains the portion from 11:40 PM to 11:59:59 PM
- **Tomorrow's file**: Contains the portion from 12:00 AM to 12:40 AM

Times are displayed in 12-hour format (e.g., "11:40 PM", "12:40 AM")

## Troubleshooting

### No schedules generated

1. Check `channels.txt` format is correct
2. Verify channel IDs/names match those in EPG XML
3. Check GitHub Actions logs for errors
4. Ensure EPG URLs are accessible

### Workflow not running

1. Verify **Workflow permissions** are set to "Read and write"
2. Check the workflow file is in `.github/workflows/`
3. Ensure repository is not a fork (or enable Actions for forks)

### Wrong timezone

The script uses `Europe/Rome` timezone. If you need a different timezone, edit:

```python
self.italian_tz = pytz.timezone('Europe/Rome')  # Change this
```

## Customization

### Add More EPG Sources

Edit the `URLS` list in `epg_schedule_extractor.py`:

```python
URLS = [
    'https://source1.com/epg.xml.gz',
    'https://source2.com/epg.xml',
    'https://source3.com/another.xml.gz',
]
```

### Change Schedule Frequency

Edit `.github/workflows/epg-extractor.yml`:

```yaml
schedule:
  - cron: '30 0 * * *'  # Change this cron expression
```

Cron examples:
- `0 */6 * * *` - Every 6 hours
- `0 0,12 * * *` - Twice daily (midnight and noon)
- `30 0 * * *` - Daily at 00:30 UTC

## License

MIT License - Feel free to use and modify as needed.

## Support

For issues or questions, please open an issue in the repository.
