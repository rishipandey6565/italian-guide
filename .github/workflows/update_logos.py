name: Download Logos & Update JSONs

on:
  workflow_dispatch:      # Allows you to click a "Run" button manually
  schedule:
    - cron: '0 2 * * *'   # Optional: Runs automatically every day at 2:00 AM UTC

permissions:
  contents: write         # This is CRITICAL for saving files back to the repo

jobs:
  run-script:
    runs-on: ubuntu-latest

    steps:
      # 1. Get the latest code
      - name: Checkout repository
        uses: actions/checkout@v4

      # 2. Install Python
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      # 3. Install libraries (Requests & Pillow)
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests pillow

      # 4. Run your script
      - name: Run update script
        run: python update_logos.py

      # 5. Save changes (Commit & Push)
      - name: Commit and Push changes
        run: |
          git config --global user.name "github-actions[bot]"
          git config --global user.email "41898282+github-actions[bot]@users.noreply.github.com"
          
          # Add the modified JSON files and the new images
          git add schedule/ downloaded-images/
          
          # Commit (only if there are changes)
          git commit -m "Auto-update: Downloaded logos and updated JSONs [skip ci]" || echo "No changes to commit"
          
          # Push back to the repository
          git push
