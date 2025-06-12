import os
import json
import logging
import requests
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Google Sheets configuration
SHEET_ID = "1NQT9pj0P_C-iuwTRANtOXyGM1zVXkhL_b4TuGMxR3qY"
SHEET_NAME = "IMD_News"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
KEY_FILE_CONTENT = os.environ.get("JSON_KEY")

# Setup logging
logging.basicConfig(filename="scraper.log", level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = lambda msg: (print(msg), logging.info(msg))

# Algolia API configuration
ALGOLIA_URL = "https://x47r3da403-3.algolianet.com/1/indexes/IMD_WEBSITE/query"
ALGOLIA_HEADERS = {
    "Content-Type": "application/json",
    "X-Algolia-API-Key": "ZjM2ZWEwZjJmNzdjNDE1N2Y0Zjc1ODkyOWRmNDFkM2ZkYjIwNDRjYWEzYzhjNGM3ZDcwNzY5NzU5ZmQ5ZjMwMnZhbGlkVW50aWw9NDE5ODAwNDYwODgmZmlsdGVycz1jYXRlZ29yeSUzQU5ld3M",
    "X-Algolia-Application-Id": "X47R3DA403",
}

def get_sheet_service():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(KEY_FILE_CONTENT), scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()

def fetch_existing_object_ids(service):
    log("📥 Fetching existing objectIDs from Google Sheet...")
    result = service.values().get(spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!E2:E").execute()
    values = result.get("values", [])
    existing_ids = set(v[0].strip() for v in values if v)
    log(f"✅ Loaded {len(existing_ids)} objectIDs")
    return existing_ids

def fetch_articles(max_pages=15):
    hits = []
    for page in range(max_pages):
        res = requests.post(ALGOLIA_URL, headers=ALGOLIA_HEADERS, json={"params": f"query=&hitsPerPage=100&page={page}"})
        if res.status_code != 200:
            log(f"❌ Failed to fetch page {page}: {res.status_code}")
            break
        page_hits = res.json().get("hits", [])
        if not page_hits:
            break
        hits.extend(page_hits)
    log(f"✅ Fetched {len(hits)} articles")
    return hits

def extract_row(article):
    title = article.get("title", "")
    date = article.get("publicationDate", "")[:10]
    url = article.get("docLink", "")
    image = article.get("imageURL", "")
    object_id = str(article.get("objectID", ""))
    summary = article.get("description") or article.get("abstract", "")
    raw_cat = article.get("type", "News Stories")
    category = " ".join(word.capitalize() for word in raw_cat.split()[:2])  # First 2 words capitalized
    return [title, date, url, image, object_id, summary, category]

def upload_articles(service, articles, existing_ids):
    new_rows = []
    for article in articles:
        object_id = str(article.get("objectID", ""))
        if object_id in existing_ids:
            continue
        new_rows.append(extract_row(article))
        log(f"✅ Queued upload: {article.get('title', '')} | ID: {object_id}")

    if new_rows:
        service.values().append(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A2",
            valueInputOption="RAW",
            body={"values": new_rows}
        ).execute()
        log(f"✅ Uploaded {len(new_rows)} new rows")
    else:
        log("ℹ️ No new rows to upload")

def main():
    log("\n🔄 Starting IMD Scraper Run")
    service = get_sheet_service()
    existing_ids = fetch_existing_object_ids(service)
    articles = fetch_articles()
    upload_articles(service, articles, existing_ids)
    log("✅ Done.")

if __name__ == "__main__":
    main()
