import os
import json
import requests
import logging
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Google Sheet Configuration
SPREADSHEET_ID = "1NQT9pj0P_C-iuwTRANtOXyGM1zVXkhL_b4TuGMxR3qY"
SHEET_NAME = "IMD_News"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Logging
logging.basicConfig(filename="scraper.log", level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = lambda msg: (print(msg), logging.info(msg))

# Algolia API
ALGOLIA_URL = "https://x47r3da403-3.algolianet.com/1/indexes/IMD_WEBSITE/query"
ALGOLIA_HEADERS = {
    "Content-Type": "application/json",
    "X-Algolia-API-Key": "ZjM2ZWEwZjJmNzdjNDE1N2Y0Zjc1ODkyOWRmNDFkM2ZkYjIwNDRjYWEzYzhjNGM3ZDcwNzY5NzU5ZmQ5ZjMwMnZhbGlkVW50aWw9NDE5ODAwNDYwODgmZmlsdGVycz1jYXRlZ29yeSUzQU5ld3M",
    "X-Algolia-Application-Id": "X47R3DA403",
}


def get_sheet_service():
    KEY_FILE_CONTENT = os.getenv("JSON_KEY")
    if not KEY_FILE_CONTENT:
        raise ValueError("Missing environment variable: JSON_KEY")
    creds = service_account.Credentials.from_service_account_info(
        json.loads(KEY_FILE_CONTENT), scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds)


def fetch_articles(max_pages=15):
    hits = []
    for page in range(max_pages):
        res = requests.post(ALGOLIA_URL,
                            headers=ALGOLIA_HEADERS,
                            json={"params": f"query=&hitsPerPage=100&page={page}"})
        if res.status_code != 200:
            log(f"❌ Failed to fetch page {page}: {res.status_code}")
            break
        page_hits = res.json().get("hits", [])
        if not page_hits:
            break
        hits.extend(page_hits)
    log(f"✅ Fetched {len(hits)} articles")
    return hits


def fetch_existing_object_ids(service):
    log("📥 Fetching existing objectIDs from Google Sheet...")
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!E2:E"
    ).execute()
    values = result.get("values", [])
    ids = {row[0].strip() for row in values if row}
    log(f"✅ Loaded {len(ids)} objectIDs from Sheet")
    return ids


def upload_article(service, article):
    title = article.get("title", "")
    date = article.get("publicationDate", "")[:10]
    url = article.get("docLink", "")
    image = article.get("imageURL", "")
    object_id = str(article.get("objectID"))
    summary = article.get("description") or article.get("abstract", "")
    category = article.get("type", "News Stories").capitalize()

    row = [title, date, url, image, object_id, summary, category]
    body = {"values": [row]}
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A2",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()
    log(f"✅ Uploaded: {title} | ID: {object_id} | Category: {category}")


def main():
    log("\n🔄 Starting IMD Scraper Run")
    service = get_sheet_service()
    existing_ids = fetch_existing_object_ids(service)

    uploaded = 0
    for article in fetch_articles():
        object_id = str(article.get("objectID"))
        if object_id not in existing_ids:
            upload_article(service, article)
            uploaded += 1

    log(f"✅ Done. Total new articles uploaded: {uploaded}")


if __name__ == "__main__":
    main()
