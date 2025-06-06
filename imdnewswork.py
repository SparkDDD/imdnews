import os
import requests
import logging
from pyairtable import Api

# Configuration
BASE_ID = "appoz4aD0Hjolycwd"
TABLE_ID = "tblNHB2pKfqaofSjw"
FIELD_ID_TITLE = "fldXvVScxrdTJ6Dx3"
FIELD_ID_DATE = "fldWXTXju5UsrFka2"
FIELD_ID_URL = "fldHFkqkFl5DLfv9r"
FIELD_ID_IMAGE = "fldPMpqmEPBJk27Ln"
FIELD_ID_OBJECTID = "fldY7th9xzEaA6zZg"
FIELD_ID_SUMMARY = "fldJdm7QoZBrRd1ZW"
FIELD_ID_CATEGORY = "fldXiWDEl625zpBsz"

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

def fetch_existing_object_ids(api):
    log("📥 Fetching existing objectIDs from Airtable...")
    ids = set()
    offset = None
    while True:
        params = {"pageSize": 100, "fields[]": ["object_id"]}
        if offset:
            params["offset"] = offset
        resp = api.request("get", f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}", params=params)
        for record in resp.get("records", []):
            obj_id = str(record.get("fields", {}).get("object_id", "")).strip()
            if obj_id:
                ids.add(obj_id)
        if not (offset := resp.get("offset")):
            break
    log(f"✅ Loaded {len(ids)} objectIDs from Airtable")
    return ids

def upload_article(api, article):
    fields = {
        FIELD_ID_TITLE: article.get("title", ""),
        FIELD_ID_DATE: article.get("publicationDate", "")[:10],
        FIELD_ID_URL: article.get("docLink", ""),
        FIELD_ID_IMAGE: article.get("imageURL", ""),
        FIELD_ID_OBJECTID: str(article.get("objectID")),
        FIELD_ID_SUMMARY: article.get("description") or article.get("abstract", ""),
        FIELD_ID_CATEGORY: article.get("type", "News Stories")
    }
    api.table(BASE_ID, TABLE_ID).create(fields)
    log(f"✅ Uploaded: {fields[FIELD_ID_TITLE]} | ID: {fields[FIELD_ID_OBJECTID]} | Category: {fields[FIELD_ID_CATEGORY]}")

def main():
    log("\n🔄 Starting IMD Scraper Run")
    api = Api(os.environ["AIRTABLE_API_KEY"])
    existing_ids = fetch_existing_object_ids(api)

    uploaded = 0
    for article in fetch_articles():
        if str(article.get("objectID")) not in existing_ids:
            upload_article(api, article)
            uploaded += 1

    log(f"✅ Done. Total new articles uploaded: {uploaded}")

if __name__ == "__main__":
    main()
