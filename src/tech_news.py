"""
tech_news_to_drive.py
---------------------
Fetch latest tech‐industry news and push the file to Google Drive.

SETUP
-----
1. pip install newsapi-python google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
2. Get a free NewsAPI key → https://newsapi.org.
3. Enable Google Drive API in your Google Cloud project.
4. Download OAuth2 credentials as 'credentials.json' (OAuth client type: Desktop).
5. Run once → browser opens → grant Drive access → a token.json cache is stored.

USAGE
-----
export NEWS_API_KEY="your_newsapi_key"
python tech_news_to_drive.py
"""

import csv
import os
import pathlib
import datetime as dt

from newsapi import NewsApiClient
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import google.oauth2.credentials as creds_mod

# ────────────────────────────────
# 1. CONFIG
# ────────────────────────────────
NEWS_API_KEY   = os.environ["NEWS_API_KEY"]          # raise KeyError if not set
DRIVE_FOLDER_PATH = "hackathon/data/industry"        # path inside your Drive
LOCAL_DIR       = pathlib.Path("tmp_news")           # local scratch
LOCAL_DIR.mkdir(exist_ok=True)

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# ────────────────────────────────
# 2. GOOGLE DRIVE AUTH (token cache)
# ────────────────────────────────
def get_drive_service():
    token_path = "token.json"
    creds = None
    if pathlib.Path(token_path).exists():
        creds = creds_mod.Credentials.from_authorized_user_file(token_path, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w") as f:
            f.write(creds.to_json())
    return build("drive", "v3", credentials=creds)

drive = get_drive_service()

# Helper: ensure nested folders exist; returns folderId of deepest
def ensure_drive_path(path: str) -> str:
    parent_id = "root"
    for part in path.strip("/").split("/"):
        query = (
            f"'{parent_id}' in parents and name = '{part}' "
            "and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        )
        res = drive.files().list(q=query, fields="files(id, name)").execute()
        if res["files"]:
            parent_id = res["files"][0]["id"]
        else:
            meta = {"name": part,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [parent_id]}
            parent_id = drive.files().create(body=meta, fields="id").execute()["id"]
    return parent_id

folder_id = ensure_drive_path(DRIVE_FOLDER_PATH)

# ────────────────────────────────
# 3. FETCH TECH NEWS
# ────────────────────────────────
newsapi = NewsApiClient(api_key=NEWS_API_KEY)
articles = newsapi.get_top_headlines(category="technology", language="en", page_size=20)["articles"]

ts  = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
csv_name = f"tech_news_{ts}.csv"
csv_path = LOCAL_DIR / csv_name

with csv_path.open("w", newline="", encoding="utf-8") as fp:
    w = csv.writer(fp)
    w.writerow(["publishedAt", "source", "title", "url"])
    for a in articles:
        w.writerow([a["publishedAt"], a["source"]["name"], a["title"], a["url"]])

print(f"Saved {csv_path.relative_to(pathlib.Path.cwd())}")

# ────────────────────────────────
# 4. UPLOAD / OVERWRITE ON DRIVE
# ────────────────────────────────
# If a previous tech_news_*.csv exists, we keep it (versioned file names).
media = MediaFileUpload(csv_path, mimetype="text/csv")
drive.files().create(
    body={"name": csv_name, "parents": [folder_id]},
    media_body=media,
    fields="id"
).execute()

print(f"Uploaded '{csv_name}' to Drive folder '{DRIVE_FOLDER_PATH}' (id={folder_id})")
