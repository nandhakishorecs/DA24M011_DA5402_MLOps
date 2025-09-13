import os
import time
import requests
import psycopg2
import feedparser
from io import BytesIO
from PIL import Image

# Load environment variables
RSS_FEED_URL = os.getenv("RSS_FEED_URL", "https://www.thehindu.com/news/national/feeder/default.rss")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", 600))  # Default: 10 minutes
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "news_db"),
    "user": os.getenv("POSTGRES_USER", "news_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "securepassword"),
    "host": os.getenv("POSTGRES_HOST", "database"),  # Updated to match service name
    "port": os.getenv("POSTGRES_PORT", "5432"),
}

LAST_FEED_CONTENT = None  # Stores last feed content for change detection

def connect_db():
    """Connect to PostgreSQL database."""
    return psycopg2.connect(**DB_CONFIG)

def fetch_rss_feed():
    """Fetch and parse the RSS feed."""
    global LAST_FEED_CONTENT
    print(f"Fetching RSS feed from {RSS_FEED_URL}...")
    feed = feedparser.parse(RSS_FEED_URL)
    
    if not feed.entries:
        print("No new entries found.")
        return []
    
    feed_content = [(entry.title, entry.link) for entry in feed.entries]
    
    # Check if content has changed
    if feed_content == LAST_FEED_CONTENT:
        print("Feed has not changed since the last fetch. Skipping...")
        return []
    
    LAST_FEED_CONTENT = feed_content  # Update last fetched content
    return feed.entries

def process_and_store_news(entries):
    """Process and store news articles in PostgreSQL."""
    conn = connect_db()
    cur = conn.cursor()

    for entry in entries:
        headline = entry.title
        url = entry.link
        summary = entry.get("summary", "")

        # Fetch the first image (if available)
        thumbnail_data = None
        if "media_content" in entry:
            image_url = entry.media_content[0]['url']
            try:
                response = requests.get(image_url)
                if response.status_code == 200:
                    img = Image.open(BytesIO(response.content))
                    img = img.convert("RGB")
                    img_io = BytesIO()
                    img.save(img_io, format="JPEG")
                    thumbnail_data = img_io.getvalue()
            except Exception as e:
                print(f"Could not fetch image: {e}")

        try:
            cur.execute("""
                INSERT INTO news_articles (headline, url, summary, thumbnail)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING;
            """, (headline, url, summary, psycopg2.Binary(thumbnail_data) if thumbnail_data else None))

            conn.commit()
            print(f"Inserted: {headline}")
        except psycopg2.IntegrityError:
            conn.rollback()
            print(f"Skipping duplicate: {headline}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    while True:
        articles = fetch_rss_feed()
        if articles:
            process_and_store_news(articles)
        print(f"Sleeping for {FETCH_INTERVAL} seconds...\n")
        time.sleep(FETCH_INTERVAL)
