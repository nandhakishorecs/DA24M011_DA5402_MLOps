from fastapi import FastAPI, Request, Query
from fastapi.templating import Jinja2Templates
import psycopg2
import os
import base64
from datetime import date
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Database connection
def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
    )

@app.get("/")
def home(request: Request):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT headline, url, summary, thumbnail
        FROM news_articles
        WHERE date(published_at) = CURRENT_DATE
        ORDER BY published_at DESC
    """)
    articles = cur.fetchall()
    conn.close()

    # Format articles properly
    formatted_articles = []
    for article in articles:
        headline, url, summary, thumbnail = article
        if isinstance(thumbnail, memoryview):  # Convert binary images to Base64
            thumbnail = f"data:image/png;base64,{base64.b64encode(thumbnail.tobytes()).decode()}"
        formatted_articles.append((headline, url, summary, thumbnail))

    # Pass your name and roll number to the template
    return templates.TemplateResponse("index.html", {
        "request": request,
        "articles": formatted_articles,
        "name": "Nandhakishore CS",  # Replace with your actual name
        "roll_number": "DA24M011"    # Replace with your actual roll number
    })
