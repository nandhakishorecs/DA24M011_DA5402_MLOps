#   Author: Nandhakishore C S
#   Roll number: DA24M011
# This DAG file contains Module 1 to Module 5 (in code Module 4 and 5 are collapsed together)
# -------------------------------- Libraries Import --------------------------------

# Libraries for Module1
import requests 
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Libraries for Module2
from bs4 import BeautifulSoup

# Libraries for Module3
import bs4
import base64
from datetime import datetime, timedelta
import time

# Libraries for Module4 and Module5 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

# Libraries for Module 6
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from difflib import SequenceMatcher

# -------------------------------- Global Variables --------------------------------
url = "https://news.google.com"
sub_section_str = 'Top stories'
postgres_connection_ID = 'tutorial_pg_conn'

# -------------------------------- MODULE 1 --------------------------------
# Module 1 - get HTML content
def module1(**kwargs):
    response = requests.get(url)
    if response.status_code == 200:
        kwargs['ti'].xcom_push(key='home_page', value=response.text)
    else:
        raise Exception(f"\nFailed to scrape:\t {url}\n")

# -------------------------------- MODULE 2 --------------------------------
# Module 2 - get URL for top stories sub section 
def module2(**kwargs):
    ti = kwargs['ti']
    html = ti.xcom_pull(task_ids='sub_section_url', key='home_page')
    soup = BeautifulSoup(html, 'html.parser')
    # top_story = soup.find('a', {'aria-label': 'Top stories'})
    top_story = soup.find_all('a')
    pattern = sub_section_str

    for link in top_story: 
        link_text = link.get_text().strip()
        if(pattern.lower() in link_text.lower()): 
            href = link.get('href')
            if(href is not None): 
                # href = f'{url}{href[1 : -1]}'
                top_stories_url = f'https://news.google.com{href[1:-1]}'
                ti.xcom_push(key='top_stories_url', value=top_stories_url)

# -------------------------------- MODULE 3 --------------------------------
# Helper function for module3
def _extract_story_data(article: bs4.element.Tag) -> dict:
    try:
        # Extract headline and URL first
        headline = None
        for a_tag in article.find_all('a'):
            if(a_tag.get_text().strip() is not None):
                headline = a_tag.get_text().strip()
                article_url = a_tag.get('href')
                if((article_url) and (not article_url.startswith('http'))):
                    # article_url = f"https://news.google.com{article_url[1:-1]}"
                    article_url = "https://news.google.com" + article_url[1:-1]
                else: 
                    break
        
        if(not headline):
            return None
        
        # Extract thumbnail
        thumbnail_url = None

        # Try figure tag first
        figure = article.find('figure')
        if(figure is not None):
            img = figure.find('img')
            if(img is not None):
                thumbnail_url = img.get('src') or img.get('data-src')
                # thumbnail_url = f"https://news.google.com{thumbnail_url}"
                thumbnail_url = "https://news.google.com" + thumbnail_url
        
        # If no image found, skip this story
        if(not thumbnail_url): return
        
        # Extract and convert publication date
        time_elem = article.find('time')
        relative_time = time_elem.get_text().strip() if time_elem else None
        pub_date = _get_time(relative_time) if relative_time else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return {
            'Headline': headline,
            'Thumbnail_url': thumbnail_url,
            'Article_url': article_url,
            'Date': pub_date
        }
        
    except Exception as error:
        print(f"Error:\t {error}")
        return
    
# Helper function for module 3    
def _get_time(relative_time: str) -> str:
    try:
        now = datetime.now()
        
        if 'minute' in relative_time or 'minutes' in relative_time:
            minutes = int(relative_time.split()[0])
            return (now - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
        
        elif 'hour' in relative_time or 'hours' in relative_time:
            hours = int(relative_time.split()[0])
            return (now - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        
        elif 'day' in relative_time or 'days' in relative_time:
            days = int(relative_time.split()[0])
            return (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        
        elif 'Yesterday' in relative_time:
            return (now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        
        else:
            return now.strftime("%Y-%m-%d %H:%M:%S")
            
    except Exception:
        return now.strftime("%Y-%m-%d %H:%M:%S")

# Module 3 - get thumbnail, headlines and META data
def module3(**kwargs):
    ti = kwargs['ti']
    top_stories_url = ti.xcom_pull(task_ids='extract_top_stories', key='top_stories_url')
    
    stories = []
    page = 1
    while(page <= 5):
        try: 
            response = requests.get(top_stories_url)
            response.raise_for_status() 
            soup = BeautifulSoup(response.text, 'html.parser')
            articles = soup.find_all('article')

            for article in articles:
                story = _extract_story_data(article)
                if story and story not in stories:
                    stories.append(story)

            if ((not articles) or (len(articles) == 0)):
                break
            
            page += 1
            time.sleep(5)

        except requests.RequestException as error: 
            print(f'Error fetching {page}: {error}')
            break
    
    ti.xcom_push(key='news_data', value=stories)

def _jaccard_similarity(document1: str, document2: str) -> float: 
    s1 = set(document1.lower().split())
    s2 = set(document2.lower().split())
    
    intersection = len(s1.intersection(s2))
    union = len(s1.union(s2))

    return intersection/ union if union > 0 else 0

def is_similar(new_headline, existing_headlines):
    for headline in existing_headlines:
        if SequenceMatcher(None, new_headline, headline).ratio() > 0.85:
            return True
    return False

def module4(**kwargs):
    try:
        ti = kwargs['ti']
        news_data = ti.xcom_pull(task_ids='extract_news_data', key='news_data')
        pg_hook = PostgresHook(postgres_conn_id = postgres_connection_ID)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        # counter variable for successfull inserts 
        counter = 0 

        for story in news_data:
            cursor.execute(
                "SELECT Headline FROM news_headlines"
            )
            current_headlines = cursor.fetchall()

            duplicate = False 
            for (current_headline, ) in current_headlines: 
                # similarity = is_similar(story['Headline'], current_headline)
                # if(similarity is not True):
                #     print("\nDuplicate values of headline found!\n")
                #     duplicate = True
                #     break
                similarity = _jaccard_similarity(story['Headline'], current_headline)
                if(similarity >= .75):
                    print("\nDuplicate values of headline found!\n")
                    duplicate = True
                    break
            
            if(duplicate == True): 
                continue

            thumbnail_url = story['Thumbnail_url']
            thumbnail_data = requests.get(thumbnail_url)
            thumbnail_data.raise_for_status() 
            thumbnail_image = base64.b64encode(thumbnail_data.content).decode('utf-8')

            cursor.execute(
                "INSERT INTO news_images (image_data) VALUES (%s) RETURNING image_id", 
                (thumbnail_image,)
            )

            thumbnail_ID = cursor.fetchone()[0]

            cursor.execute(
                """
                    INSERT INTO news_headlines 
                    (image_id, headline, thumbnail_url, article_url, publication_date, scrape_timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (headline, article_url) DO NOTHING
                    RETURNING headline_id
                """, 
                (
                    thumbnail_ID, 
                    story['Headline'], 
                    story['Thumbnail_url'],
                    story['Article_url'],
                    story['Date'], 
                    datetime.now()
                )
            )  
            
            if (cursor.fetchone() is not None):
                counter += 1
            else: 
                cursor.execute(
                    "DELETE FROM news_images WHERE image_id = %s", (thumbnail_ID, )
                )
        # connection commit    
        connection.commit() 

        # Status check 
        dir_path = '/opt/airflow/dags'
        os.makedirs(dir_path, exist_ok = True)

        with open(os.path.join(dir_path, 'status'), 'w') as file: 
            file.write(str(counter))  
            
    except Exception as error: 
        print(error)
        raise 

# -------------------------------- Configuration for access GMail --------------------------------
# Configuration to send notification email using DAGs
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = "" # sender email 
PASSWORD = "" # send 2FA pass key
RECIPIENT_EMAIL = "" # reciepient email 

# -------------------------------- Helper Function to do Module 6 --------------------------------
def _send_email(message):
    # Send email using SMTP with App Password
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            # Using App Password here - get token from GMail 2FA
            server.login(SENDER_EMAIL, PASSWORD)  
            server.send_message(message)
            print("Email sent successfully")
    except Exception as error:
        print(f"Failed to send email: {error}")
        raise

# -------------------------------- Helper Function to do Module 6 --------------------------------
def _load_previous_state():
    # Load previous database content - the pnultimate state from a JSON file
    try:
        with open('/opt/airflow/dags/previous_state.json', 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {'last_headline_id': 0}
    
# -------------------------------- Helper Function to do Module 6 --------------------------------
def save_current_state(state):
    # Save current database state to a JSON file
    with open('/opt/airflow/dags/previous_state.json', 'w') as file:
        json.dump(state, file)

# -------------------------------- Helper Function to do Module 6 --------------------------------
def _compose_email(new_entries):
    # To email the new changes compose an email with content from new entries
    message = MIMEMultipart()
    message["Subject"] = f"New Articles Alert: {len(new_entries)} new entries"
    message["From"] = SENDER_EMAIL
    message["To"] = RECIPIENT_EMAIL

    if(len(new_entries) > 0): 
        content = "New articles have been added!:\n"
        for entry in new_entries:
            content += f"• {entry['headline']}\n"
            if entry.get('publication_date'):
                content += f"Published on\t: {entry['publication_date']}\n"
            content += f" URL: {entry['article_url']}\n\n"

        message.attach(MIMEText(content, "plain"))
    else: 
        content = 'No new top stories!\n'
        message.attach(MIMEText(content, "plain"))
    return message

# -------------------------------- Main Function for Module 6 --------------------------------
def module6(**context):
    try:
        # Get previous state
        prev_state = _load_previous_state()
        last_headline_id = prev_state['last_headline_id']
        
        # Connect to database
        pg_hook = PostgresHook(postgres_conn_id = postgres_connection_ID)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        # Get new entries - SQL Query 
        cursor.execute(
            """
                SELECT h.headline_id, h.Headline, h.Article_url, h.publication_date
                FROM news_headlines h
                WHERE h.headline_id > %s
                ORDER BY h.headline_id
            """, 
            (last_headline_id, )
        )
        
        new_entries = []
        max_headline_id = last_headline_id
        
        for row in cursor.fetchall():
            headline_id, headline, article_url, pub_date = row
            max_headline_id = max(max_headline_id, headline_id)
            new_entries.append({
                'headline': headline,
                'article_url': article_url,
                'publication_date': pub_date.isoformat() if pub_date else None
            })
        
        if (new_entries is not None):
            # Prepare and send email
            message = _compose_email(new_entries)
            _send_email(message)
            
            # Update state
            save_current_state({'last_headline_id': max_headline_id})
            
        # Clean up status file
        os.remove('/opt/airflow/dags/status')
            
    except Exception as error:
        print(f"Error in sending email: {error}")
        raise

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
            
# -------------------------------- Define default_args for DAGs --------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 18),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# -------------------------------- DAG Configuration --------------------------------
with DAG('DAG_1_FINAL', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag1:
    scrape_task = PythonOperator(task_id='sub_section_url', python_callable=module1, provide_context=True)
    extract_task = PythonOperator(task_id='extract_top_stories', python_callable=module2, provide_context=True)
    get_news_task = PythonOperator(task_id='extract_news_data', python_callable=module3, provide_context=True)
    create_table_task = PostgresOperator(
        task_id = 'create_tables',
        postgres_conn_id = postgres_connection_ID, 
        sql= """
            DO $$ 
            BEGIN
                -- Create news_images table if it doesn't exist
                IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'news_images') THEN
                    CREATE TABLE news_images (
                        image_id SERIAL PRIMARY KEY,
                        image_data BYTEA NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                END IF;

                -- Create news_headlines table if it doesn't exist
                IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'news_headlines') THEN
                    CREATE TABLE news_headlines (
                        headline_id SERIAL PRIMARY KEY,
                        image_id INTEGER REFERENCES news_images(image_id),
                        headline TEXT NOT NULL,
                        thumbnail_url TEXT,
                        article_url TEXT,
                        publication_date TIMESTAMP,
                        scrape_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(headline, article_url)
                    );
                END IF;
            END $$;
        """
    )
    store_data_task = PythonOperator(task_id='store_news_data', python_callable=module4, provide_context=True)
    create_table_task >> scrape_task >> extract_task >> get_news_task >> store_data_task


with DAG('DAG_2_FINAL', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag2:
    wait_for_dag1 = ExternalTaskSensor(
        task_id = 'wait_for_scraping',
        # The external tag ID should be same as DAG1
        external_dag_id = 'DAG_1_FINAL',
        # Wait for the last task of DAG1
        external_task_id = 'store_news_data',  
        timeout = 600,
        poke_interval = 60
    )
    
    # Wait for status file
    wait_for_status = FileSensor(
        task_id = 'wait_for_status_file',
        # Use 'file' as connection ID - cope this from DAG Dashboard - admin - connections 
        fs_conn_id = postgres_connection_ID,  
        # For macbook, the opt arguement is required. 
        filepath = '/opt/airflow/dags/status',
        poke_interval = 30,
        timeout = 600
    )
    
    # Process new entries and send email
    notify_task = PythonOperator(
        task_id = 'Module6',
        python_callable = module6,
        provide_context = True
    )
    
    wait_for_dag1 >> wait_for_status >> notify_task