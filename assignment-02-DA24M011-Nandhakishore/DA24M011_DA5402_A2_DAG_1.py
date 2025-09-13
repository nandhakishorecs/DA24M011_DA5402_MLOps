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
import psycopg2
import os

# -------------------------------- Global Variables --------------------------------
url = "https://news.google.com"
sub_section_str = 'Top stories'

# -------------------------------- MODULE 1 --------------------------------
# Module 1 - get HTML content
def module1(url: str, ti):
    ping_responce = requests.get(url+'/')
    ti.xcom_push(key = 'home_page', value = ping_responce.text)

# -------------------------------- MODULE 2 --------------------------------
# Module 2 - get URL for top stories sub section 
def module2(ti): 
    homepage_content = ti.xcom_pull(task_ids = 'Module1', key = 'home_page')
    soup = BeautifulSoup(homepage_content, 'html.parser')
    pattern = sub_section_str
    links = soup.find_all('a')

    for link in links: 
        link_text = link.get_text().strip()
        if(pattern.lower() in link_text.lower()): 
            href = link.get('href')
            if(href != None): 
                # href = f'{url}{href[1 : -1]}'
                href = f'https://news.google.com{href[1:-1]}'
                ti.xcom_push(key = 'sub_page_url', value = href)

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
                    article_url = url + article_url[1:-1]
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
                thumbnail_url = url + thumbnail_url
        
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
def module3(ti): 
    sub_section_url = ti.xcom_pull(task_ids = 'Module2', key = 'sub_page_url')
    
    stories = []
    page = 1
    
    # Wait for lazy loading
    while(page <= 5):
        try:
            response = requests.get(sub_section_url)
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
            print(f"Error fetching page {page}: {error}")
            break
            
    ti.xcom_push(key = 'sub_page_content', value = stories)

# -------------------------------- MODULE 4 and 5 --------------------------------
# Helper function for module 4 and 5
def _jaccard_similarity(document1: str, document2: str) -> float: 
    s1 = set(document1.lower().split())
    s2 = set(document2.lower().split())
    
    intersection = len(s1.intersection(s2))
    union = len(s1.union(s2))

    return intersection/ union if union > 0 else 0

# Module 4 and 5 - store the thumbnail image, headlines and meta data in database after duplication checking
# Done using Postgres and DBeaver - connection request via Airflow 
def module4(ti):
    try: 
        # get data from module 3 
        stories = ti.xcom_pull(task_ids = 'Module3', key = 'sub_page_content')

        # configuration settings to establish connection between DAGs and Postgres 
        connection_parameters = {
            'host': 'host.docker.internal', 
            'port': '54320', 
            'database': 'airflow',
            'user': 'airflow', 
            'password': 'airflow'
        } 

        # SQL Query to create tables in database 
        table_create_query = """
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

        # connect to Postgres Database 
        connection = psycopg2.connect(**connection_parameters)
        cursor = connection.cursor() 

        cursor.execute(table_create_query)

        # counter variable for successfull inserts 
        counter = 0 

        for story in stories:

            # Check for duplicates: 
            cursor.execute(
                "SELECT headline FROM news_headlines"
            )
            current_headlines = cursor.fetchall() 

            # Duplicate checking using Flag variables 
            duplicate = False
            for (current_headline, ) in current_headlines: 
                similarity = _jaccard_similarity(story['Headline'], current_headline)
                if(similarity >= .75): 
                    print("\nDuplicate values of headline found!\n")
                    duplicate = True
                    break
            
            if(duplicate == True): 
                continue

            # Get image data 
            thumbnail_url = story['Thumbnail_url']
            thumbnail_data = requests.get(thumbnail_url)
            thumbnail_data.raise_for_status() 
            thumbnail_image = base64.b64encode(thumbnail_data.content).decode('utf-8')

            # Proceed with Insertion 
            cursor.execute(
                "INSERT INTO news_images (image_data) VALUES (%s) RETURNING image_id", 
                (thumbnail_image, )
            )
            thumbnail_ID = cursor.fetchone()[0]

            cursor.execute("""
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

        # connection commmit        
        connection.commit() 

        # Status check 
        dir_path = '/opt/airflow/dags'
        os.makedirs(dir_path, exist_ok = True)

        with open(os.path.join(dir_path, 'status'), 'w') as file: 
            file.write(str(counter))
    except Exception as error: 
        print(error)
        raise 

    finally: 
        if ('cursor' in locals()):
            cursor.close() 
        if( 'connection' in locals()): 
            connection.close() 

# -------------------------------- DAG parameters --------------------------------
default_args ={
    'owner': 'airflow', 
    'start_date': datetime(2025, 2, 11), 
    'depends_on _past': False
}

# -------------------------------- DAG Configuration --------------------------------
with DAG (
    'Final_Run_DAG1',
    default_args = default_args,
    description = 'Scrap Content from given webpage',
    schedule_interval = '@hourly',  # Run every hour
    catchup = False
)as dag: 
    # Scrape HTML Tags
    task1 = PythonOperator(
        task_id = 'Module1', 
        python_callable = module1, 
        op_kwargs = {'url': url}
    )

    # Get Top Stories URL
    task2 = PythonOperator(
        task_id = 'Module2', 
        python_callable = module2, 
        # no prompted arguements 
    )

    # Get Thumbnail, Headlines, Meta information 
    task3 = PythonOperator(
        task_id = 'Module3', 
        python_callable = module3
        # no prompted arguements 
    )

    # Store the data after checking for duplication in Postgres Database 
    task4 = PythonOperator(
        task_id = 'Module4_5',
        python_callable = module4
    )

    task1 >> task2 >> task3 >> task4