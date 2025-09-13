# This DAG file contains Module 6 
# -------------------------------- Libraries Import --------------------------------
# Libraries for Module6
# Access email and format contact 
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json

# Date time utility
from datetime import datetime, timedelta

# For DAGs
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Postgres connector 
import psycopg2

# File handling 
import os

# -------------------------------- Configuration for access GMail --------------------------------
# Configuration to send notification email using DAGs
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
# Enter the email ID from which you have to send notifications 
SENDER_EMAIL = "" 
# Add the token from 2FA of the sender email 
PASSWORD = "sjgmdgvxlknhocwr"  
# Enter the email ID from which has to recieve notifications 
RECIPIENT_EMAIL = ""

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
    # Check for changes in database and send email 
    connection_params = {
        # Borrowed server configurations - as same as DAG1
        "host": "host.docker.internal",
        "port": "54320",
        "database": "airflow",
        "user": "airflow",
        "password": "airflow"
    }
    
    try:
        # Get previous state
        prev_state = _load_previous_state()
        last_headline_id = prev_state['last_headline_id']
        
        # Connect to database
        connection = psycopg2.connect(**connection_params)
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

# -------------------------------- DAG parameters --------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 1),
}

# -------------------------------- DAG Configuration --------------------------------
with DAG(
    'Final_Run_DAG2',
    default_args = default_args,
    description = 'Send email notifications for new articles',
    # Match DAG1's schedule
    schedule_interval = '@hourly',  
    catchup = False
) as dag:
    # Wait for DAG1 to complete
    wait_for_dag1 = ExternalTaskSensor(
        task_id = 'wait_for_scraping',
        # The external tag ID should be same as DAG1
        external_dag_id = 'Final_Run_DAG1',
        # Wait for the last task of DAG1
        external_task_id = 'Module4_5',  
        timeout = 600,
        poke_interval = 60
    )
    
    # Wait for status file
    wait_for_status = FileSensor(
        task_id = 'wait_for_status_file',
        # Use 'file' as connection ID - cope this from DAG Dashboard - admin - connections 
        fs_conn_id = 'tutorial_pg_conn',  
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