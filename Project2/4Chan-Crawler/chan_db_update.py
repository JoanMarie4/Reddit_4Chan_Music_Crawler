from chan_client import ChanClient
import logging
from pyfaktory import Client, Consumer, Job, Producer
import datetime
import psycopg2 
import time
import sys
sys.path.append("../datasets")
from artist_scan import ArtistsDataset
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
register_adapter(dict, Json)
import html


from dotenv import load_dotenv

logger = logging.getLogger("chan client")
if not logger.hasHandlers():
    sh = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sh.setFormatter(formatter)
    logger.addHandler(sh)

logger.setLevel(logging.INFO)
load_dotenv()

import os

import requests
                
def hs_check_comment(comment):
    data = {
        "token": MH_API_TOKEN,
        "text": comment
    }
    try:
        raw_response = requests.post(
            "https://api.moderatehatespeech.com/api/v1/moderate/", 
            json=data,
            timeout=10  # Add a timeout to handle unresponsive requests
        )
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return None

    if raw_response.status_code != 200:
        logger.error(f"Error: {raw_response.status_code} - {raw_response.text}")
        return None

    # Validate and parse the response
    if not raw_response.text.strip():
        logger.error(f"Empty response received.")
        return None

    try:
        response = raw_response.json()
    except ValueError as e:
        logger.error(f"Failed to parse JSON: {e}, Response: {raw_response.text}")
        return None

    return response


FAKTORY_SERVER_URL = os.environ.get("FAKTORY_SERVER_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")
MH_API_TOKEN = os.environ.get("MH_API_TOKEN")
LIMIT = 200

def update_posts(rows):
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()
    artists = []
    for row in rows:
        info = row[1]
        post_ID = info['post_number']
        json = info['data']
        logger.info(f"updating post {post_ID}")
        toxicity = ""
        comment = html.unescape(json.get("com", "No comment"))
        artists = ArtistsDataset.find_names(comment)
        toxicity = hs_check_comment(comment)
        q = "UPDATE posts SET artists = %s, toxicity = %s WHERE post_number = %s;"
        cur.execute(q, (artists, toxicity, post_ID))    
      
    cur.close()
    conn.close()

import psycopg2

def fetch_paginated_posts(offset):
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()


        # Fetch a batch of rows
    query = """
    SELECT id AS post_id, 
            json_build_object('post_number', post_number, 'data', data) AS json_data
    FROM posts
    ORDER BY id
    LIMIT %s OFFSET %s;
    """
    cur.execute(query, (LIMIT, offset))
    rows = cur.fetchall()

    # Break the loop if no more rows are returned
    if not rows:
        logger.info(f'No rows returned from fetch_paginated_posts')
        
    cur.close()
    conn.close()

    return rows

def update_chan_posts(offset):
    rows = fetch_paginated_posts(offset)
    update_posts(rows)
    offset = int(offset)
    offset += LIMIT
    logger.info(f"new offset: {offset}")
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        run_at = datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=2)
        run_at = run_at.isoformat()[:-7] + "Z"
        logger.info(f"run_at = {run_at}")
        job = Job(
            jobtype="update_chan_posts", 
            args=([offset]), 
            queue='update_chan_posts', 
            at=str(run_at),)
        producer.push(job)

if __name__ == "__main__":
    offset = sys.argv[1]
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        job = Job(jobtype="update_chan_posts", args=[offset], queue='update_chan_posts')
        producer.push(job)
    
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(
            client=client, queues=["update_chan_posts"], concurrency=10
        )
        consumer.register("update_chan_posts", update_chan_posts)
        consumer.run()

