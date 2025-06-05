from chan_client import ChanClient
import logging
from pyfaktory import Client, Consumer, Job, Producer
import datetime
import psycopg2
import html
# these three lines allow psycopg to insert a dict into
# a jsonb coloumn
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
import sys
sys.path.append("../datasets")
from artist_scan import ArtistsDataset

register_adapter(dict, Json)

# load in function for .env reading
from dotenv import load_dotenv


logger = logging.getLogger("4chan client")

# to ensure no duplicate handlers are added
if not logger.hasHandlers():
    sh = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sh.setFormatter(formatter)
    logger.addHandler(sh)

# Set the logging level
logger.setLevel(logging.INFO)

load_dotenv()

import os

FAKTORY_SERVER_URL = os.environ.get("FAKTORY_SERVER_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")
MH_API_TOKEN = os.environ.get("MH_API_TOKEN")

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
            timeout=10
        )
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return None

    if raw_response.status_code != 200:
        logger.error(f"Error: {raw_response.status_code} - {raw_response.text}")
        return None
    
    if not raw_response.text.strip():
        logger.error(f"Empty response received.")
        return None

    try:
        response = raw_response.json()
    except ValueError as e:
        logger.error(f"Failed to parse JSON: {e}, Response: {raw_response.text}")
        return None

    return response


"""
Return all the thread numbers from a catalog json object
"""
def thread_numbers_from_catalog(catalog):
    thread_numbers = []
    for page in catalog:
        for thread in page["threads"]:
            thread_number = thread["no"]
            thread_numbers.append(thread_number)

    return thread_numbers


"""
Return thread numbers that existed in previous but don't exist
in current
"""
def find_dead_threads(previous_catalog_thread_numbers, current_catalog_thread_numbers):
    dead_thread_numbers = set(previous_catalog_thread_numbers).difference(
        set(current_catalog_thread_numbers)
    )
    return dead_thread_numbers


"""
Crawl a given thread and get its json.
Insert the posts into db
"""
def crawl_thread(board, thread_number):
    chan_client = ChanClient()
    thread_data = chan_client.get_thread(board, thread_number)

    if not thread_data or "posts" not in thread_data:
        logger.error(f"Skipping thread {thread_number} due to missing data.")
        return  # Skip this thread if data is missing       

    # really soould use a connection pool
    conn = psycopg2.connect(dsn=DATABASE_URL)

    cur = conn.cursor()
    # now insert into db
    # iterate through the thread data and get all the post data
    for post in thread_data["posts"]:
        post_number = post.get("no")
        logger.info(f"Post JSON: {post}")
        #filename = post.get('filename')


        
        if not post_number:
            logger.error(f"Post number missing in thread {thread_number}. Skipping this post.")
            continue  # Skip posts without a number

        comment = html.unescape(post.get("com", "No comment")) # Extract the comment (if available)
        artists = ArtistsDataset.find_names(comment)
        toxicity = hs_check_comment(comment)
        name = post.get("name", "Anonymous")  # Extract the poster's name

        # Log each post's details
        logger.info(f"Post Number: {post_number}")
        logger.info(f"Poster Name: {name}")
        logger.info(f"Comment: {comment}")    
        
        try:
            q = """
                INSERT INTO posts (board, thread_number, post_number, artists, toxicity, data) 
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """

            cur.execute(q, (board, thread_number, post_number, artists, toxicity, Json(post)))
            # commit our insert to the database.
            conn.commit()

        # it's often useful to know the id of the newly inserted
        # row. This is so you can launch other jobs that might
        # do additional processing.
        # e.g., to classify the toxicity of a post
            result = cur.fetchone()
            if result:
                db_id = result[0]
                logging.info(f"Inserted DB id: {db_id}")
            else:
                logger.info(f"Post {post_number} already exists. Skipping insertion.")
        except psycopg2.Error as e:
            logger.error(f"Database insertion error for post {post_number}: {e}")
            continue

    # close cursor connection
    cur.close()
    # close connection
    conn.close()


"""
Go out, grab the catalog for a given board, and figure out what threads we need
to collect.

For each thread to collect, enqueue a new job to crawl the thread.

Schedule catalog crawl to run again at some point in the future.
"""


def crawl_catalog(board, previous_catalog_thread_numbers=[]):
    chan_client = ChanClient()

    current_catalog = chan_client.get_catalog(board)

    current_catalog_thread_numbers = thread_numbers_from_catalog(current_catalog)

    dead_threads = find_dead_threads(
        previous_catalog_thread_numbers, current_catalog_thread_numbers
    )
    logger.info(f"dead threads: {dead_threads}")

    # issue the crawl thread jobs for each dead thread
    crawl_thread_jobs = []
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        for dead_thread in dead_threads:
            # see https://github.com/ghilesmeddour/faktory_worker_python/blob/main/src/pyfaktory/models.py
            # what a `Job` looks like
            job = Job(
                jobtype="crawl-thread", args=(board, dead_thread), queue="crawl-thread"
            )

            crawl_thread_jobs.append(job)

        producer.push_bulk(crawl_thread_jobs)

    # Schedule another catalog crawl to happen at some point in future
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        # figure out how to use non depcreated methods on your own
        # run_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=5)
        run_at = datetime.datetime.utcnow() + datetime.timedelta(minutes=5)
        run_at = run_at.isoformat()[:-7] + "Z"
        logger.info(f"run_at = {run_at}")
        job = Job(
            jobtype="crawl-catalog",
            args=(board, current_catalog_thread_numbers),
            queue="crawl-catalog",
            at=str(run_at),
        )
        producer.push(job)


if __name__ == "__main__":
    # we want to pull jobs off the queues and execute them
    # FOREVER (continuously)
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(
            client=client, queues=["crawl-catalog", "crawl-thread"], concurrency=5
        )
        consumer.register("crawl-catalog", crawl_catalog)
        consumer.register("crawl-thread", crawl_thread)
        # tell the consumer to pull jobs off queue and execute them!
        consumer.run()
