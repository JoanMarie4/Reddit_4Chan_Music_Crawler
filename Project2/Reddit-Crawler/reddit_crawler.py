from reddit_client import RedditClient
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


from dotenv import load_dotenv

logger = logging.getLogger("reddit client")
if not logger.hasHandlers():
    sh = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sh.setFormatter(formatter)
    logger.addHandler(sh)

logger.setLevel(logging.INFO)
load_dotenv()

import os

FAKTORY_SERVER_URL = os.environ.get("FAKTORY_SERVER_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")
MH_API_TOKEN = os.environ.get("MH_API_TOKEN")

def post_ids_from_listing(listing):
    post_IDs = []
    logger.info(f"getting listing data")
    if 'data' in listing:
        data = listing["data"]
        for children in data["children"]:
            post_ID = children["data"]["id"]
            post_IDs.append(post_ID)
        return post_IDs
    else:
        return []


def dead_posts_from_listing(prev_listing_post_IDs, cur_listing_post_IDs):
    dead_post_IDs = set(prev_listing_post_IDs).difference(set(cur_listing_post_IDs))
    return dead_post_IDs

import requests
                
def hs_check_comment(comment):
    data = {
    "token": MH_API_TOKEN,
    "text": comment
    }
    raw_response = requests.post("https://api.moderatehatespeech.com/api/v1/moderate/", json=data)
    if raw_response.status_code != 200:
        logger.error(f"Error: {raw_response.status_code} - {raw_response.text}")
        return None

    try:
        response = raw_response.json()
    except ValueError as e:
        logger.error(f"Failed to parse JSON: {e}, Response: {raw_response.text}")
        return None

    return response

def crawl_comments(subreddit, comment_JSON):
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()

    if comment_JSON and isinstance(comment_JSON, dict):
        time.sleep(10)
        
        replies_json = comment_JSON["data"].get("replies")

        if isinstance(replies_json, dict) and "data" in replies_json:
            for children in replies_json["data"].get("children", []):
                time.sleep(30)
                crawl_comments(subreddit, children)
                
            del comment_JSON["data"]["replies"]

        post_id = comment_JSON["data"]["name"]
        parent_id = comment_JSON["data"]["parent_id"]
        artists = []
        if 'data' in comment_JSON and 'body' in comment_JSON['data']:
            body = comment_JSON['data']['body']
            artists = ArtistsDataset.find_names(body)
            artists = list(set(artists))
            toxicity_JSON = hs_check_comment(body)
            q = "INSERT INTO posts (artists, parent_id, subreddit, post_id, toxicity, data) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id"
            cur.execute(q, (artists, parent_id, subreddit, post_id, toxicity_JSON, comment_JSON))
        else:
            logger.info("No 'body' field found in the comment.")
            q = "INSERT INTO posts (artists, parent_id, subreddit, post_id, data) VALUES (%s, %s, %s, %s, %s) RETURNING id"
            cur.execute(q, (artists, parent_id, subreddit, post_id, comment_JSON))
        conn.commit()
        db_id = cur.fetchone()[0]
        logger.info(f"Inserted DB id: {db_id}")

    cur.close()
    conn.close()


def crawl_post(subreddit, post_ID):
    reddit_client = RedditClient()                                
    post_data = reddit_client.get_comments(subreddit, post_ID)

    logger.info(f"Post: {subreddit}/{post_ID}/:n{post_data}")

    conn = psycopg2.connect(dsn=DATABASE_URL)

    cur = conn.cursor()

    for post in post_data:
        for comment in post["data"]["children"]:
            if(comment != []):
                if(comment["kind"] == "t3"): #for original post
                    post_name = comment["data"]["name"]
                    if 'data' in comment and 'selftext' in comment['data']:
                        logger.info(f"data body is: {comment["data"]["selftext"]}")
                        artists1 = ArtistsDataset.find_names(comment["data"]["selftext"])
                        artists2 = ArtistsDataset.find_names(comment["data"]["title"])
                        artists = artists1 + artists2
                        artists = list(set(artists))
                        toxicity_JSON = toxicity_JSON = hs_check_comment(comment["data"]["selftext"])
                        q = "INSERT INTO posts (artists, parent_id, subreddit, post_id, toxicity, data) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id"
                        cur.execute(q, (artists, post_name, subreddit, post_name, toxicity_JSON, comment))
                    else:
                        artists2 = ArtistsDataset.find_names(comment["data"]["title"])
                        q = "INSERT INTO posts (artists, parent_id, subreddit, post_id, data) VALUES (%s, %s, %s, %s, %s) RETURNING id"
                        cur.execute(q, (artists2, post_name, subreddit, post_name, comment))
                    conn.commit()
                    db_id = cur.fetchone()[0]
                    logger.info(f"Inserted DB id: {db_id}")
                else:
                    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
                        producer = Producer(client=client)
                        # figure out how to use non depcreated methods on your own
                        # run_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=5)
                        run_at = datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=30)
                        run_at = run_at.isoformat()[:-7] + "Z"
                        logger.info(f"run_at = {run_at}")
                        job = Job(
                            jobtype="crawl-comments",
                            args=(subreddit, comment),
                            queue="crawl-comments",
                            at=str(run_at),
                        )
                        producer.push(job)
                                    
    cur.close()

    conn.close()


def crawl_listing(subreddit, listing, prev_listing_post_IDs=[]):
    reddit_client = RedditClient()
    mins = 10
    cur_listing = reddit_client.get_listing(subreddit, listing)
    cur_listing_post_IDs = post_ids_from_listing(cur_listing)
    if(cur_listing_post_IDs != []):
        dead_post_IDs = dead_posts_from_listing(prev_listing_post_IDs, cur_listing_post_IDs)
        logger.info(f"dead threads: {dead_post_IDs}")

        crawl_post_jobs = []
        with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
            producer = Producer(client=client)
            for dead_post_ID in dead_post_IDs:
                time.sleep(5)
                job = Job(
                    jobtype="crawl-post", args=(subreddit, dead_post_ID), queue="crawl-post" 
                )

                crawl_post_jobs.append(job)
            logger.info(f"pushing bulk of crawl-post")
            producer.push_bulk(crawl_post_jobs)
    else:
        mins = 2
        cur_listing_post_IDs = prev_listing_post_IDs
        logger.info(f"listing call returned empty")
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        # figure out how to use non depcreated methods on your own
        # run_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=5)
        run_at = datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=mins)
        run_at = run_at.isoformat()[:-7] + "Z"
        logger.info(f"run_at = {run_at}")
        job = Job(
            jobtype="crawl-listing",
            args=(subreddit, listing, cur_listing_post_IDs),
            queue="crawl-listing",
            at=str(run_at),
        )
        producer.push(job)


if __name__ == "__main__":
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(
            client=client, queues=["crawl-listing", "crawl-post", "crawl-comments"], concurrency=10
        )
        consumer.register("crawl-listing", crawl_listing)
        consumer.register("crawl-post", crawl_post)
        consumer.register("crawl-comments", crawl_comments)
        # tell the consumer to pull jobs off queue and execute them!
        consumer.run()