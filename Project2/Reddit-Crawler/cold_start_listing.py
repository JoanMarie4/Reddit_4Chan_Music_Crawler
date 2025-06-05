import logging
from pyfaktory import Client, Consumer, Job, Producer
import time
import random
import sys
from dotenv import load_dotenv
import os

logger = logging.getLogger("reddit faktory test")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(mame)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

load_dotenv()

if __name__ == "__main__":
    subreddit = sys.argv[1]
    listing = sys.argv[2]
    print(f"Cold starting {listing} lising call for subreddit {subreddit}")
    faktory_server_url = os.environ.get("FAKTORY_SERVER_URL")

    with Client(faktory_url=faktory_server_url, role="producer") as client:
        producer = Producer(client=client)
        job = Job(jobtype="crawl-listing", args=(subreddit, listing,), queue='crawl-listing')
        producer.push(job)
        