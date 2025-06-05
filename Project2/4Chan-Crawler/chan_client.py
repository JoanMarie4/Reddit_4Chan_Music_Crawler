import logging
import requests
import pandas as pd
import html
# Logger setup
logger = logging.getLogger("4chan client")
logger.propagate = False

# to Ensure no duplicate handlers are added
if not logger.hasHandlers():
    sh = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sh.setFormatter(formatter)
    logger.addHandler(sh)

# Set the logging level
logger.setLevel(logging.INFO)

class ChanClient:
    API_BASE = "http://a.4cdn.org"

    def get_thread(self, board, thread_number):
        logger.info(f"Fetching thread {thread_number} from board {board}")
        api_call = self.build_request([board, "thread", f"{thread_number}.json"])
        return self.execute_request(api_call)

    def get_catalog(self, board):
        logger.info(f"Fetching catalog for board {board}")
        api_call = self.build_request([board, "catalog.json"])
        return self.execute_request(api_call)

    def build_request(self, request_pieces):
        api_call = "/".join([self.API_BASE] + request_pieces)
        return api_call

    def execute_request(self, api_call):
        try:
            resp = requests.get(api_call)
           
            if resp.status_code == 200:
                json_data = resp.json()
                return json_data
            else:
                logger.error(f"Failed API call: {api_call} with status code {resp.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error executing request: {e}")
            return None

if __name__ == "__main__":
    client = ChanClient()

    catalog_json = client.get_catalog("mu")

    if catalog_json:
        thread_numbers = []

        # Extract thread numbers from the catalog
        for page in catalog_json:
            if "threads" in page:
                for thread in page["threads"]:
                    thread_number = thread["no"]
                    thread_numbers.append(thread_number)
                    logger.info(f"Thread ID: {thread_number}")

        # Fetch and process each thread
        for thread_id in thread_numbers:
            thread_json = client.get_thread("mu", thread_id)
            if thread_json:
                client.process_posts(thread_json)
            else:
                logger.error(f"Failed to fetch data for thread ID: {thread_id}")
    else:
        logger.error("Failed to fetch the catalog.")
