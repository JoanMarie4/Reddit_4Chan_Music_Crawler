import logging
import requests

#logger setup
logger = logging.getLogger("reddit client")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

class RedditClient:
    API_BASE = "https://www.reddit.com"

    '''
    get listing josn for a given subreddit and listing category
    (list of posts on a reddit page aka hot, new, rising, etc.)
    '''
    def get_listing(self, subreddit, listing):
        #sample api call: https://www.reddit.com/r/Music/new.json
        request_pieces = ["r", subreddit, f"{listing}.json"]
        api_call = self.build_request(request_pieces)
        return self.execute_request(api_call)

    '''
    get comments json from a given subreddit and post
    '''
    def get_comments(self, subreddit, post_ID):
        request_pieces = ["r", subreddit, "comments", f"{post_ID}.json"]
        api_call = self.build_request(request_pieces)
        return self.execute_request(api_call)

    def build_request(self, request_pieces):
        api_call = "/".join([self.API_BASE] + request_pieces)
        return api_call

    def execute_request(self, api_call):
        resp = requests.get(api_call) # <-ADD ERROR HANDLING
        #   ^ ADD ERROR HANDLING ^
        logger.info(resp.status_code)
        json = resp.json() # <-ADD ERROR HANDLING
        #   ^ ADD ERROR HANDLING ^
        #logger.info(f"json: {json}")
        return json
    

if __name__ == "__main__":
    client = RedditClient()
    json = client.get_listing("Music", "new")
    