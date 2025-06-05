Files:

/4Chan-Crawler/
    chan_client.py:
        has the functions to make API requests to 4chan to retrieve data
    chan_crawler.py:
        continuously makes and consumes jobs of crawling 4Chan boards, threads, and posts
    cold_start_board.py:
        takes a parameter <board>, starts crawling on the corresponding 4Chan board
    chan_db_update:
        takes a parameter <offset> which representes the post number in the database to start updating from
        creates and executes jobs to update 4chan entries from the database


/Reddit-Crawler/
    reddit_client.py:
        has the functions to make API requests to Reddit to retrieve data
    reddit_crawler.py
        continuously makes and consumes jobs of crawling Reddit subreddits, posts, comments
    old_start_listing.py:
        takes a parameter <subreddit> <listing> , starts crawling on the corresponding subreddit board and listing ('hot', 'new')
    database_update.py
         takes a parameter <offset> which representes the post number in the database to start updating from
        creates and executes jobs to update Reddit entries from the database

/datasets/
    artist_dataset.py:
        this file was used to take the two Kaggle datasets and create my own datasets from them
    artist_scan.py:
        this file contains the artist scan functions that are used to parse bodies of text for artist name occurences
    kpop_artists.csv:
        kpop dataset
    popular_artists.csv:
        popular artsits dataset
    

/ArtistGraphs/
    chanTopArtists.py:
        this file connects to the chan_crawler database and lists the artists based on frequency
        this file then takes that data and creates the two 4chan frequency graphs
    redditTopArtists.py:
        this file connects to the reddit_crawler database and lists the artists based on frequency
        this file then takes that data and creates the two Reddit frequency graphs
    totalTopArtists.py:
        this file connects to both the reddit_crawler and chan_crawler database and combined the data
        this file then takes that data and creates the two combined frequency graphs


/ToxicityGraphs/
    chanTopFlagged.py:
        Calculated the toxicity score for 4Chan posts and creates 4Chan CDF graph
    redditTopFlagged.py:
        Calculated the toxicity score for Reddit posts and creates Reddit CDF graph

/PoliticsGraphs/
    politics_comments.py:
        Gets r/politics database data and graphs politics comments per hour
    politics_posts.py:
        Gets r/politics database data and graphs
        politics posts per day
