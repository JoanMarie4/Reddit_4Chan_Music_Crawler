import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from dotenv import load_dotenv
import os

load_dotenv()

REDDIT_DATABASE = os.environ.get("REDDIT_DATABASE")
CHAN_DATABASE = os.environ.get("4CHAN_DATABASE")

def get_top_artists_from_reddit():
    conn = psycopg2.connect(dsn=REDDIT_DATABASE)
    cur = conn.cursor()

    kpop_csv = pd.read_csv('kpop_artists.csv')
    pop_csv = pd.read_csv('popular_artists.csv')

    kpop_csv = kpop_csv.rename(columns={'Gender.x': 'gender'})
    kpop_csv = kpop_csv.rename(columns={'Stage.Name': 'artist'})
    kpop_csv['gender'] = kpop_csv['gender'].replace({'Boy': 'male', 'Girl': 'female'})
    group_gender = kpop_csv[['Group', 'gender']].drop_duplicates(subset=['Group'])
    group_gender.columns = ['artist', 'gender']
    kpop_csv = kpop_csv[['artist', 'gender']]
    pop_csv = pop_csv[['artist', 'gender']]

    gender_data = pd.concat([kpop_csv, pop_csv, group_gender], ignore_index=True)

    # Reddit query
    q = '''
    WITH artist_mentions AS (
        SELECT 
            artist,
            post_id
        FROM 
            posts, 
            UNNEST(artists) AS artist
        UNION ALL
        SELECT 
            artist,
            p.post_id
        FROM 
            posts p
        JOIN 
            posts parent ON p.parent_id = parent.post_id,
            UNNEST(parent.artists) AS artist
    ),
    subreddit_posts AS (
        SELECT COUNT(*) AS total_posts
        FROM posts
        WHERE subreddit IN ('popheads', 'music')
    )
    SELECT 
        artist,
        COUNT(*) AS post_count,
        COUNT(*) * 1.0 / (SELECT total_posts FROM subreddit_posts) AS frequency
    FROM 
        artist_mentions
    WHERE 
        post_id IN (SELECT post_id FROM posts WHERE subreddit IN ('popheads', 'music'))
    GROUP BY 
        artist
    ORDER BY 
        frequency DESC
    LIMIT 40;
    '''

    cur.execute(q)
    result = cur.fetchall()
    df = pd.DataFrame(result)
    df.columns = ['artist', 'num_posts', 'frequency']df = df.merge(gender_data, on='artist', how='left')
    
    # Close connection
    cur.close()
    conn.close()
    
    return df

def get_top_artists_from_4chan():
    conn = psycopg2.connect(dsn=CHAN_DATABASE)
    cur = conn.cursor()

    kpop_csv = pd.read_csv('kpop_artists.csv')
    pop_csv = pd.read_csv('popular_artists.csv')

    kpop_csv = kpop_csv.rename(columns={'Gender.x': 'gender'})
    kpop_csv = kpop_csv.rename(columns={'Stage.Name': 'artist'})
    kpop_csv['gender'] = kpop_csv['gender'].replace({'Boy': 'male', 'Girl': 'female'})
    group_gender = kpop_csv[['Group', 'gender']].drop_duplicates(subset=['Group'])
    group_gender.columns = ['artist', 'gender']
    kpop_csv = kpop_csv[['artist', 'gender']]
    pop_csv = pop_csv[['artist', 'gender']]

    gender_data = pd.concat([kpop_csv, pop_csv, group_gender], ignore_index=True)

    # 4chan query
    q = '''
    WITH artist_mentions AS (
        SELECT 
            artist,
            post_number
        FROM 
            posts, 
            UNNEST(artists) AS artist
        UNION ALL
        SELECT 
            artist,
            p.post_number
        FROM 
            posts p
        JOIN 
            posts parent ON p.thread_number = parent.thread_number,
            UNNEST(parent.artists) AS artist
    ),
    board_posts AS (
        SELECT COUNT(*) AS total_posts
        FROM posts
        WHERE board = 'mu'
    )
    SELECT 
        artist,
        COUNT(*) AS post_count,
        COUNT(*) * 1.0 / (SELECT total_posts FROM board_posts) AS frequency
    FROM 
        artist_mentions
    WHERE 
        post_number IN (SELECT post_number FROM posts WHERE board = 'mu')
    GROUP BY 
        artist
    ORDER BY 
        frequency DESC
    LIMIT 60;
    '''

    cur.execute(q)
    result = cur.fetchall()
    df = pd.DataFrame(result)
    df.columns = ['artist', 'num_posts', 'frequency']

    # Merge with gender data
    df = df.merge(gender_data, on='artist', how='left')

    # Close connection
    cur.close()
    conn.close()

    return df

if __name__ == "__main__":
    # Fetch data from both Reddit and 4chan
    reddit_df = get_top_artists_from_reddit()
    chan_df = get_top_artists_from_4chan()

    # Combine both dataframes
    combined_df = pd.concat([reddit_df, chan_df], ignore_index=True)

    # Ensure 'frequency' column is numeric
    combined_df['frequency'] = pd.to_numeric(combined_df['frequency'], errors='coerce')

    # Drop duplicates and get top 20 artists based on frequency
    top20_df = combined_df.drop_duplicates(subset='artist').nlargest(20, 'frequency')


    # Handle missing gender values
    top20_df['gender'] = top20_df['gender'].fillna('unknown')
    color_map = {'male': 'blue', 'female': 'pink', 'unknown': 'gray'}
    bar_colors = top20_df['gender'].map(color_map).fillna('gray')

    # GRAPH of the top 20 artists based on frequency
    plt.figure(figsize=(14, 8))
    plt.barh(top20_df['artist'], top20_df['frequency'], color=bar_colors)
    plt.xlabel('Frequency')
    plt.ylabel('Artists')
    plt.title('Top 20 Artists by Frequency (Reddit + 4Chan)')

    # Add a legend
    blue_patch = mpatches.Patch(color='blue', label='Male')
    pink_patch = mpatches.Patch(color='pink', label='Female')
    plt.legend(handles=[blue_patch, pink_patch])

    plt.savefig('Top20_Artist_Frequencies_Combined.png')
    plt.show()

    # GRAPH of top 10 male and female artists
    top_males_df = combined_df[combined_df['gender'] == 'male'].nlargest(10, 'frequency')
    top_females_df = combined_df[combined_df['gender'] == 'female'].nlargest(10, 'frequency')

    # Concatenate the two DataFrames
    top_gender_df = pd.concat([top_males_df, top_females_df])

    plt.figure(figsize=(14, 8))
    plt.barh(top_gender_df['artist'], top_gender_df['frequency'], color=top_gender_df['gender'].map(color_map))
    plt.xlabel('Frequency')
    plt.ylabel('Artists')
    plt.title('Top 10 Male and Top 10 Female Artists by Frequency (Reddit + 4Chan)')

    # Add a legend
    plt.legend(handles=[blue_patch, pink_patch])

    plt.savefig('Top_10_Male_Female_Artist_Frequencies_Combined.png')
    plt.show()