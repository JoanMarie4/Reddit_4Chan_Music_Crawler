import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from dotenv import load_dotenv
import os

load_dotenv()

CHAN_DATABASE = os.environ.get("4CHAN_DATABASE")

if __name__ == "__main__":
    conn = psycopg2.connect(dsn=CHAN_DATABASE)
    cur = conn.cursor()

    kpop_csv = pd.read_csv('../datasets/kpop_artists.csv')
    pop_csv = pd.read_csv('../datasets/popular_artists.csv')

    kpop_csv = kpop_csv.rename(columns={'Gender.x': 'gender'})
    kpop_csv = kpop_csv.rename(columns={'Stage.Name': 'artist'})
    kpop_csv['gender'] = kpop_csv['gender'].replace({'Boy': 'male', 'Girl': 'female'})
    group_gender = kpop_csv[['Group', 'gender']]
    group_gender = group_gender.drop_duplicates(subset=['Group'])
    group_gender.columns = ['artist', 'gender']
    kpop_csv = kpop_csv[['artist', 'gender']]
    #print(kpop_csv)
    pop_csv = pop_csv[['artist', 'gender']]

    gender_data = pd.concat([kpop_csv, pop_csv, group_gender], ignore_index=True)

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
            posts parent ON p.thread_number = parent.thread_number,  -- Matching posts within the same thread
            UNNEST(parent.artists) AS artist
    ),
    board_posts AS (
        SELECT COUNT(*) AS total_posts
        FROM posts
        WHERE board = 'mu'  -- Assuming the board column in 4chan posts is named 'board'
    )
    SELECT 
        artist,
        COUNT(*) AS post_count,
        COUNT(*) * 1.0 / (SELECT total_posts FROM board_posts) AS frequency
    FROM 
        artist_mentions
    WHERE 
        post_number IN (SELECT post_number FROM posts WHERE board = 'mu')  -- Filter by the 'mu' board
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
    print(df)

    df = df.merge(gender_data, on='artist', how='left')

    # Handle missing gender values
    df['gender'] = df['gender'].fillna('unknown')  # Assign 'unknown' for missing genders
    color_map = {'male': 'blue', 'female': 'pink', 'unknown': 'gray'}  # Add color for unknown
    bar_colors = df['gender'].map(color_map)

    bar_colors = bar_colors.fillna('gray')

    # GRAPH of the top 20 artists based on frequency
    top20_df = df.head(20)
    plt.figure(figsize=(14, 8))
    plt.barh(top20_df['artist'], top20_df['frequency'], color=bar_colors)
    plt.xlabel('Frequency')
    plt.ylabel('Artists')
    plt.title('Top 20 Artists by Frequency in 4Chan Music Board')

    # Add a legend
    blue_patch = mpatches.Patch(color='blue', label='Male')
    pink_patch = mpatches.Patch(color='pink', label='Female')
    plt.legend(handles=[blue_patch, pink_patch])

    plt.savefig('Top20_Chan_Artist_Frequencies.png')
    plt.show()

    # GRAPH of top 10 male and female artists
    top_males_df = df[df['gender'] == 'male'].head(10)
    top_females_df = df[df['gender'] == 'female'].head(10)

    # Concatenate the two DataFrames
    top_gender_df = pd.concat([top_males_df, top_females_df])

    plt.figure(figsize=(14, 8))
    plt.barh(top_gender_df['artist'], top_gender_df['frequency'], color=top_gender_df['gender'].map(color_map))
    plt.xlabel('Frequency')
    plt.ylabel('Artists')
    plt.title('Top 10 Male and Top 10 Female Artists by Frequency 4Chan Music Board')

    # Add a legend
    plt.legend(handles=[blue_patch, pink_patch])

    plt.savefig('Top10_Chan_MF_Artist_Frequencies.png')
    plt.show()
    
    cur.close()
    conn.close()