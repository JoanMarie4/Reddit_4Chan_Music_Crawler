import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from dotenv import load_dotenv
import numpy as np
import os
import sys
sys.path.append("../datasets")

load_dotenv()

CHAN_DATABASE = os.environ.get("4CHAN_DATABASE")

if __name__ == "__main__":
    conn = psycopg2.connect(dsn=CHAN_DATABASE)
    cur = conn.cursor()

    # Load artist gender data
    kpop_csv = pd.read_csv('../datasets/kpop_artists.csv')
    pop_csv = pd.read_csv('../datasets/popular_artists.csv')

    kpop_csv = kpop_csv.rename(columns={'Gender.x': 'gender', 'Stage.Name': 'artist'})
    kpop_csv['gender'] = kpop_csv['gender'].replace({'Boy': 'male', 'Girl': 'female'})
    kpop_csv = kpop_csv[['artist', 'gender']]
    pop_csv = pop_csv[['artist', 'gender']]

    gender_data = pd.concat([kpop_csv, pop_csv], ignore_index=True)

    # SQL Query
    q = """
    WITH toxicity_data AS (
        SELECT 
            UNNEST(posts.artists) AS artist,  -- Unnest artists from posts
            CASE 
                WHEN posts.toxicity->>'class' = 'flag' THEN 1 * (posts.toxicity->>'confidence')::FLOAT
                WHEN posts.toxicity->>'class' = 'normal' THEN -1 * (posts.toxicity->>'confidence')::FLOAT
                ELSE NULL
            END AS score,
            posts.thread_number
        FROM posts

        UNION ALL

        SELECT 
            UNNEST(parent.artists) AS artist,  -- Unnest artists from parent posts
            CASE 
                WHEN parent.toxicity->>'class' = 'flag' THEN 1 * (parent.toxicity->>'confidence')::FLOAT
                WHEN parent.toxicity->>'class' = 'normal' THEN -1 * (parent.toxicity->>'confidence')::FLOAT
                ELSE NULL
            END AS score,
            parent.thread_number
        FROM posts AS child
        JOIN posts AS parent 
        ON child.thread_number = parent.thread_number  -- Include artists from posts with the same thread number
    )
    SELECT 
        artist,
        COUNT(*) AS total_mentions,  -- Total mentions for the artist
        AVG(score) AS average_score  -- Average toxicity score for the artist
    FROM toxicity_data
    WHERE score IS NOT NULL  -- Exclude rows where score could not be calculated
    GROUP BY artist
    HAVING COUNT(*) >= 50  -- Filter out artists with fewer than 50 mentions
    ORDER BY average_score DESC;
    """

    # Execute query
    cur.execute(q)
    result = cur.fetchall()

    # Convert results to DataFrame
    df = pd.DataFrame(result, columns=['artist', 'num_posts', 'toxicity_score'])
    df = df.merge(gender_data, on='artist', how='left')

    print(df.head(20))  # Preview data

    # Separate data by gender
    male_data = df[df['gender'] == 'male']['toxicity_score']
    female_data = df[df['gender'] == 'female']['toxicity_score']

    # Calculate CDF for males and females
    male_data_sorted = np.sort(male_data)
    female_data_sorted = np.sort(female_data)

    male_cdf = np.arange(1, len(male_data_sorted) + 1) / len(male_data_sorted)
    female_cdf = np.arange(1, len(female_data_sorted) + 1) / len(female_data_sorted)

    # Plot CDF
    plt.figure(figsize=(8, 6))
    plt.plot(male_data_sorted, male_cdf, label='Male Artists', color='blue')
    plt.plot(female_data_sorted, female_cdf, label='Female Artists', color='red')

    plt.title('CDF of Toxicity Scores for Male vs Female Artists on 4Chan')
    plt.xlabel('Toxicity Score')
    plt.ylabel('Cumulative Distribution')
    plt.legend()

    # Save plot
    plt.grid(True)
    plt.savefig('Chan_Male_Female_Toxicity_CDF.png')

    # Show plot
    plt.show()

    # Close DB connection
    cur.close()
    conn.close()
