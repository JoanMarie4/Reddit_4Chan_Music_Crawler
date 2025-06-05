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

REDDIT_DATABASE = os.environ.get("REDDIT_DATABASE")

if __name__ == "__main__":
    conn = psycopg2.connect(dsn=REDDIT_DATABASE)
    cur = conn.cursor()

    kpop_csv = pd.read_csv('../datasets/kpop_artists.csv')
    pop_csv = pd.read_csv('../datasets/popular_artists.csv')

    kpop_csv = kpop_csv.rename(columns={'Gender.x': 'gender'})
    kpop_csv = kpop_csv.rename(columns={'Stage.Name': 'artist'})
    kpop_csv['gender'] = kpop_csv['gender'].replace({'Boy': 'male', 'Girl': 'female'})
    kpop_csv = kpop_csv[['artist', 'gender']]
    pop_csv = pop_csv[['artist', 'gender']]

    gender_data = pd.concat([kpop_csv, pop_csv], ignore_index=True)

    q = """
    WITH toxicity_data AS (
    SELECT 
        UNNEST(artists) AS artist,  -- Unnest the artists array to work with individual artists
        CASE 
            WHEN toxicity->>'class' = 'flag' THEN 1 * (toxicity->>'confidence')::FLOAT  -- Score for 'flag' is positive confidence
            WHEN toxicity->>'class' = 'normal' THEN -1 * (toxicity->>'confidence')::FLOAT  -- Score for 'normal' is negative confidence
            ELSE NULL  -- Handle cases where the class or confidence field might be missing
        END AS score,
        parent_id
    FROM posts
    UNION ALL
    SELECT 
        UNNEST(p.artists) AS artist,  -- Unnest artists from parent posts
        CASE 
            WHEN p.toxicity->>'class' = 'flag' THEN 1 * (p.toxicity->>'confidence')::FLOAT
            WHEN p.toxicity->>'class' = 'normal' THEN -1 * (p.toxicity->>'confidence')::FLOAT
            ELSE NULL
        END AS score,
        NULL AS parent_id
    FROM posts AS c
    JOIN posts AS p ON c.parent_id = p.post_id  -- Join on parent_id to include parent posts
    )
    SELECT 
        artist,
        COUNT(*) AS total_mentions,  -- Total mentions including both direct and parent posts
        AVG(score) AS average_score  -- Calculate the average score for each artist
    FROM toxicity_data
    WHERE score IS NOT NULL  -- Exclude entries where score could not be calculated
    GROUP BY artist
    HAVING COUNT(*) >= 50  -- Filter out artists with less than 50 posts
    ORDER BY average_score DESC;
    """

    cur.execute(q)
    result = cur.fetchall()
    df = pd.DataFrame(result)
    df.columns = ['artist', 'num posts', 'toxicity_score']
    df = df.merge(gender_data, on='artist', how='left')
    print(df.head(20))

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

    plt.title('CDF of Toxicity Scores for Male vs Female Artists on Reddit')
    plt.xlabel('Toxicity Score')
    plt.ylabel('Cumulative Distribution')
    plt.legend()

    # Show plot
    plt.grid(True)
    plt.savefig('Reddit_Male_Female_Toxicity_CDF.png')
    
    cur.close()
    conn.close()