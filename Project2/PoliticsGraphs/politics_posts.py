import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import time
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")

if __name__ == "__main__":
    print(DATABASE_URL)
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()

    # Define start and end date range for the query
    start_date = datetime(2024, 11, 1, 0, 0)
    end_date = datetime(2024, 11, 14, 23, 59)
    start_timestamp = int(time.mktime(start_date.timetuple()))
    end_timestamp = int(time.mktime(end_date.timetuple()))

    print(f"Start timestamp: {start_timestamp}, End timestamp: {end_timestamp}")
    
    query = """
    SELECT (floor((data->'data'->>'created_utc')::numeric))::bigint AS created_utc
    FROM posts
    WHERE post_id LIKE 't3%'
    AND (floor((data->'data'->>'created_utc')::numeric))::bigint >= 1730433600
    AND (floor((data->'data'->>'created_utc')::numeric))::bigint < 1731646740;
    """

    # Execute the query
    cur.execute(query)
    data = cur.fetchall()

    # Extract the timestamps
    timestamps = [row[0] for row in data]
    df = pd.DataFrame(timestamps, columns=["created_utc"])

    # Convert the created_utc timestamp to a datetime object
    df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')

    # Extract the date (not the time)
    df['date'] = df['created_utc'].dt.date

    # Group by date and count the number of posts for each date
    posts_per_day = df.groupby(['date']).size()

    # Reset the index to make it easier to plot
    posts_per_day = posts_per_day.reset_index(name='posts')

    # Plotting the data (one line for posts per day)
    plt.figure(figsize=(12, 8))
    plt.plot(posts_per_day['date'], posts_per_day['posts'], label="Posts per Day", marker='o')

    plt.title('Number of r/politics Posts per Day (Nov 1st - Nov 14th)')
    plt.xlabel('Date')
    plt.ylabel('Number of Posts')

    # Set the x-axis format to display date
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=1))

    # Rotate labels for better readability
    plt.xticks(rotation=45, ha='right')

    # Tight layout to avoid clipping labels
    plt.tight_layout()

    # Save the plot to a file
    plt.savefig('politics_posts_per_day.png')

    # Show the plot
    plt.show()

    # Close the database connection
    cur.close()
    conn.close()
