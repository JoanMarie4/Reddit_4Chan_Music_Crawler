import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from dotenv import load_dotenv
import os
from datetime import datetime
import time

load_dotenv()


DATABASE_URL = os.environ.get("DATABASE_URL")

if __name__ == "__main__":
    print(DATABASE_URL)
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()

    start_date = datetime(2024, 11, 1, 0, 0)
    end_date = datetime(2024, 11, 14, 23, 59)
    start_timestamp = int(time.mktime(start_date.timetuple()))
    end_timestamp = int(time.mktime(end_date.timetuple()))

    print(f"Start timestamp: {start_timestamp}, End timestamp: {end_timestamp}")
    
    query = """
    SELECT (floor((data->'data'->>'created_utc')::numeric))::bigint AS created_utc
    FROM posts
    WHERE post_id LIKE 't1%'
    AND (floor((data->'data'->>'created_utc')::numeric))::bigint >= 1730433600
    AND (floor((data->'data'->>'created_utc')::numeric))::bigint < 1731646740;
    """

    cur.execute(query)
    data = cur.fetchall()
    timestamps = [row[0] for row in data]
    df = pd.DataFrame(timestamps, columns=["created_utc"])
    print(df)


    # Convert the created_utc timestamp to a datetime object
    df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')

    df['date'] = df['created_utc'].dt.date
    df['hour'] = df['created_utc'].dt.hour

    # Group by date and hour, and count the number of posts for each (date, hour)
    posts_per_hour_per_day = df.groupby(['date', 'hour']).size()

    # Reset the index to make it easier to plot
    posts_per_hour_per_day = posts_per_hour_per_day.reset_index(name='posts')

    # Create a new datetime column combining date and hour for plotting
    posts_per_hour_per_day['datetime'] = pd.to_datetime(posts_per_hour_per_day['date'].astype(str) + ' ' + posts_per_hour_per_day['hour'].astype(str) + ':00')

    # Plotting the data (one line for posts per hour per day)
    plt.figure(figsize=(12, 8))
    plt.plot(posts_per_hour_per_day['datetime'], posts_per_hour_per_day['posts'], label="Posts per Hour", marker='o')

    plt.title('Number of r/politics Comments per Hour (Nov 1st - Nov 14th)')
    plt.xlabel('Date and Hour')
    plt.ylabel('Number of Posts')

    # Set the x-axis format to display both date and hour
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=6))

    # Rotate labels for better readability
    plt.xticks(rotation=45, ha='right')

    # Tight layout to avoid clipping labels
    plt.tight_layout()

    # Save the plot to a file
    plt.savefig('politics_comments_per_hour.png')

    # Show the plot
    plt.show()

    # Close the database connection
    cur.close()
    conn.close()