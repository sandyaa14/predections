import asyncio
import os
import csv
import httpx
import pandas as pd
from datetime import datetime
from random import randint
from configparser import ConfigParser
from twikit import Client, TooManyRequests

# Minimum number of tweets required
MINIMUM_TWEETS = 100

# List of queries related to Tamil Nadu Elections 2026
QUERIES = [
    'TVK', 'DMK', 'AIADMK', 'BJP Tamil Nadu', 'Tamil Nadu Elections 2026',
    'Makkal Needhi Maiam', 'Naam Tamilar Katchi', 'Congress Tamil Nadu'
]

# Load login credentials
config_file = 'config.ini'

if not os.path.exists(config_file):
    print(f"Error: '{config_file}' not found! Please create it with your login credentials.")
    exit()

config = ConfigParser()
config.read(config_file)

if 'X' not in config:
    print("Error: Section [X] not found in config.ini. Check the file formatting.")
    exit()

try:
    username = config['X']['username']
    email = config['X']['email']
    password = config['X']['password']
except KeyError as e:
    print(f"Error: Missing key {e} in config.ini. Please check the file.")
    exit()

# Authenticate to X.com (Twitter)
# Initialize the Client without passing any unsupported arguments
client = Client(language='en-US')

async def authenticate():
    """Authenticate to X (Twitter) and save cookies if not already saved."""
    try:
        if os.path.exists('cookies.json'):
            client.load_cookies('cookies.json')
            print(f"{datetime.now()} - Loaded cookies successfully!")
        else:
            await client.login(auth_info_1=username, auth_info_2=email, password=password)
            await client.save_cookies('cookies.json')
            print(f"{datetime.now()} - Logged in and saved cookies successfully!")
    except Exception as e:
        print(f"{datetime.now()} - Authentication failed: {e}")
        exit()

# Function to fetch tweets with retry mechanism
async def get_tweets(query, tweets):
    retry_attempts = 3  # Set the number of retry attempts

    for attempt in range(retry_attempts):
        try:
            if tweets is None:
                print(f'{datetime.now()} - Fetching tweets for query: {query}...')
                tweets = await client.search_tweet(query, product='Top', count=30)
            else:
                wait_time = randint(1, 5)
                print(f'{datetime.now()} - Getting next tweets for {query} after {wait_time} seconds...')
                await asyncio.sleep(wait_time)
                tweets = await tweets.next()
            return tweets
        except httpx.ReadTimeout:
            print(f'{datetime.now()} - Timeout occurred for query "{query}", retrying ({attempt + 1}/{retry_attempts})...')
            await asyncio.sleep(5 * (attempt + 1))  # Exponential backoff
        except TooManyRequests as e:
            await handle_rate_limit(e)

    print(f'{datetime.now()} - Failed to fetch tweets for "{query}" after {retry_attempts} attempts.')
    return None  # Return None if all attempts fail

# Handle rate limit errors
async def handle_rate_limit(e):
    rate_limit_reset = datetime.fromtimestamp(e.rate_limit_reset)
    print(f'{datetime.now()} - Rate limit reached. Waiting until {rate_limit_reset}')
    wait_time = (rate_limit_reset - datetime.now()).total_seconds()
    await asyncio.sleep(max(wait_time, 1))  # Ensure at least 1 second wait

# Main function to fetch tweets for multiple queries
async def fetch_tweets():
    await authenticate()  # Authenticate before fetching tweets

    for query in QUERIES:
        csv_file = f'tweets_{query.replace(" ", "_")}.csv'  # Save tweets in separate files per query

        # Create CSV file
        with open(csv_file, 'w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerow(['Tweet_count', 'Username', 'Text', 'Created At', 'Retweets', 'Likes'])

        tweet_count = 0
        tweets = None

        while tweet_count < MINIMUM_TWEETS:
            try:
                tweets = await get_tweets(query, tweets)
            except TooManyRequests as e:
                await handle_rate_limit(e)
                continue

            if not tweets:
                print(f'{datetime.now()} - No more tweets found for "{query}"')
                break

            for tweet in tweets:
                tweet_count += 1
                tweet_data = [
                    tweet_count, tweet.user.name, tweet.text,
                    tweet.created_at, tweet.retweet_count, tweet.favorite_count
                ]

                # Write tweet data
                with open(csv_file, 'a', newline='', encoding='utf-8-sig') as file:
                    writer = csv.writer(file)
                    writer.writerow(tweet_data)

            print(f'{datetime.now()} - Collected {tweet_count} tweets for "{query}"')

        print(f'{datetime.now()} - Done! Total {tweet_count} tweets collected for "{query}".')

# Merge all individual tweet CSVs into one
def merge_csv_files():
    output_file = "all_tweets.csv"

    # Get all CSV files that start with 'tweets_'
    csv_files = [f for f in os.listdir() if f.startswith("tweets_") and f.endswith(".csv")]

    # Initialize an empty DataFrame
    combined_df = pd.DataFrame()

    # Loop through each file and merge
    for file in csv_files:
        # Extract category from filename
        category = file.replace("tweets_", "").replace(".csv", "").replace("_", " ")

        # Read CSV file
        df = pd.read_csv(file)

        # Add a new column for category
        df["Category"] = category

        # Append to the combined DataFrame
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    # Save the merged DataFrame
    combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"✅ Merged all tweets into '{output_file}' successfully!")

# Run the async tweet fetching and merging process
if __name__ == "__main__":
    asyncio.run(fetch_tweets())  # Fetch tweets first
    merge_csv_files()  # Then merge into one file