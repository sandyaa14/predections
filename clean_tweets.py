import re
import csv
import os
import emoji

# Function to remove all emojis
def remove_emojis(text):
    return emoji.replace_emoji(text, replace='')  # Removes all emojis

# Function to clean tweets while keeping Tamil & English text
def clean_text(text):
    text = text.lower()  # Convert to lowercase
    text = re.sub(r'http\S+', '', text)  # Remove URLs
    text = re.sub(r'@\w+', '', text)  # Remove mentions (@username)
    text = re.sub(r'#\w+', '', text)  # Remove hashtags
    text = re.sub(r'[^\w\s\u0B80-\u0BFF]', '', text)  # Keep only English & Tamil characters
    text = remove_emojis(text)  # Remove emojis
    text = re.sub(r'\s+', ' ', text).strip()  # Remove extra spaces
    return text

# Function to clean all tweets from a CSV file and remove empty ones
def clean_tweets(input_file, output_file):
    if not os.path.exists(input_file):
        print(f"❌ Error: {input_file} not found!")
        return

    with open(input_file, 'r', encoding='utf-8-sig') as infile, open(output_file, 'w', newline='', encoding='utf-8-sig') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # Read header
        header = next(reader)
        writer.writerow(header)

        removed_count = 0
        kept_count = 0

        # Process each tweet
        for row in reader:
            if len(row) > 2:  # Ensure tweet text column exists
                cleaned_tweet = clean_text(row[2])  # Clean the tweet text
                if cleaned_tweet:  # Keep only non-empty tweets
                    row[2] = cleaned_tweet
                    writer.writerow(row)
                    kept_count += 1
                else:
                    removed_count += 1

    print(f"✅ Cleaned tweets saved to: {output_file}")
    print(f"🗑 Removed {removed_count} empty tweets, kept {kept_count} valid tweets.")

# Example Usage (Modify filenames as needed)
if __name__ == "__main__":
    input_csv = "all_tweets.csv"  # Change to your actual filename
    output_csv = "all_cleaned_tweets.csv"
    clean_tweets(input_csv, output_csv)
