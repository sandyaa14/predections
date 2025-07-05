import pandas as pd
import nltk
import time
from nltk.sentiment import SentimentIntensityAnalyzer
from googletrans import Translator

# Ensure VADER Lexicon is downloaded only if needed
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon')

# Initialize VADER and Translator
sia = SentimentIntensityAnalyzer()
translator = Translator()

# Load the cleaned tweets
df = pd.read_csv("all_cleaned_tweets.csv")

# Ensure there's a column named 'Text'
if 'Text' not in df.columns:
    raise ValueError("Column 'Text' not found in CSV. Ensure correct column name.")

# **Translation Caching (Avoid Repeating Translations)**
translation_cache = {}

def translate_to_english(text, max_retries=3):
    """Translate Tamil text to English with caching & error handling."""
    if not isinstance(text, str) or text.strip() == "":  # Skip empty text
        return "No text"

    # **Return cached translation if available**
    if text in translation_cache:
        return translation_cache[text]

    for attempt in range(max_retries):
        try:
            translated = translator.translate(text, src='ta', dest='en')
            if translated and translated.text:  # Ensure valid translation
                translation_cache[text] = translated.text  # **Store in cache**
                return translated.text
            else:
                return "Translation failed"
        except Exception as e:
            print(f"⚠️ Translation error: {e} (Attempt {attempt + 1}/{max_retries})")
            time.sleep(2 * (attempt + 1))  # Exponential backoff

    return text  # Return original text if translation fails

def analyze_sentiment(text):
    """Perform sentiment analysis using VADER."""
    sentiment_score = sia.polarity_scores(text)['compound']
    if sentiment_score >= 0.05:
        return "Positive"
    elif sentiment_score <= -0.05:
        return "Negative"
    else:
        return "Neutral"

# **Batch Processing: Translate only Tamil tweets**
df['Translated_Tweet'] = df['Text'].astype(str).apply(translate_to_english)

# Perform Sentiment Analysis
df['Sentiment'] = df['Translated_Tweet'].apply(analyze_sentiment)

# Save results
df.to_csv("sentiment_analysis.csv", index=False, encoding='utf-8-sig')

print("✅ Sentiment analysis completed! Results saved in 'sentiment_analysis.csv'.")
