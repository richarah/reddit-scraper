import praw
import pandas as pd
import logging
import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for detailed logs
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]  # This ensures logs go to stdout
)

# Reddit API credentials from .env
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")

# Initialize PRAW Reddit instance
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

# Database URL from .env
DATABASE_URL = os.getenv("DATABASE_URL")

# Subreddits to scrape from .env (comma-separated)
SUBREDDITS = os.getenv("SUBREDDITS").split(',')

# Rate limit from .env (default to 5 seconds if not set)
RATE_LIMIT = int(os.getenv("RATE_LIMIT", 5))

# Function to scrape all comments from a subreddit
def scrape_all_comments(subreddit_name):
    try:
        subreddit = reddit.subreddit(subreddit_name)
        
        # List to store all posts and comments
        post_comment_data = []

        # Log the start of the scraping process
        logging.info(f"Started scraping posts and comments from subreddit: {subreddit_name}")

        # Loop through the posts
        for post in subreddit.new(limit=None):  # "new" for all posts in reverse chronological order
            logging.info(f"Scraping post: {post.title}, permalink: {post.permalink}")
            
            # Post metadata
            post_data = {
                "post_id": post.id,
                "post_title": post.title,
                "post_body": post.selftext,
                "post_author": post.author.name if post.author else None,
                "post_score": post.score,
                "post_created_utc": post.created_utc,
                "post_permalink": post.permalink,
                "post_num_comments": post.num_comments,
                "post_url": post.url,
                "post_subreddit": post.subreddit.display_name
            }

            # Scraping comments related to the post
            post.comments.replace_more(limit=0)  # Ensure all comments are loaded
            for comment in post.comments.list():  # Iterate through all comments on the post
                comment_data = {
                    "comment_id": comment.id,
                    "comment_body": comment.body,
                    "comment_author": comment.author.name if comment.author else None,
                    "comment_score": comment.score,
                    "comment_created_utc": comment.created_utc,
                    "comment_permalink": f"https://www.reddit.com{comment.permalink}",
                    "post_id": post.id,  # Linking the comment to the post
                    "parent_comment_id": comment.parent().id if comment.parent() else None  # Parent comment ID
                }
                post_comment_data.append({**post_data, **comment_data})

            # Sleep to rate limit requests (configurable from RATE_LIMIT)
            logging.info(f"Sleeping for {RATE_LIMIT} seconds.")
            time.sleep(RATE_LIMIT)

        # Convert to DataFrame
        post_comment_df = pd.DataFrame(post_comment_data)

        # Save to the database
        save_to_database(post_comment_df)

        logging.info(f"Successfully scraped and saved {len(post_comment_data)} posts and comments from subreddit: {subreddit_name}")
        return post_comment_df

    except Exception as e:
        logging.error(f"Error occurred while scraping posts and comments from subreddit {subreddit_name}: {e}")
        return pd.DataFrame()

# Function to save data to the database
def save_to_database(df):
    try:
        # Create database engine
        engine = create_engine(DATABASE_URL)

        # Save DataFrame to PostgreSQL
        df.to_sql('reddit_posts_comments', con=engine, if_exists='append', index=False)
        logging.info("Data saved to the database successfully.")
    except SQLAlchemyError as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"Error saving to the database: {e}")

# Main execution
if __name__ == "__main__":
    # Scrape all posts and comments from the selected subreddits
    for subreddit_name in SUBREDDITS:
        post_comment_df = scrape_all_comments(subreddit_name.strip())

        # Display the results (first 5 rows for inspection)
        if not post_comment_df.empty:
            print(f"Results from {subreddit_name}:\n", post_comment_df.head())
        else:
            print(f"No data scraped from {subreddit_name}.")
