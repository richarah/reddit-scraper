import praw
import pandas as pd
import logging
import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

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

# Scrape posts and comments options from .env
SCRAPE_POSTS = os.getenv("SCRAPE_POSTS", "True") == "True"
SCRAPE_COMMENTS = os.getenv("SCRAPE_COMMENTS", "True") == "True"

from sqlalchemy import text

def get_earliest_post_timestamp(subreddit_name):
    try:
        # Create a session to query the database
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Use raw SQL to get the earliest post timestamp
        query = text("""
            SELECT MIN(post_created_utc) 
            FROM reddit_posts_comments 
            WHERE post_subreddit = :subreddit_name
        """)
        
        result = session.execute(query, {"subreddit_name": subreddit_name}).scalar()

        session.close()

        if result:
            return result
        else:
            return None

    except SQLAlchemyError as e:
        logging.error(f"Error occurred while fetching the earliest post timestamp: {e}")
        return None


# Function to scrape posts from a subreddit
def scrape_posts(subreddit_name):
    try:
        subreddit = reddit.subreddit(subreddit_name)
        
        # List to store all posts
        post_data = []

        # Log the start of the scraping process
        logging.info(f"Started scraping posts from subreddit: {subreddit_name}")

        # Loop through the posts
        for post in subreddit.new(limit=None):  # "new" for all posts in reverse chronological order
            logging.info(f"Scraping post: {post.title}, permalink: {post.permalink}")
            
            # Post metadata
            post_data.append({
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
            })

            # Sleep to rate limit requests (configurable from RATE_LIMIT)
            logging.info(f"Sleeping for {RATE_LIMIT} seconds.")
            time.sleep(RATE_LIMIT)

        # Convert to DataFrame
        post_df = pd.DataFrame(post_data)

        # Save to the database
        save_to_database(post_df)

        logging.info(f"Successfully scraped and saved {len(post_data)} posts from subreddit: {subreddit_name}")
        return post_df

    except Exception as e:
        logging.error(f"Error occurred while scraping posts from subreddit {subreddit_name}: {e}")
        return pd.DataFrame()


# Function to scrape comments from a subreddit
def scrape_comments(subreddit_name):
    try:
        subreddit = reddit.subreddit(subreddit_name)

        # Find the earliest post timestamp to resume scraping from
        earliest_post_timestamp = get_earliest_post_timestamp(subreddit_name)
        
        # List to store all comments
        comment_data = []

        # Log the start of the scraping process
        logging.info(f"Started scraping comments from subreddit: {subreddit_name}")

        # Loop through the posts
        for post in subreddit.new(limit=None):  # "new" for all posts in reverse chronological order
            if earliest_post_timestamp and post.created_utc <= earliest_post_timestamp:
                logging.info(f"Skipping post {post.id} as it is older than the earliest post timestamp.")
                continue

            logging.info(f"Scraping post: {post.title}, permalink: {post.permalink}")
            
            # Scraping comments related to the post
            post.comments.replace_more(limit=0)  # Ensure all comments are loaded
            for comment in post.comments.list():  # Iterate through all comments on the post
                comment_data.append({
                    "comment_id": comment.id,
                    "comment_body": comment.body,
                    "comment_author": comment.author.name if comment.author else None,
                    "comment_score": comment.score,
                    "comment_created_utc": comment.created_utc,
                    "comment_permalink": f"https://www.reddit.com{comment.permalink}",
                    "post_id": post.id,  # Linking the comment to the post
                    "parent_comment_id": comment.parent().id if comment.parent() else None  # Parent comment ID
                })

            # Sleep to rate limit requests (configurable from RATE_LIMIT)
            logging.info(f"Sleeping for {RATE_LIMIT} seconds.")
            time.sleep(RATE_LIMIT)

            # Append data to the database immediately after processing each post and its comments
            save_to_database(pd.DataFrame(comment_data))

        logging.info(f"Successfully scraped and saved {len(comment_data)} comments from subreddit: {subreddit_name}")
        return pd.DataFrame(comment_data)

    except Exception as e:
        logging.error(f"Error occurred while scraping comments from subreddit {subreddit_name}: {e}")
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
    # Scrape posts or comments based on the .env configuration
    for subreddit_name in SUBREDDITS:
        if SCRAPE_POSTS:
            post_df = scrape_posts(subreddit_name.strip())

        if SCRAPE_COMMENTS:
            comment_df = scrape_comments(subreddit_name.strip())

        # Display the results (first 5 rows for inspection)
        if 'post_df' in locals() and not post_df.empty:
            print(f"Results from {subreddit_name} (Posts):\n", post_df.head())

        if 'comment_df' in locals() and not comment_df.empty:
            print(f"Results from {subreddit_name} (Comments):\n", comment_df.head())

        # Clean up local variables to avoid carrying over to next subreddit
        del post_df, comment_df
