import pandas as pd
import logging

from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv
from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH

def reddit_pipeline(file_name: str, subreddits:str, time_filter:'day', limit:None):
    # connecting to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'Tony Montana')
    # extraction
    all_posts = []
    for subreddit in subreddits:
        logging.info(f"Extracting from r/{subreddit}")
        try:
            posts = extract_posts(instance, subreddit, time_filter, limit)
            all_posts.extend(posts)
            logging.info(f"Extracted {len(posts)} posts from r/{subreddit}")
        except Exception as e:
            logging.error(f"Failed to extract from r/{subreddit}: {e}")
            continue

    post_df = pd.DataFrame(all_posts)
    # transformation
    post_df = transform_data(post_df)
    # loading to csv
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)

    return file_path
