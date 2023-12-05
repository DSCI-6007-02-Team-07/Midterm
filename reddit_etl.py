import time
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

def run_reddit_etl():
    api = KaggleApi()
    api.authenticate()

    # Retry up to 3 times
    for _ in range(3):
        try:
            # Download the dataset from Kaggle using the Kaggle API
            api.dataset_download_file('devanshivipul/reddit-hateful-comment-detetction', file_name='reddit.csv')
            break  # If successful, exit the loop
        except Exception as e:
            print(f"Error downloading dataset: {e}")
            time.sleep(5)  # Wait for 5 seconds before retrying

    data = pd.read_csv('reddit.csv')

    # Save the dataset with an absolute path
    #data.to_csv('C:\\Users\\Devanshi_24\\Airflow\\projects\\reddit.csv', index=False)

    # Print the first few rows of the DataFrame
    
    print(data)

    df= pd.DataFrame(data)
    df.to_csv("s3://team-nine-bucket/reddit_data.csv")

# Run the ETL pipeline
#run_reddit_etl()
