import pandas as pd
import requests
import json
import time
from prefect import flow, task, get_run_logger
from prefect.logging import get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect.blocks.system import Secret

# Replace with your actual Telegram token and chat ID from Prefect Secrets
TELEGRAM_TOKEN = Secret.load("telegram-bot-token").get()
CHAT_ID = Secret.load("telegram-chat-id").get()

# Helper function to send a Telegram notification
def send_telegram_message(token, chat_id, message):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": message}
    requests.post(url, data=data)

# Task to load data from a CSV file
@task(retries=3, retry_delay_seconds=10)
def load_data(filepath):
    logger = get_run_logger()
    logger.info("Loading data from CSV file.")
    data = pd.read_csv(filepath)
    return data

# Task to fetch data from a public API for each row
@task(retries=3, retry_delay_seconds=5)
def fetch_data_from_api(row):
    logger = get_run_logger()
    logger.info(f"Fetching data for user ID: {row['userId']}")
    time.sleep(30)  # Rate limit - wait 30 seconds between each request

    # Sample API call to JSONPlaceholder to get posts for a user
    response = requests.get(f"https://jsonplaceholder.typicode.com/posts?userId={row['userId']}")
    if response.status_code != 200:
        logger.error(f"API request failed for user ID {row['userId']}: {response.status_code}")
        raise Exception(f"Failed to fetch data: {response.status_code}")
    
    return response.json()

# Task to process the fetched data using pandas
@task
def process_data(data):
    logger = get_run_logger()
    logger.info("Processing data.")
    df = pd.DataFrame(data)
    # You can add more data processing logic here if needed
    return df

# Task to save the processed data as a JSON file
@task
def save_results(data, output_path):
    logger = get_run_logger()
    logger.info(f"Saving results to {output_path}.")
    data.to_json(output_path, orient='records', lines=True)

# Task to send a completion notification via Telegram
@task
def notify_completion():
    logger = get_run_logger()
    message = "Data processing complete!"
    send_telegram_message(TELEGRAM_TOKEN, CHAT_ID, message)
    logger.info("Notification sent to Telegram.")

# The main flow that orchestrates the data pipeline
@flow(task_runner=SequentialTaskRunner())
def data_pipeline(filepath, output_path):
    data = load_data(filepath)
    processed_data = []

    # Fetch data for each row in sequence (no parallel requests allowed)
    for _, row in data.iterrows():
        api_response = fetch_data_from_api(row)
        processed_data.extend(api_response)  # Flatten the list of results

    processed_df = process_data(processed_data)
    save_results(processed_df, output_path)
    notify_completion()

if __name__ == "__main__":
    data_pipeline("data.csv", "results.json")
