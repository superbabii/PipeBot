# PipeBot

This project implements a data processing pipeline using Prefect 3.0. The pipeline performs the following tasks:

1. Loads data from a CSV file.
2. Fetches data from a third-party API for each row.
3. Processes the data using pandas.
4. Saves the results as JSON files.
5. Sends a notification via Telegram on completion.

## Prerequisites

- Python 3.8+
- Docker
- Prefect 3.0

## Setup

1. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2. Set up Prefect by running:
    ```bash
    prefect orion start
    prefect worker start --pool default-agent-pool
    ```

3. Build the Docker image:
    ```bash
    docker build -t data-pipeline .
    ```

4. Run the pipeline:
    ```bash
    docker run -v $(pwd):/app data-pipeline
    ```

## Configuration

- Edit the `data_pipeline.py` to set your API endpoints and parameters.
- Set up Prefect secrets for Telegram notifications.

## Monitoring

Use the Prefect Orion UI to monitor the pipeline at [http://localhost:4200](http://localhost:4200).

