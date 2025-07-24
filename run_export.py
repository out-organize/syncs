#!/usr/bin/env python3
"""
Script to read data from BigQuery and write it as CSV to Google Cloud Storage
using Application Default Credentials (runs as the service account).
"""

import os
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import io
import json
from datetime import datetime

def main():
    # Configuration from environment variables
    SERVICE_ACCOUNT_CREDENTIALS = os.getenv("SERVICE_ACCOUNT_CREDENTIALS")
    SOURCE_PROJECT_ID = os.getenv("SOURCE_PROJECT_ID")
    DESTINATION_PROJECT_ID = os.getenv("DESTINATION_PROJECT_ID")
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    DATASET_NAME = os.getenv("DATASET_NAME")
    OUTPUT_FILE_TYPE = os.getenv("OUTPUT_FILE_TYPE", "export")  # Default fallback
    QUERY_FILTER = os.getenv("BIGQUERY_SQL_FILTER","") 
    
    # Generate timestamped filename
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    OUTPUT_FILENAME = f"{DATASET_NAME}/{OUTPUT_FILE_TYPE}/{OUTPUT_FILE_TYPE}_{current_datetime}.csv"
    
    
    if QUERY_FILTER != "":
        QUERY = f"""
        SELECT *
        FROM `{SOURCE_PROJECT_ID}.{DATASET_NAME}.{OUTPUT_FILE_TYPE}`
        WHERE {QUERY_FILTER}
        """
    else:
        QUERY = f"""
        SELECT *
        FROM `{SOURCE_PROJECT_ID}.{DATASET_NAME}.{OUTPUT_FILE_TYPE}`
        """
    
    # Validate required environment variables
    required_vars = {
        "SERVICE_ACCOUNT_CREDENTIALS": SERVICE_ACCOUNT_CREDENTIALS,
        "SOURCE_PROJECT_ID": SOURCE_PROJECT_ID,
        "DESTINATION_PROJECT_ID": DESTINATION_PROJECT_ID,
        "BUCKET_NAME": BUCKET_NAME,
        "DATASET_NAME": DATASET_NAME
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    # try:
    # Parse service account credentials from environment variable
    credentials_info = json.loads(SERVICE_ACCOUNT_CREDENTIALS)
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    
    # Initialize BigQuery client
    bq_client = bigquery.Client(
        credentials=credentials,
        project=SOURCE_PROJECT_ID
    )
    
    print("Executing BigQuery query...")
    print(f"Query: {QUERY}")
    
    # Execute query and load results into pandas DataFrame
    df = bq_client.query(QUERY).to_dataframe()
    
    print(f"Query returned {len(df)} rows")
    
    # Convert DataFrame to CSV string
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    
    # Initialize Cloud Storage client
    storage_client = storage.Client(
        credentials=credentials,
        project=DESTINATION_PROJECT_ID
    )
    
    # Get bucket and create blob
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(OUTPUT_FILENAME)
    
    print(f"Uploading CSV to gs://{BUCKET_NAME}/{OUTPUT_FILENAME}...")
    
    # Upload CSV data to GCS
    blob.upload_from_string(csv_data, content_type='text/csv')
    
    print("Upload completed successfully!")
    print(f"File available at: gs://{BUCKET_NAME}/{OUTPUT_FILENAME}")
        
    # except Exception as e:
    #     print(f"Error occurred: {str(e)}")
    #     raise

if __name__ == "__main__":
    main()