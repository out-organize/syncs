"""
Script to read data from BigQuery and write it as CSV to Google Cloud Storage
using Application Default Credentials (runs as the service account).
"""

import argparse
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import io
from datetime import datetime

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Export BigQuery data to Google Cloud Storage as CSV')
    
    parser.add_argument('--source-project-id', required=True,
                        help='Source project ID where BigQuery data resides')
    parser.add_argument('--destination-project-id', required=True,
                        help='Destination project ID for GCS bucket')
    parser.add_argument('--bucket-name', required=True,
                        help='Name of the GCS bucket to upload to')
    parser.add_argument('--dataset-name', required=True,
                        help='BigQuery dataset name')
    parser.add_argument('--output-file-type', default='export',
                        help='Output file type (used for table name and file naming) (default: export)')
    parser.add_argument('--query-filter', default='',
                        help='Optional WHERE clause filter for the BigQuery query')
    
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    SOURCE_PROJECT_ID = args.source_project_id
    DESTINATION_PROJECT_ID = args.destination_project_id
    BUCKET_NAME = args.bucket_name
    DATASET_NAME = args.dataset_name
    OUTPUT_FILE_TYPE = args.output_file_type
    QUERY_FILTER = args.query_filter
    
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
    
    try:
        # Initialize BigQuery client using Application Default Credentials
        # This will use the service account the job is running as
        bq_client = bigquery.Client(project=SOURCE_PROJECT_ID)
        
        print("Executing BigQuery query...")
        print(f"Query: {QUERY}")
        
        # Execute query and load results into pandas DataFrame
        df = bq_client.query(QUERY).to_dataframe()
        
        print(f"Query returned {len(df)} rows")
        
        # Convert DataFrame to CSV string
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        # Initialize Cloud Storage client using Application Default Credentials
        storage_client = storage.Client(project=DESTINATION_PROJECT_ID)
        
        # Get bucket and create blob
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(OUTPUT_FILENAME)
        
        print(f"Uploading CSV to gs://{BUCKET_NAME}/{OUTPUT_FILENAME}...")
        
        # Upload CSV data to GCS
        blob.upload_from_string(csv_data, content_type='text/csv')
        
        print("Upload completed successfully!")
        print(f"File available at: gs://{BUCKET_NAME}/{OUTPUT_FILENAME}")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()