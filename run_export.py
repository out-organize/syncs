"""
Script to read data from BigQuery and write it as CSV to Google Cloud Storage
using Application Default Credentials (runs as the service account).
"""

import argparse
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import io
import logging
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bigquery_gcs_export.log')
    ]
)
logger = logging.getLogger(__name__)

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
    logger.info("Starting BigQuery to GCS export script")
    
    try:
        # Parse command line arguments
        args = parse_arguments()
        logger.info("Command line arguments parsed successfully")
        
        SOURCE_PROJECT_ID = args.source_project_id
        DESTINATION_PROJECT_ID = args.destination_project_id
        BUCKET_NAME = args.bucket_name
        DATASET_NAME = args.dataset_name
        OUTPUT_FILE_TYPE = args.output_file_type
        QUERY_FILTER = args.query_filter
        
        logger.info(f"Configuration - Source Project: {SOURCE_PROJECT_ID}, "
                   f"Destination Project: {DESTINATION_PROJECT_ID}, "
                   f"Bucket: {BUCKET_NAME}, Dataset: {DATASET_NAME}, "
                   f"Output Type: {OUTPUT_FILE_TYPE}")
        
        # Generate timestamped filename
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        OUTPUT_FILENAME = f"{DATASET_NAME}/{OUTPUT_FILE_TYPE}/{OUTPUT_FILE_TYPE}_{current_datetime}.csv"
        logger.info(f"Generated output filename: {OUTPUT_FILENAME}")
        
        # Build query
        if QUERY_FILTER != "":
            QUERY = f"""
            SELECT *
            FROM `{SOURCE_PROJECT_ID}.{DATASET_NAME}.{OUTPUT_FILE_TYPE}`
            WHERE {QUERY_FILTER}
            """
            logger.info(f"Using query with filter: {QUERY_FILTER}")
        else:
            QUERY = f"""
            SELECT *
            FROM `{SOURCE_PROJECT_ID}.{DATASET_NAME}.{OUTPUT_FILE_TYPE}`
            """
            logger.info("Using query without filter")
        
        # Initialize BigQuery client using Application Default Credentials
        logger.info("Initializing BigQuery client for source project")
        bq_client = bigquery.Client(project=SOURCE_PROJECT_ID)
        logger.info("BigQuery client initialized successfully")
        
        logger.info("Executing BigQuery query...")
        logger.debug(f"Query: {QUERY}")
        
        # Execute query and load results into pandas DataFrame
        df = bq_client.query(QUERY).to_dataframe()
        logger.info(f"Query executed successfully - returned {len(df)} rows")
        
        if len(df) == 0:
            logger.warning("Query returned 0 rows - proceeding with empty dataset")
        
        # Convert DataFrame to CSV string
        logger.info("Converting DataFrame to CSV format")
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        logger.info(f"CSV conversion completed - data size: {len(csv_data)} characters")
        
        # Initialize Cloud Storage client using Application Default Credentials
        logger.info("Initializing Cloud Storage client for destination project")
        storage_client = storage.Client(project=DESTINATION_PROJECT_ID)
        logger.info("Cloud Storage client initialized successfully")
        
        # Get bucket and create blob
        logger.info(f"Accessing bucket: {BUCKET_NAME}")
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(OUTPUT_FILENAME)
        
        logger.info(f"Uploading CSV to gs://{BUCKET_NAME}/{OUTPUT_FILENAME}...")
        
        # Upload CSV data to GCS
        blob.upload_from_string(csv_data, content_type='text/csv')
        logger.info("CSV upload to GCS completed successfully!")
        logger.info(f"File available at: gs://{BUCKET_NAME}/{OUTPUT_FILENAME}")

        # Prepare for final load query
        logger.info("Preparing final load query to destination BigQuery table")
        job_config = bigquery.QueryJobConfig()
        job_config.location = "us-central1"
        logger.debug(f"Job config location set to: {job_config.location}")

        bq_destination_client = bigquery.Client(project=DESTINATION_PROJECT_ID)
        logger.info("BigQuery destination client initialized successfully")

        load_query = f"""
            LOAD DATA OVERWRITE `{DESTINATION_PROJECT_ID}.{DATASET_NAME}.{OUTPUT_FILE_TYPE}`
            FROM FILES (
                format = 'CSV',
                uris = ['gs://{BUCKET_NAME}/{OUTPUT_FILENAME}']
            );
        """
        
        logger.info("Executing final load query to BigQuery destination table")
        logger.debug(f"Load query: {load_query}")

        # Execute the final query and handle potential failure
        try:
            res = bq_destination_client.query(load_query, job_config=job_config)
            
            # Wait for the job to complete and check for errors
            res.result()  # This will raise an exception if the job failed
            
            logger.info("Final load query executed successfully!")
            logger.info(f"Data successfully loaded into table: {DESTINATION_PROJECT_ID}.{DATASET_NAME}.{OUTPUT_FILE_TYPE}")
            
        except Exception as load_error:
            logger.error(f"CRITICAL: Final load query failed: {str(load_error)}")
            logger.error("Script execution failed due to final query failure")
            sys.exit(1)  # Exit with error code 1
        
        logger.info("BigQuery to GCS export script completed successfully!")

    except Exception as e:
        logger.error(f"Script failed with error: {str(e)}")
        logger.exception("Full exception traceback:")
        sys.exit(1)  # Exit with error code 1

if __name__ == "__main__":
    main()