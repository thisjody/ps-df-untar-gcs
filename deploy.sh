#!/bin/bash

# Ensure the script stops on first error
set -e

# Replace these values with your own settings
export PROJECT_ID='airgcp'
export REGION='us-west1'
export STAGING_LOCATION='gs://untar/staging'
export TEMP_LOCATION='gs://untar/temp'

# Pub/Sub topic
export TOPIC='projects/airgcp/topics/ps-df-untar'
export OUTPUT_TOPIC='projects/airgcp/topics/ps-df-bigquery-ingest'

# Install required packages
pip install -r ./requirements.txt
pip install -e .

# Run the Apache Beam pipeline with DataflowRunner
python src/main.py \
  --runner DataflowRunner \
  --project $PROJECT_ID \
  --topic $TOPIC \
  --output_topic $OUTPUT_TOPIC \
  --region $REGION \
  --temp_location $TEMP_LOCATION \
  --staging_location $STAGING_LOCATION \
  --job_name ps-df-untar-gcs-notify6 \
  --setup_file ./setup.py \
  --streaming
