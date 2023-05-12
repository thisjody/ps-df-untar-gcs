#!/bin/bash

# Ensure the script stops on first error
set -e

# Replace these values with your own settings
export PROJECT_ID='acep-ext-eielson-2021'
export REGION='us-west1'
# Environment variable for destination bucket
export DESTINATION_BUCKET='eielson-untar'

export STAGING_LOCATION='gs://eielson-tar-archive/staging'
export TEMP_LOCATION='gs://eielson-tar-archive/temp'

# Pub/Sub topic
export TOPIC='projects/acep-ext-eielson-2021/topics/ps-df-untar'
export OUTPUT_TOPIC='projects/acep-ext-eielson-2021/topics/ps-df-bigquery-ingest'

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
  --job_name ps-df-untar-gcs6 \
  --setup_file ./setup.py \
  --streaming
