import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import PTransform, DoFn
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.io.gcp.pubsub import PubsubMessage
import logging
import sys
import json


class ProcessFile(DoFn):
    def start_bundle(self):
        from apache_beam.io.gcp.bigquery import BigQueryDisposition
        from apache_beam.io.gcp.gcsio import GcsIO
        import tarfile
        from io import BytesIO
        from apache_beam.io.gcp.pubsub import PubsubMessage
        from google.cloud import bigquery
        from google.api_core.exceptions import NotFound
        import re
        import os
        self.BigQueryDisposition = BigQueryDisposition
        self.gcs = GcsIO()
        self.tarfile = tarfile
        self.BytesIO = BytesIO
        self.bigquery = bigquery
        self.NotFound = NotFound
        self.re = re
        self.os = os
        self.PubsubMessage = PubsubMessage

    def process(self, element, *args, **kwargs):
        try:
            file_name = element.attributes['name']
            source_bucket_name = element.attributes['bucket']
            logging.info(f"Processing file: {file_name} in bucket {source_bucket_name}")

            # Get the file size
            source_file_path = f"gs://{source_bucket_name}/{file_name}"
            file_size = self.gcs.size(source_file_path)
            logging.info(f"File size: {file_size} bytes")

            # Read the tar.gz file and upload its contents to the destination bucket
            destination_bucket_name = 'eielson-untar'
            #logging.info(f"destination bucket: {destination_bucket_name}")
            directory_structure = []
            with self.gcs.open(source_file_path, 'rb') as f:
                with self.tarfile.open(fileobj=f, mode='r|gz') as tar:
                    top_level_dir = None
                    for member in tar:
                        if not member.isfile():
                            continue

                        # Extract the top-level directory
                        if top_level_dir is None:
                            logging.info(f"Top level directory arrives as: {member.name}")
                            #Ignore hidden files or directories
                            if not member.name.startswith('.'):
                                top_level_dir = member.name.split('/')[0].replace('-', '_')
                                pattern = r"^\d{4}_\d{2}_\d{2}$"  # Pattern to match YYYY_MM_DD
                                if not self.re.match(pattern, top_level_dir):  # Check if top_level_dir matches the pattern
                                    logging.error(f"Invalid top-level directory: {top_level_dir}. Skipping file: {file_name}")
                                    return  # Skip processing this tar.gz file if the top-level directory is invalid
                            logging.info(f"Top level directory set: {top_level_dir}")  # Log the top-level directory when it is set
                        
                        # Read the file from the tarball
                        file_data = tar.extractfile(member).read()

                        # Add the file path and size to the directory structure
                        directory_structure.append({'path': member.name, 'size': member.size})
                        
                        # Write the file to the destination bucket
                        destination_file_path = f"gs://{destination_bucket_name}/{member.name}"
                        with self.gcs.open(destination_file_path, 'wb') as dest_file:
                            dest_file.write(file_data)

            logging.info(f"Contents of {file_name} uploaded to bucket {destination_bucket_name}")
            logging.info(f"Top level directory: {top_level_dir}")  # Log the top-level directory after processing the tar.gz file

            # Connect to BigQuery
            client = self.bigquery.Client()

            # Use 'top_level_dir' as the dataset ID
            dataset_id = top_level_dir
            dataset_ref = client.dataset(dataset_id)

            try:
                dataset = client.get_dataset(dataset_ref)
                logging.info(f"Dataset '{dataset_id}' already exists.")
            except self.NotFound:  # Use NotFound instead of self.NotFound
                logging.info(f"Dataset '{dataset_id}' not found. Creating...")
                dataset = client.create_dataset(self.bigquery.Dataset(dataset_ref))
                logging.info(f"Dataset '{dataset_id}' created.")

            table_id = "vtnd"  # change this to something more dynamic soon
            table_ref = dataset_ref.table(table_id)
            try:
                table = client.get_table(table_ref)  # Make an API request.
                logging.info(f"Table {table_id} already exists in the dataset {dataset_id}")
            except self.NotFound:
                table = client.create_table(table_ref)  # Make an API request.
                logging.info(f"Created table {table_id} in the dataset {dataset_id}")

            # Create a message for the second Dataflow pipeline
            #remove for now 'directory_structure': directory_structure
            message = {
                'file_name': file_name,
                'dataset_id': dataset_id,
                'source_bucket': source_bucket_name,
                'destination_bucket': destination_bucket_name,
                'file_size': file_size,
            }

            # Return the message as a PubsubMessage instance
            return [json.dumps(message).encode('utf-8')]
        
        except KeyError as e:
            logging.error(f"Required attribute is missing in the message: {e}")


class ReadAndProcessFiles(PTransform):
    def __init__(self, topic, output_topic):
        self.topic = topic
        self.output_topic = output_topic

    def expand(self, pcoll):
        return (
            pcoll
            | "Read from Pub/Sub" >> ReadFromPubSub(topic=self.topic, with_attributes=True).with_output_types(PubsubMessage)
            | "Process files" >> beam.ParDo(ProcessFile())
            | "Write to Pub/Sub" >> WriteToPubSub(topic=self.output_topic)  # Added this line to write messages to OUTPUT_TOPIC
        )


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True, help="Google Cloud project ID")
    parser.add_argument("--topic", required=True, help="Pub/Sub topic")
    parser.add_argument("--output_topic", required=True, help="Pub/Sub output topic")  # Add an argument for the output topic
    parser.add_argument("--runner", default="DirectRunner", help="Pipeline runner")
    args, _ = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(runner=args.runner, project=args.project)
    with beam.Pipeline(options=pipeline_options) as pipeline:
        _ = pipeline | "Listen to Pub/Sub topic" >> ReadAndProcessFiles(topic=args.topic, output_topic=args.output_topic)  # Pass output_topic to ReadAndProcessFiles


if __name__ == "__main__":
    main(sys.argv)