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
    def __init__(self, project_id):  # Add this line
        self.project_id = project_id  # And this line

    def start_bundle(self):
        from apache_beam.io.gcp.gcsio import GcsIO
        import tarfile
        from io import BytesIO
        from google.api_core.exceptions import NotFound
        import re
        import os
        self.gcs = GcsIO()
        self.tarfile = tarfile
        self.BytesIO = BytesIO
        self.NotFound = NotFound
        self.re = re
        self.os = os

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
                                top_level_dir_original = member.name.split('/')[0]
                                top_level_dir = member.name.split('/')[0].replace('-', '_')
                                pattern = r"^\d{4}_\d{2}_\d{2}$"  # Pattern to match YYYY_MM_DD
                                if not self.re.match(pattern, top_level_dir):  # Check if top_level_dir matches the pattern
                                    logging.error(f"Invalid top-level directory: {top_level_dir}. Skipping file: {file_name}")
                                    return  # Skip processing this tar.gz file if the top-level directory is invalid
                            logging.info(f"Top level directory set: {top_level_dir}")  # Log the top-level directory when it is set
                        
                        # Read the file from the tarball
                        file_data = tar.extractfile(member).read()
                        
                        # Write the file to the destination bucket
                        destination_file_path = f"gs://{destination_bucket_name}/{member.name}"
                        with self.gcs.open(destination_file_path, 'wb') as dest_file:
                            dest_file.write(file_data)

            logging.info(f"Contents of {file_name} uploaded to bucket {destination_bucket_name}")
            logging.info(f"Top level directory: {top_level_dir}")  # Log the top-level directory after processing the tar.gz file

            # Use 'top_level_dir' as the dataset ID
            original_date = top_level_dir_original
            dataset_id = top_level_dir

            # Use the directory as the table name
            table_id = "vtnd"

            message = {
                'project_id': self.project_id,
                'dataset_id': dataset_id,
                'table_id' : table_id,
                'original_date' : original_date,
                'source_bucket': source_bucket_name,
                'destination_bucket': destination_bucket_name,
                'file_size': file_size,
            }

            # Return the message as a PubsubMessage instance
            return [json.dumps(message).encode('utf-8')]
        
        except KeyError as e:
            logging.error(f"Required attribute is missing in the message: {e}")


class ReadAndProcessFiles(PTransform):
    def __init__(self, topic, output_topic, project_id):
        self.topic = topic
        self.output_topic = output_topic
        self.project_id = project_id

    def expand(self, pcoll):
        return (
            pcoll
            | "Read from Pub/Sub" >> ReadFromPubSub(topic=self.topic, with_attributes=True).with_output_types(PubsubMessage)
            | "Process files" >> beam.ParDo(ProcessFile(self.project_id))
            | "Write to Pub/Sub" >> WriteToPubSub(topic=self.output_topic)
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
        _ = pipeline | "Listen to Pub/Sub topic" >> ReadAndProcessFiles(project_id=args.project, topic=args.topic, output_topic=args.output_topic)  # Pass output_topic to ReadAndProcessFiles


if __name__ == "__main__":
    main(sys.argv)