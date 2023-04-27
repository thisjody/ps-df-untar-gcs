import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import PTransform, DoFn
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.io.gcp.pubsub import PubsubMessage
import logging
import sys
import json  # Import json to create a message in JSON format


class ProcessFile(DoFn):
    def start_bundle(self):
        from apache_beam.io.gcp.gcsio import GcsIO
        import tarfile
        from io import BytesIO
        from apache_beam.io.gcp.pubsub import PubsubMessage  # Add this import
        self.gcs = GcsIO()
        self.tarfile = tarfile
        self.BytesIO = BytesIO
        logging.info(f"GcsIO instance: {self.gcs}")
        self.PubsubMessage = PubsubMessage  # Instantiate PubsubMessage

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
            destination_bucket_name = 'untar'
            with self.gcs.open(source_file_path, 'rb') as f:
                with self.tarfile.open(fileobj=f, mode='r|gz') as tar:
                    for member in tar:
                        if not member.isfile():
                            continue
                        
                        # Read the file from the tarball
                        file_data = tar.extractfile(member).read()
                        
                        # Write the file to the destination bucket
                        destination_file_path = f"gs://{destination_bucket_name}/{member.name}"
                        with self.gcs.open(destination_file_path, 'wb') as dest_file:
                            dest_file.write(file_data)

            logging.info(f"Contents of {file_name} uploaded to bucket {destination_bucket_name}")

            # Create a message for the second Dataflow pipeline
            message = {
                'file_name': file_name,
                'source_bucket': source_bucket_name,
                'destination_bucket': destination_bucket_name,
                'file_size': file_size
            }

            # Return the message as a PubsubMessage instance
            return [json.dumps(message).encode('utf-8')]  # Return the message data as bytes


        
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
