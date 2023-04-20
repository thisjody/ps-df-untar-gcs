import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import PTransform, DoFn
from apache_beam.io import ReadFromPubSub
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.gcsio import GcsIO
import logging
import sys




class ProcessFile(DoFn):
    def start_bundle(self):
        from apache_beam.io.gcp.gcsio import GcsIO
        self.gcs = GcsIO()
        logging.info(f"GcsIO instance: {self.gcs}")

class ProcessFile(DoFn):
    def start_bundle(self):
        from apache_beam.io.gcp.gcsio import GcsIO
        self.gcs = GcsIO()
        logging.info(f"GcsIO instance: {self.gcs}")

    def process(self, element, *args, **kwargs):
        try:
            file_name = element.attributes['name']
            source_bucket_name = element.attributes['bucket']
            logging.info(f"Processing file: {file_name} in bucket {source_bucket_name}")

            # Get the file size
            source_file_path = f"gs://{source_bucket_name}/{file_name}"
            file_size = self.gcs.size(source_file_path)
            logging.info(f"File size: {file_size} bytes")

            # Copy the file to a new bucket
            destination_bucket_name = 'untar'
            destination_file_path = f"gs://{destination_bucket_name}/{file_name}"
            self.gcs.copy(source_file_path, destination_file_path)
            logging.info(f"File copied to: {destination_file_path}")

            # ...rest of the code...
        except KeyError as e:
            logging.error(f"Required attribute is missing in the message: {e}")




class ReadAndProcessFiles(PTransform):
    def __init__(self, topic):
        self.topic = topic

    def expand(self, pcoll):
        return (
            pcoll
            | "Read from Pub/Sub" >> ReadFromPubSub(topic=self.topic, with_attributes=True).with_output_types(PubsubMessage)
            | "Process files" >> beam.ParDo(ProcessFile())
        )


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True, help="Google Cloud project ID")
    parser.add_argument("--topic", required=True, help="Pub/Sub topic")
    parser.add_argument("--runner", default="DirectRunner", help="Pipeline runner")
    args, _ = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(runner=args.runner, project=args.project)
    with beam.Pipeline(options=pipeline_options) as pipeline:
        _ = pipeline | "Listen to Pub/Sub topic" >> ReadAndProcessFiles(topic=args.topic)


if __name__ == "__main__":
    main(sys.argv)
