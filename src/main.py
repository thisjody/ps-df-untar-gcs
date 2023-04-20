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

    def process(self, element, *args, **kwargs):
        try:
            file_name = element.attributes['name']
            bucket_name = element.attributes['bucket']
            logging.info(f"Processing file: {file_name} in bucket {bucket_name}")
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
