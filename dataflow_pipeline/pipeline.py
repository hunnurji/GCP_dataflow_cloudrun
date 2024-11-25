import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
import logging
from apache_beam.options.pipeline_options import StandardOptions

from transforms.parse_transform import ParseTransform
from transforms.validate_transform import ValidateTransform
from transforms.anomaly_transform import AnomalyDetectionTransform
from transforms.write_transform import WriteToStorageTransform
from options.pipeline_options import VitalsOptions

class VitalsProcessingPipeline:
    def __init__(self, options):
        self.options = options
        # Set streaming and runner options
        self.options.view_as(StandardOptions).streaming = True
        self.options.view_as(StandardOptions).runner = 'DataflowRunner'
        self.pipeline = beam.Pipeline(options=self.options)

    def build_pipeline(self):
        # Read messages from Pub/Sub and process them
        (self.pipeline 
         | 'Read from PubSub' >> ReadFromPubSub(topic=self.options.input_topic)
         | 'Parse' >> ParseTransform()
         | 'Validate' >> ValidateTransform()
         | 'Detect Anomalies' >> AnomalyDetectionTransform()
         | 'Write' >> WriteToStorageTransform(self.options.bigquery_table))

    def run(self):
        self.build_pipeline()
        return self.pipeline.run()

def run():
    pipeline_options = VitalsOptions()
    pipeline = VitalsProcessingPipeline(pipeline_options)
    pipeline.run()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

"""
python -m pipeline \
    --input_topic="projects/YOUR_PROJECT/topics/YOUR_TOPIC" \
    --bigquery_table="YOUR_PROJECT:DATASET.TABLE"
"""