from apache_beam.options.pipeline_options import PipelineOptions

class VitalsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_topic', required=True,
                          help='Input Pub/Sub topic')
        parser.add_argument('--bigquery_table', required=True,
                          help='BigQuery table: PROJECT:DATASET.TABLE') 