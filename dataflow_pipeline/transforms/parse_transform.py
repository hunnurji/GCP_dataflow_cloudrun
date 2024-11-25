import apache_beam as beam
import json
import logging

class ParseJsonDoFn(beam.DoFn):
    def process(self, element):
        try:
            return [json.loads(element.decode('utf-8'))]
        except Exception as e:
            logging.error(f"Error parsing JSON: {e}")
            return []

class ParseTransform(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | "Parse JSON" >> beam.ParDo(ParseJsonDoFn()) 