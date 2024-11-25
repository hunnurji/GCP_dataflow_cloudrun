import apache_beam as beam
from datetime import datetime
from typing import Dict, Tuple
import logging

class VitalsValidator:
    REQUIRED_FIELDS = {
        'patient_id': str,
        'timestamp': str,
        'heart_rate': float,
        'blood_pressure_systolic': float,
        'blood_pressure_diastolic': float,
        'temperature': float,
        'oxygen_saturation': float,
        'respiratory_rate': float
    }

    @staticmethod
    def validate_fields(element: Dict) -> Dict:
        for field, field_type in VitalsValidator.REQUIRED_FIELDS.items():
            if field not in element:
                raise ValueError(f"Missing required field: {field}")
            if not isinstance(element[field], field_type):
                element[field] = field_type(element[field])
        return element

class ValidateVitalsDoFn(beam.DoFn):
    def process(self, element: Dict) -> Tuple[Dict, bool]:
        try:
            validated_element = VitalsValidator.validate_fields(element)
            validated_element['processing_timestamp'] = datetime.utcnow().isoformat()
            return [validated_element]
        except Exception as e:
            logging.error(f"Validation error for record {element}: {e}")
            return []

class ValidateTransform(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | "Validate Data" >> beam.ParDo(ValidateVitalsDoFn()) 