import apache_beam as beam
from typing import Dict

class VitalsAnomalyDetector:
    VITAL_RANGES = {
        'heart_rate': (60, 100),
        'blood_pressure_systolic': (90, 140),
        'blood_pressure_diastolic': (60, 90),
        'temperature': (36.1, 37.8),
        'oxygen_saturation': (95, 100),
        'respiratory_rate': (12, 20)
    }

    @staticmethod
    def detect_anomalies(element: Dict) -> Dict:
        is_critical = False
        anomalies = []

        for vital, (min_val, max_val) in VitalsAnomalyDetector.VITAL_RANGES.items():
            if element[vital] < min_val or element[vital] > max_val:
                is_critical = True
                anomalies.append(f"{vital}: {element[vital]}")

        element['is_critical'] = is_critical
        element['anomalies'] = '; '.join(anomalies) if anomalies else None
        return element

class DetectAnomaliesDoFn(beam.DoFn):
    def process(self, element: Dict) -> Dict:
        return [VitalsAnomalyDetector.detect_anomalies(element)]

class AnomalyDetectionTransform(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | "Detect Anomalies" >> beam.ParDo(DetectAnomaliesDoFn()) 