import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery

class BigQuerySchema:
    VITALS_SCHEMA = {
        'fields': [
            {'name': 'patient_id', 'type': 'STRING'},
            {'name': 'timestamp', 'type': 'TIMESTAMP'},
            {'name': 'processing_timestamp', 'type': 'TIMESTAMP'},
            {'name': 'heart_rate', 'type': 'FLOAT'},
            {'name': 'blood_pressure_systolic', 'type': 'FLOAT'},
            {'name': 'blood_pressure_diastolic', 'type': 'FLOAT'},
            {'name': 'temperature', 'type': 'FLOAT'},
            {'name': 'oxygen_saturation', 'type': 'FLOAT'},
            {'name': 'respiratory_rate', 'type': 'FLOAT'},
            {'name': 'is_critical', 'type': 'BOOLEAN'},
            {'name': 'anomalies', 'type': 'STRING'},
        ]
    }

    ALERTS_SCHEMA = {
        'fields': [
            {'name': 'patient_id', 'type': 'STRING'},
            {'name': 'timestamp', 'type': 'TIMESTAMP'},
            {'name': 'processing_timestamp', 'type': 'TIMESTAMP'},
            {'name': 'anomalies', 'type': 'STRING'},
            {'name': 'vital_readings', 'type': 'STRING'},
        ]
    }

class WriteToStorageTransform(beam.PTransform):
    def __init__(self, vitals_table, alerts_table):
        super().__init__()
        self.vitals_table = vitals_table
        self.alerts_table = f"{vitals_table}_alerts"  # Create alerts table name based on vitals table

    def expand(self, pcoll):
        # Split records based on criticality
        critical_records, normal_records = (
            pcoll | 'Split Critical/Normal' >> beam.Partition(
                lambda record, _: 1 if record['is_critical'] else 0, 2))

        # Write critical records to alerts table
        (critical_records 
         | 'Prepare Alert Records' >> beam.Map(self._prepare_alert_record)
         | 'Write Alerts to BigQuery' >> WriteToBigQuery(
             self.alerts_table,
             schema=BigQuerySchema.ALERTS_SCHEMA,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
         ))

        # Write all records to main vitals table
        return (pcoll | 'Write Vitals to BigQuery' >> WriteToBigQuery(
            self.vitals_table,
            schema=BigQuerySchema.VITALS_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        ))

    @staticmethod
    def _prepare_alert_record(record):
        """Prepare record for alerts table with relevant fields."""
        vital_readings = {
            'heart_rate': record.get('heart_rate'),
            'blood_pressure_systolic': record.get('blood_pressure_systolic'),
            'blood_pressure_diastolic': record.get('blood_pressure_diastolic'),
            'temperature': record.get('temperature'),
            'oxygen_saturation': record.get('oxygen_saturation'),
            'respiratory_rate': record.get('respiratory_rate')
        }
        
        return {
            'patient_id': record['patient_id'],
            'timestamp': record['timestamp'],
            'processing_timestamp': record['processing_timestamp'],
            'anomalies': record['anomalies'],
            'vital_readings': str(vital_readings)
        } 