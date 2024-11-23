from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client()

# Define the schema for the vitals table
schema = [
    bigquery.SchemaField("patient_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "DATETIME", mode="REQUIRED"),
    bigquery.SchemaField("heart_rate", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("blood_pressure_systolic", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("blood_pressure_diastolic", "FLOAT", mode="NULLABLE"), 
    bigquery.SchemaField("temperature", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("oxygen_saturation", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("respiratory_rate", "FLOAT", mode="NULLABLE")
]

# Configure the table
table_id = "primal-duality-442608-r4.PatientsDataset.patient_vitals"
table = bigquery.Table(table_id, schema=schema)

# Create the table
table = client.create_table(table, exists_ok=True)
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
