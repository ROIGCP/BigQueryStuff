def gcs_to_bq(event, context):
  from google.cloud import bigquery
  gcsevent = event
  client = bigquery.Client()
  dataset = "bqload"
  dataset_ref = client.dataset(dataset)
  table = "names"
  

  gcsbucket = event['bucket']
  gcsfile = event['name']
  uri = 'gs://{}/{}'.format(gcsbucket,gcsfile)
  
  job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
  )
  load_job = client.load_table_from_uri(
    uri,
    dataset_ref.table(table),
    job_config=job_config
  )
  print("Load Job Submitted")
  
def pubsub_to_bq(event, context):
  from google.cloud import bigquery
  import base64
  pubsub_message = base64.b64decode(event['data']).decode('utf-8')
  table_id="bqload.messages"
  print("Row To Insert: " + pubsub_message)
    
  client = bigquery.Client()
  table = client.get_table(table_id)
  row_to_insert = [(pubsub_message,)]     # NOTE - the trailing comma is required for this case - it expects a tuple
  errors = client.insert_rows(table, row_to_insert)
  if errors == []:
    print("Row Inserted: " + pubsub_message)
