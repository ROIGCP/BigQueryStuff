gcs_to_bq
  Requires
    Dataset:  bqload
    Table:    names
    (schema should match your file)

pubsub_to_bq
  Requires:
    Dataset:  bqload
    Table:    messages
    (schema - start with a single string column)
