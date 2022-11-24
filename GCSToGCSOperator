copy_single_file = GCSToGCSOperator(
    task_id="copy_single_gcs_file",
    source_bucket=BUCKET_NAME_SRC,
    source_object=OBJECT_1,
    destination_bucket=BUCKET_NAME_DST,  # If not supplied the source_bucket value will be used
    destination_object="backup_" + OBJECT_1,  # If not supplied the source_object value will be used
)
