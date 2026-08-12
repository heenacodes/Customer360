from pyspark.sql import DataFrame


def write_to_s3_create_table(df: DataFrame, s3_path: str, output_db: str, table_name: str, format="parquet", mode="overwrite"):
    """Write a DataFrame to S3 in parquet and register the table in the Glue catalog."""
    full_path = f"{s3_path}/{table_name}"
    print(f"Writing {table_name} to {full_path}")
    df.show(5, truncate=False)
    print(f"Records: {df.count()}")
    df.write.mode(mode).format(format).save(full_path)
    df.write.mode(mode).format(format).option("path", full_path).saveAsTable(f"{output_db}.{table_name}")
    print(f"Completed writing {table_name} and registered table {output_db}.{table_name}")
    return full_path
