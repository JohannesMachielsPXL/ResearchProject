import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
import sys, re, json
import pyarrow as pa
import pyarrow.parquet as pq


# Function for extracting the first object in a folder and getting the object key if parquet file
def get_first_parquet_file(bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
    for obj in response.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            return obj["Key"]
    return None


# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Get the S3 output bucket and database name based on job parameters
args = getResolvedOptions(sys.argv, ["bucket_output", "databasename"])
bucket_output = args["bucket_output"]
bucket_input = bucket_output.replace("-parquet", "")
databasename = args["databasename"]

# Use Boto3 to list device IDs in the S3 output bucket (and filter to only include valid devices)
s3_client = boto3.client("s3")
result = s3_client.list_objects_v2(Bucket=bucket_output, Prefix="", Delimiter="/")
all_prefixes = [o.get("Prefix") for o in result.get("CommonPrefixes")]
deviceids_and_aggregations = [s for s in all_prefixes if (re.match(r"^[0-9A-F]{8}/$", s) or re.match(r"aggregations/$", s))]


# For each device ID, list sub folders (i.e., tables) and sample 1 parquet file from each sub folder
tables = set()
print("Identifying sub folders and 1st parquet file for devices and aggregations: ", deviceids_and_aggregations)
for deviceid in deviceids_and_aggregations:
    print(f"\n------------\n- deviceid: {deviceid}")
    result_tablename = s3_client.list_objects_v2(Bucket=bucket_output, Prefix=deviceid, Delimiter="/")
    subfolders = [o.get("Prefix") for o in result_tablename.get("CommonPrefixes")]

    # For each device ID, store a Parquet file with all unique message names (to enable fast message drop down queries)
    if re.match(r"^[0-9A-F]{8}/$", deviceid):
        deviceid_clean = deviceid.rstrip('/')
        message_names = [subfolder.replace(deviceid, "").rstrip("/").split("/")[-1] for subfolder in subfolders]
        unique_messages = list(set(message_names))
        messages_table = pa.Table.from_pydict({"MessageName": unique_messages})

        messages_key = f"{deviceid_clean}/messages/2024/01/01/messages.parquet"
        messages_output_path = f"s3://{bucket_output}/{messages_key}"

        pq.write_table(messages_table, "/tmp/messages.parquet", compression='snappy')
        s3_client.upload_file("/tmp/messages.parquet", bucket_output, messages_key)
        print(f"Messages Parquet file successfully written to {messages_output_path}")

        # Add the path for the new messages table to the list of tables
        table_name = f"tbl_{deviceid_clean}_messages"
        tables.add((table_name, messages_key))

    # For all device IDs and aggregations/ map all tables excl. messages table
    for subfolder in subfolders:
        if subfolder != "messages":
            print(f"- subfolder: {subfolder}")
            sample_file_key = get_first_parquet_file(bucket_output, f"{subfolder}")
            if sample_file_key:
                print(f"- sample_file_key: {sample_file_key}")
                tablename_clean = subfolder.replace(deviceid, "").rstrip("/")
                table_name = f"tbl_{deviceid.rstrip('/')}_{tablename_clean}"
                tables.add((table_name, sample_file_key))
            else:
                print(f"No parquet files found under prefix: {subfolder}")

# Create devicemeta table with all device IDs and prefix with device.json log_meta (if available)
metadata_list = []
print("Create devicemeta table with meta names and device IDs (exclude aggregations")
deviceids = [s for s in deviceids_and_aggregations if re.match(r"^[0-9A-F]{8}/$", s)]
for deviceid in deviceids:
    device_json_key = f"{deviceid.rstrip('/')}/device.json"
    deviceid_clean = deviceid.rstrip('/').lower()
    metaname = deviceid_clean.upper()
    
    try:
        response = s3_client.get_object(Bucket=bucket_input, Key=device_json_key)
        device_meta = response["Body"].read().decode('utf-8')
        device_meta_json = json.loads(device_meta)
        log_meta = device_meta_json.get("log_meta", "")
        if log_meta != "":
            metaname = f"{log_meta} ({metaname})"
    except Exception as e:
        print(f"Unable to extract meta data from device.json of {deviceid_clean}: ",e)
    
    metadata_list.append({"MetaName": metaname, "DeviceId": deviceid_clean})


# Create a PyArrow Table from the metadata_list
if metadata_list:
    meta_table = pa.Table.from_pydict({"MetaName": [item['MetaName'] for item in metadata_list],
                                       "DeviceId": [item['DeviceId'] for item in metadata_list]})

    # Define S3 output path (use hardcoded yyyy/mm/dd to allow default method for mapping table)
    meta_key = "aggregations/devicemeta/2024/01/01/devicemeta.parquet"
    output_path = f"s3://{bucket_output}/{meta_key}"

    # Save the PyArrow table to a local Parquet file and upload to S3
    pq.write_table(meta_table, "/tmp/devicemeta.parquet", compression='snappy')
    s3_client.upload_file("/tmp/devicemeta.parquet", bucket_output, meta_key)
    print(f"Device meta Parquet file successfully written to {output_path}")
    
    # Add the table to your Glue tables
    table_name = f"tbl_aggregations_devicemeta"
    tables.add((table_name, meta_key))
else:
    print("No metadata available to write to Parquet.")
    

# Check if the database exists and delete if it does
glue_client = boto3.client("glue")

try:
    glue_client.get_database(Name=databasename)
    glue_client.delete_database(Name=databasename)
except glue_client.exceptions.EntityNotFoundException:
    pass

# Now, create the database
glue_client.create_database(DatabaseInput={"Name": databasename})

print(f"Populating AWS Glue Database {databasename} with the {len(tables)} tables:")
# Create tables in the Data Catalog
for table_name, sample_file_key in tables:
    print(f"- bucket: {bucket_output} | table_name: {table_name}")

    # Sample the parquet file to infer the schema
    sample_file = f"s3://{bucket_output}/{sample_file_key}"
    df = spark.read.parquet(sample_file) 

    # Define the partition keys and other table properties
    partition_keys = [{"Name": "date_created", "Type": "string"}]
    sample_file_key_parts = sample_file_key.split("/")
    s3_table_path = "/".join(sample_file_key_parts[:2])
    date_path = "${date_created}"
    full_s3_path = f"s3://{bucket_output}/{s3_table_path}/{date_path}/"

    projection_props = {
        "typeOfData": "file",
        "classification": "parquet",
        "partition_filtering.enabled": "true",
        "projection.enabled": "true",
        "projection.date_created.type": "date",
        "projection.date_created.format": "yyyy/MM/dd",
        "projection.date_created.range": "2019/01/01,NOW",
        "projection.date_created.interval": "1",
        "projection.date_created.interval.unit": "DAYS",
        "storage.location.template": full_s3_path,
    }

    # Create table in the Glue Catalog directly using the Glue client
    glue_client.create_table(
        DatabaseName=databasename,
        TableInput={
            "Name": table_name,
            "StorageDescriptor": {
                "Columns": [dict(Name=col.name, Type=col.dataType.simpleString()) for col in df.schema],
                "Location": f"s3://{bucket_output}/{s3_table_path}/",
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {"serialization.format": "1"},
                },
            },
            "PartitionKeys": partition_keys,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": projection_props,
        },
    )
