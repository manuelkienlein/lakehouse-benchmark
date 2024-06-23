import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

target_bucket = "lakehouse-deltalake"
source_bucket = "tpc-h-dataset"
tpc_h_scale_factor = 1
tpc_h_database_name = f"tpc_h_sf{tpc_h_scale_factor}"
tpc_h_tables = ["region", "nation", "customer", "lineitem", "orders", "part", "partsupp", "supplier"]

target_bucket_prefix = f"{target_bucket}/{tpc_h_database_name}/"
database_location = f"s3://{target_bucket_prefix}"
import boto3
import time

## Delete existing Files in DeltaLake S3 Bucket
s3 = boto3.resource('s3')
bucket = s3.Bucket(target_bucket)
bucket.objects.filter(Prefix=target_bucket_prefix).delete()

## Drop Tables in Glue Data Catalog
for table_name in tpc_h_tables:
    try:
        glue = boto3.client('glue')
        glue.delete_table(DatabaseName=tpc_h_database_name, Name=table_name)
    except glue.exceptions.EntityNotFoundException:
        print(f"Table {tpc_h_database_name}.{table_name} does not exist")

try:
    glue = boto3.client('glue')
    res = glue.get_database(Name=tpc_h_database_name)
    print(f"Database {tpc_h_database_name} exists.")
    if 'LocationUri' not in res['Database']:
        print(
            f"Warning: Database {tpc_h_database_name} does not have Location. You need to configure location in the database.")
except glue.exceptions.EntityNotFoundException:
    print(f"Database {tpc_h_database_name} does not exist.")
    glue = glue.create_database(
        DatabaseInput={
            'Name': tpc_h_database_name,
            'LocationUri': database_location
        }
    )
    print(f"Created a new database {tpc_h_database_name}.")
start = time.time()
query = f"""
CREATE TABLE {tpc_h_database_name}.region
(
    R_REGIONKEY INT,
    R_NAME STRING,
    R_COMMENT STRING
) USING delta LOCATION '{database_location}region';
"""
spark.sql(query)
query = f"""
CREATE TABLE {tpc_h_database_name}.nation
(
    N_NATIONKEY INT,
    N_NAME STRING,
    N_REGIONKEY INT,
    N_COMMENT STRING
) USING delta LOCATION '{database_location}nation';
"""
spark.sql(query)
query = f"""
CREATE TABLE {tpc_h_database_name}.customer
(
    C_CUSTKEY INT,
    C_NAME STRING,
    C_ADDRESS STRING,
    C_NATIONKEY INT,
    C_PHONE STRING,
    C_ACCTBAL DOUBLE,
    C_MKTSEGMENT STRING,
    C_COMMENT STRING
) USING delta LOCATION '{database_location}customer';
"""
spark.sql(query)
query = f"""
CREATE TABLE {tpc_h_database_name}.lineitem
(
    L_ORDERKEY INT,
    L_PARTKEY INT,
    L_SUPPKEY INT,
    L_LINENUMBER INT,
    L_QUANTITY DOUBLE,
    L_EXTENDEDPRICE DOUBLE,
    L_DISCOUNT DOUBLE,
    L_TAX DOUBLE,
    L_RETURNFLAG STRING,
    L_LINESTATUS STRING,
    L_SHIPDATE DATE,
    L_COMMITDATE DATE,
    L_RECEIPTDATE DATE,
    L_SHIPINSTRUCT STRING,
    L_SHIPMODE STRING,
    L_COMMENT STRING
) USING delta LOCATION '{database_location}lineitem';
"""
spark.sql(query)
query = f"""
CREATE TABLE {tpc_h_database_name}.orders
(
    O_ORDERKEY INT,
    O_CUSTKEY INT,
    O_ORDERSTATUS STRING,
    O_TOTALPRICE DOUBLE,
    O_ORDERDATE DATE,
    O_ORDERPRIORITY STRING,
    O_CLERK STRING,
    O_SHIPPRIORITY INT,
    O_COMMENT STRING
) USING delta LOCATION '{database_location}orders';
"""
spark.sql(query)
query = f"""
CREATE TABLE {tpc_h_database_name}.part
(
    P_PARTKEY INT,
    P_NAME STRING,
    P_MFGR STRING,
    P_BRAND STRING,
    P_TYPE STRING,
    P_SIZE INT,
    P_CONTAINER STRING,
    P_RETAILPRICE DOUBLE,
    P_COMMENT STRING
) USING delta LOCATION '{database_location}part';
"""
spark.sql(query)
query = f"""
CREATE TABLE {tpc_h_database_name}.partsupp
(
    PS_PARTKEY INT,
    PS_SUPPKEY INT,
    PS_AVAILQTY INT,
    PS_SUPPLYCOST DOUBLE,
    PS_COMMENT STRING
) USING delta LOCATION '{database_location}partsupp';
"""
spark.sql(query)
query = f"""
CREATE TABLE {tpc_h_database_name}.supplier
(
    S_SUPPKEY INT,
    S_NAME STRING,
    S_ADDRESS STRING,
    S_NATIONKEY INT,
    S_PHONE STRING,
    S_ACCTBAL DOUBLE,
    S_COMMENT STRING
) USING delta LOCATION '{database_location}supplier';
"""
spark.sql(query)
duration = time.time() - start
print(f"Total to create tables: {duration}")
from delta.tables import *

for table_name in tpc_h_tables:
    print(f"Importing table {table_name}")
    table_location = f"{database_location}{table_name}"
    csv_location = f"s3://{source_bucket}/sf{tpc_h_scale_factor}/{table_name}/"

    start = time.time()

    # Read the schema of the Delta table
    delta_table_schema = spark.read.format("delta").load(table_location).schema

    duration_1 = time.time() - start
    print(f"Time for reading schema of {table_name}: {duration_1}")
    start_2 = time.time()

    # Define the schema for the CSV file based on the Delta table schema
    csv_schema = StructType([StructField(field.name, field.dataType, field.nullable) for field in delta_table_schema])

    # Read the CSV file into a DataFrame with defined schema
    df_orders = spark.read.options(delimiter="|", header=False, inferSchema=True).schema(csv_schema).csv(csv_location)

    duration_2 = time.time() - start_2
    print(f"Time for reading csv {table_name}: {duration_2}")
    start_3 = time.time()

    df_orders.write.format("delta").mode("overwrite").option("path", table_location).saveAsTable(
        f"{tpc_h_database_name}.{table_name}")

    duration_3 = time.time() - start_3
    print(f"Time for writing delta table {table_name}: {duration_3}")
    duration = time.time() - start
    print(f"Total time for {table_name}: {duration}")

job.commit()