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
tpc_h_scale_factor = 1000
tpc_h_database_name = f"tpc_h_sf{tpc_h_scale_factor}"
tpc_h_tables = ["region", "nation", "customer", "lineitem", "orders", "part", "partsupp", "supplier"]

target_bucket_prefix = f"{target_bucket}/{tpc_h_database_name}/"
database_location = f"s3://{target_bucket_prefix}"

from delta.tables import *

#for table_name in tpc_h_tables:
def import_csv_to_delta_table(table_name):
    print(f"Importing table {table_name}")
    table_location = f"{database_location}{table_name}"
    csv_location = f"s3://{source_bucket}/sf{tpc_h_scale_factor}/{table_name}/"

    # Read the schema of the Delta table
    delta_table_schema = spark.read.format("delta").load(table_location).schema

    # Define the schema for the CSV file based on the Delta table schema
    csv_schema = StructType([StructField(field.name, field.dataType, field.nullable) for field in delta_table_schema])

    # Read the CSV file into a DataFrame with defined schema
    df_orders = spark.read.options(delimiter="|", header=False, inferSchema=True).schema(csv_schema).csv(csv_location)

    df_orders.write.format("delta").mode("overwrite").option("path", table_location).saveAsTable(f"{tpc_h_database_name}.{table_name}")
    return table_name

# Parallelize the import of CSV files to Delta Lake
#sc.parallelize(tpc_h_tables).foreach(lambda table_name: import_csv_to_delta_table(table_name, spark))
sc.parallelize(tpc_h_tables).foreach(import_csv_to_delta_table)
#sc.parallelize(tpc_h_tables).map(import_csv_to_delta_table).collect()

job.commit()