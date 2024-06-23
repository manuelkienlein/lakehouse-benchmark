# Apache Iceberg

https://iceberg.apache.org/spark-quickstart/

Enthält ein lokales Spark Cluster mit konfiguriertem Iceberg Catalog.

# Get started
```
docker-compose up
```

## Spark SQL Shell

Starte Spark-SQL Session
```
docker exec -it spark-iceberg spark-sql
```

## Spark Shell (Scala, Python)

Starte Spark-Shell Session
```
docker exec -it spark-iceberg spark-shell
```

Starte PySpark-Shell Session
```
docker exec -it spark-iceberg pyspark
```

## Jupiter Notebook

Starte Notebook Server unter http://localhost:8888
```
docker exec -it spark-iceberg notebook
```

# Create table
> Let’s create a table using **demo.nyc.taxis** where **demo** is the catalog name, **nyc** is the database name, and **taxis** is the table name.

SparkSQL
```SQL
CREATE TABLE demo.nyc.taxis
(
  vendor_id bigint,
  trip_id bigint,
  trip_distance float,
  fare_amount double,
  store_and_fwd_flag string
)
PARTITIONED BY (vendor_id);
```

PySpark
```Python
from pyspark.sql.types import DoubleType, FloatType, LongType, StructType,StructField, StringType
schema = StructType([
  StructField("vendor_id", LongType(), True),
  StructField("trip_id", LongType(), True),
  StructField("trip_distance", FloatType(), True),
  StructField("fare_amount", DoubleType(), True),
  StructField("store_and_fwd_flag", StringType(), True)
])

df = spark.createDataFrame([], schema)
df.writeTo("demo.nyc.taxis").create()
```