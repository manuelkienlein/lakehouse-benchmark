# Local spark docker cluster for transforming csv to parquet files

```
# Start spark cluster
docker-compose up -d

# Stop spark cluster
docker-compose down
```

## Execution of a spark batch job

There is a spark job in the file `csv-to-parquet-job.py`

Copy it in the docker container
```
docker cp -L csv-to-parquet-job.py spark-transformer-spark-master-1:/opt/bitnami/spark/csv-to-parquet-job.py
```

Get ip of running spark master
```
docker logs spark-transformer-spark-master-1
```

Execute pyspark file
```
docker exec spark-transformer-spark-master-1 spark-submit --master spark://spark-transformer-spark-master-1:7077 csv-to-parquet-job.py
```