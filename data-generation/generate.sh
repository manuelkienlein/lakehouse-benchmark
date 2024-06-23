#!/bin/bash

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd $parent_path/tpc-h/dbgen

output_dir=$parent_path/tpc-h/dbgen/output
mkdir $output_dir
export DSS_PATH=$output_dir

# == DBGEN ==
# -s <n>: Scale factor in <s> GB
# -T: schema table name
# -f: Force, overwrite existing files
# -b <s>: Load distribution for <s> (default: dists.dss)
# -C <n>: Split data set into <n> chunks
# -S <n>: Build the <n>th step of data set

#./dbgen -s 1
#./dbgen -s 1 -C 5 -S 1

SCALE_FACTOR=1
PARTITIONS=5

DBGEN_PATH=./dbgen

S3_BUCKET=tpc-h-dataset
export AWS_ACCESS_KEY_ID=key
export AWS_SECRET_ACCESS_KEY=secret


for (( i=0; i<=$PARTITIONS; i++ ))
do
  echo $DBGEN_PATH -s $SCALE_FACTOR -C $PARTITIONS -S $i -f -v
  $DBGEN_PATH -s $SCALE_FACTOR -C $PARTITIONS -S $i -f -v

  output_files=$output_dir/*
  for file in $output_files
  do
    # Extract file name and table name
    file_name=$(basename -- "$file")
    table=$(echo $file_name | cut -d'.' -f1)

    # Skip already processed files
    #case $file_name in *.gz.csv) continue;; esac

    # Compress with gzip
    #gzip -v $file
    gzip -c $file > $file.csv.gz
    rm $file
    file=$file.csv.gz
    file_name=$(basename -- "$file")

    # Copy files to S3
    echo aws s3 cp $file s3://$S3_BUCKET/sf$SCALE_FACTOR/$table/$file_name
    #aws s3 cp $file s3://$S3_BUCKET/sf$SCALE_FACTOR/$table/$file_name

    # Delete file after upload
    rm $file
  done
done