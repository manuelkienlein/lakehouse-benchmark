#!/bin/bash

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd $parent_path/tpc-h/dbgen

# == DBGEN ==
# -s <n>: Scale factor in <s> GB
# -T: schema table name
# -f: Force, overwrite exisiting files
# -b <s>: Load distribution for <s> (default: dists.dss)
# -C <n>: Split data set into <n> chunks
# -S <n>: Build the <n>th step of data set

#./dbgen -s 1
#./dbgen -s 1 -C 5 -S 1

for i in {1..5}
do
   ./dbgen -s 1 -C 5 -S $i -f
done