-- Start warehouse for data loading
ALTER WAREHOUSE Load_WH_TPC_H_SF100 RESUME;

-- Select database for table import
use database TPC_H_SF100;

-- Create tables
CREATE TABLE region
(
    R_REGIONKEY NUMBER(38, 0),
    R_NAME VARCHAR(25),
    R_COMMENT VARCHAR(152)
);

CREATE TABLE nation
(
    N_NATIONKEY NUMBER(38, 0),
    N_NAME VARCHAR(25),
    N_REGIONKEY NUMBER(38, 0),
    N_COMMENT VARCHAR(152)
);

CREATE TABLE customer
(
    C_CUSTKEY NUMBER(38, 0),
    C_NAME VARCHAR(25),
    C_ADDRESS VARCHAR(40),
    C_NATIONKEY NUMBER(38, 0),
    C_PHONE VARCHAR(15),
    C_ACCTBAL NUMBER(12, 2),
    C_MKTSEGMENT VARCHAR(10),
    C_COMMENT VARCHAR(117)
);

CREATE TABLE lineitem
(
    L_ORDERKEY NUMBER(38, 0),
    L_PARTKEY NUMBER(38, 0),
    L_SUPPKEY NUMBER(38, 0),
    L_LINENUMBER NUMBER(38, 0),
    L_QUANTITY NUMBER(12, 2),
    L_EXTENDEDPRICE NUMBER(12, 2),
    L_DISCOUNT NUMBER(12, 2),
    L_TAX NUMBER(12, 2),
    L_RETURNFLAG VARCHAR(1),
    L_LINESTATUS VARCHAR(1),
    L_SHIPDATE DATE,
    L_COMMITDATE DATE,
    L_RECEIPTDATE DATE,
    L_SHIPINSTRUCT VARCHAR(25),
    L_SHIPMODE VARCHAR(10),
    L_COMMENT VARCHAR(44)
);

CREATE TABLE orders
(
    O_ORDERKEY NUMBER(38, 0),
    O_CUSTKEY NUMBER(38, 0),
    O_ORDERSTATUS VARCHAR(1),
    O_TOTALPRICE NUMBER(12, 2),
    O_ORDERDATE DATE,
    O_ORDERPRIORITY VARCHAR(15),
    O_CLERK VARCHAR(15),
    O_SHIPPRIORITY NUMBER(38, 0),
    O_COMMENT VARCHAR(79)
);

CREATE TABLE part
(
    P_PARTKEY NUMBER(38, 0),
    P_NAME VARCHAR(55),
    P_MFGR VARCHAR(25),
    P_BRAND VARCHAR(10),
    P_TYPE VARCHAR(25),
    P_SIZE NUMBER(38, 0),
    P_CONTAINER VARCHAR(10),
    P_RETAILPRICE NUMBER(12, 2),
    P_COMMENT VARCHAR(23)
);

CREATE TABLE partsupp
(
    PS_PARTKEY NUMBER(38, 0),
    PS_SUPPKEY NUMBER(38, 0),
    PS_AVAILQTY NUMBER(38, 0),
    PS_SUPPLYCOST NUMBER(12, 2),
    PS_COMMENT VARCHAR(199)
);

CREATE TABLE supplier
(
    S_SUPPKEY NUMBER(38, 0),
    S_NAME VARCHAR(25),
    S_ADDRESS VARCHAR(40),
    S_NATIONKEY NUMBER(38, 0),
    S_PHONE VARCHAR(15),
    S_ACCTBAL NUMBER(12, 2),
    S_COMMENT VARCHAR(101)
);

-- Load TPC-H files from S3 bucket (CSV format, gzip compressed)
COPY INTO region
  FROM s3://tpc-h-dataset/sf100/region/
  CREDENTIALS = (AWS_KEY_ID='<aws-iam-key>' AWS_SECRET_KEY='<aws-iam-key-secret>')
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0 ERROR_ON_COLUMN_COUNT_MISMATCH = false);

COPY INTO nation
  FROM s3://tpc-h-dataset/sf100/nation/
  CREDENTIALS = (AWS_KEY_ID='<aws-iam-key>' AWS_SECRET_KEY='<aws-iam-key-secret>')
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0 ERROR_ON_COLUMN_COUNT_MISMATCH = false);


COPY INTO customer
  FROM s3://tpc-h-dataset/sf100/customer/
  CREDENTIALS = (AWS_KEY_ID='<aws-iam-key>' AWS_SECRET_KEY='<aws-iam-key-secret>')
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0 ERROR_ON_COLUMN_COUNT_MISMATCH = false);


COPY INTO lineitem
  FROM s3://tpc-h-dataset/sf100/lineitem/
  CREDENTIALS = (AWS_KEY_ID='<aws-iam-key>' AWS_SECRET_KEY='<aws-iam-key-secret>')
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0 ERROR_ON_COLUMN_COUNT_MISMATCH = false);


COPY INTO orders
  FROM s3://tpc-h-dataset/sf100/orders/
  CREDENTIALS = (AWS_KEY_ID='<aws-iam-key>' AWS_SECRET_KEY='<aws-iam-key-secret>')
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0 ERROR_ON_COLUMN_COUNT_MISMATCH = false);


COPY INTO part
  FROM s3://tpc-h-dataset/sf100/part/
  CREDENTIALS = (AWS_KEY_ID='<aws-iam-key>' AWS_SECRET_KEY='<aws-iam-key-secret>')
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0 ERROR_ON_COLUMN_COUNT_MISMATCH = false);


COPY INTO partsupp
  FROM s3://tpc-h-dataset/sf100/partsupp/
  CREDENTIALS = (AWS_KEY_ID='<aws-iam-key>' AWS_SECRET_KEY='<aws-iam-key-secret>')
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0 ERROR_ON_COLUMN_COUNT_MISMATCH = false);


COPY INTO supplier
  FROM s3://tpc-h-dataset/sf100/supplier/
  CREDENTIALS = (AWS_KEY_ID='<aws-iam-key>' AWS_SECRET_KEY='<aws-iam-key-secret>')
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0 ERROR_ON_COLUMN_COUNT_MISMATCH = false);

-- Shutdown warehouse cluster
ALTER WAREHOUSE Load_WH_TPC_H_SF100 SUSPEND;