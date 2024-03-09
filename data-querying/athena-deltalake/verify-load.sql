SELECT 'region' AS table_name, COUNT(*) AS records FROM "AwsDataCatalog"."tpc_h_sf100_glue"."region"
UNION ALL
SELECT 'nation' AS table_name, COUNT(*) AS records FROM "AwsDataCatalog"."tpc_h_sf100_glue"."nation"
UNION ALL
SELECT 'customer' AS table_name, COUNT(*) AS records FROM "AwsDataCatalog"."tpc_h_sf100_glue"."customer"
UNION ALL
SELECT 'lineitem' AS table_name, COUNT(*) AS records FROM "AwsDataCatalog"."tpc_h_sf100_glue"."lineitem"
UNION ALL
SELECT 'orders' AS table_name, COUNT(*) AS records FROM "AwsDataCatalog"."tpc_h_sf100_glue"."orders"
UNION ALL
SELECT 'part' AS table_name, COUNT(*) AS records FROM "AwsDataCatalog"."tpc_h_sf100_glue"."part"
UNION ALL
SELECT 'partsupp' AS table_name, COUNT(*) AS records FROM "AwsDataCatalog"."tpc_h_sf100_glue"."partsupp"
UNION ALL
SELECT 'supplier' AS table_name, COUNT(*) AS records FROM "AwsDataCatalog"."tpc_h_sf100_glue"."supplier";