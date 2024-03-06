-- Verify TPC-H database
SELECT 'region' AS table_name, COUNT(*) AS records FROM region
UNION ALL
SELECT 'nation' AS table_name, COUNT(*) AS records FROM nation
UNION ALL
SELECT 'customer' AS table_name, COUNT(*) AS records FROM customer
UNION ALL
SELECT 'lineitem' AS table_name, COUNT(*) AS records FROM lineitem
UNION ALL
SELECT 'orders' AS table_name, COUNT(*) AS records FROM orders
UNION ALL
SELECT 'part' AS table_name, COUNT(*) AS records FROM part
UNION ALL
SELECT 'partsupp' AS table_name, COUNT(*) AS records FROM partsupp
UNION ALL
SELECT 'supplier' AS table_name, COUNT(*) AS records FROM supplier;