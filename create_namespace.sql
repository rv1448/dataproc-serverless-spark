CREATE DATABASE IF NOT EXISTS iceberg_catalog;
SHOW TABLES IN iceberg_catalog;
 

use DATABASE iceberg_catalog;
-- DROP TABLE IF EXISTS test_iceberg1.test_iceberg1;


CREATE TABLE IF NOT EXISTS iceberg_catalog.demo1 (id bigint, demo_name string)
           USING iceberg
           TBLPROPERTIES(bq_table='biglake_managed_tables.demo1');


SHOW TABLES IN iceberg_catalog;
select * from iceberg_catalog.demo1;
