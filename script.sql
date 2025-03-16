CREATE OR REPLACE TABLE `resolute-bloom-451602-h1.prd_proceso_backup_bq.tables_backup` (
  id_tables_backup INT64 NOT NULL,
  project_name STRING NOT NULL,
  dataset_name STRING NOT NULL,
  table_name STRING NOT NULL,
  status STRING NOT NULL,
  date_created TIMESTAMP NOT NULL,
  user_created STRING NOT NULL,
  date_modified TIMESTAMP,
  user_modified STRING
);

CREATE OR REPLACE TABLE `resolute-bloom-451602-h1.prd_proceso_backup_bq.tables_row_counts` (
  id_tables_row_counts INT64 NOT NULL,
  id_tables_backup INT64 NOT NULL,
  process_number INT64 NOT NULL,
  project_name STRING NOT NULL,
  dataset_name STRING NOT NULL,
  table_name STRING NOT NULL,
  row_count INT64 NOT NULL,
  path_backup STRING,
  status_backup STRING NOT NULL,
  date_created DATETIME NOT NULL,
  user_created STRING NOT NULL
);

INSERT INTO `resolute-bloom-451602-h1.prd_proceso_backup_bq.tables_backup`
(id_tables_backup, project_name, dataset_name, table_name, status, date_created, user_created)
VALUES
(1, 'resolute-bloom-451602-h1', 'ecommerce', 'productos', 'ACTIVE', CURRENT_TIMESTAMP(), 'data');

INSERT INTO `resolute-bloom-451602-h1.prd_proceso_backup_bq.tables_backup`
(id_tables_backup, project_name, dataset_name, table_name, status, date_created, user_created)
VALUES
(2, 'resolute-bloom-451602-h1', 'ecommerce', 'partition_by_day', 'ACTIVE', CURRENT_TIMESTAMP(), 'data');

INSERT INTO `resolute-bloom-451602-h1.prd_proceso_backup_bq.tables_backup`
(id_tables_backup, project_name, dataset_name, table_name, status, date_created, user_created)
VALUES
(3, 'resolute-bloom-451602-h1', 'fruit_store', 'fruit_details', 'ACTIVE', CURRENT_TIMESTAMP(), 'data');

select * from `resolute-bloom-451602-h1.prd_proceso_backup_bq.tables_backup` order by id_tables_backup;
select * from `resolute-bloom-451602-h1.prd_proceso_backup_bq.tables_row_counts` order by id_tables_row_counts desc;


UPDATE `resolute-bloom-451602-h1.prd_proceso_backup_bq.tables_row_counts`
    SET path_backup = 'gs://prd-backup-tables-bq/ecommerce/productos_20250301215625.parquet',
        status_backup = 'SUCCESS'
    WHERE id_tables_row_counts = 4;

