-- Postgres: Extracts column metadata for mapping to BigQuery attribute table
-- (Helps drive your metadata-driven pipeline)

SELECT
  table_name,
  column_name,
  CASE
    WHEN data_type IN ('uuid', 'character varying', 'text', 'jsonb') THEN 'STRING'
    WHEN data_type IN ('integer', 'bigint') THEN 'INTEGER'
    WHEN data_type = 'timestamp without time zone' THEN 'TIMESTAMP'
    ELSE data_type
  END AS data_type,
  ordinal_position,
  FALSE AS business_key,
  CASE WHEN is_nullable = 'YES' THEN TRUE ELSE FALSE END AS is_nullable,
  table_catalog,
  table_schema,
  column_name,
  data_type
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position;
