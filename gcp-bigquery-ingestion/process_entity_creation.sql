-- BigQuery: Dynamic Entity Table & Procedure Generator
-- Author: Justin Lowe
-- Purpose: Given database, schema, entity name, and drop/replace flag,
--   dynamically builds staging/base tables and an entity-level processing procedure.

CREATE OR REPLACE PROCEDURE framework.process_entity_creation(
    _db STRING,
    _schema STRING,
    _entity STRING,
    _drop BOOLEAN
)
BEGIN
  -- Declare variables
  DECLARE db STRING;
  DECLARE schema STRING;
  DECLARE schema_stage STRING;
  DECLARE entity STRING;
  DECLARE table_strat STRING;
  DECLARE table STRING;
  DECLARE create_stage_sql STRING;
  DECLARE create_base_sql STRING;
  DECLARE select_stage_sql STRING;
  DECLARE update_clause_sql STRING;
  DECLARE insert_p1_sql STRING;
  DECLARE insert_p2_sql STRING;
  DECLARE biz_keys_sql STRING;
  DECLARE process_sproc STRING;

  -- Assign values
  SET db = _db;
  SET schema = _schema;
  SET schema_stage = CONCAT(_schema,'_stage');
  SET entity = _entity;
  SET table = CONCAT(_db, '.', _schema, '.', _entity);

  -- Choose table creation strategy
  SET table_strat = CASE WHEN _drop = TRUE THEN 'create or replace table ' ELSE 'alter table ' END;

  -- Script to create staging table
  SET create_stage_sql = (
    SELECT CONCAT(
      'create or replace table ', db, '.', schema_stage, '.', entity,
      ' (framework_md5 STRING,',
      STRING_AGG(CONCAT(LOWER(ATTRIBUTE_NAME), ' ', DATA_TYPE, CASE WHEN NULLABLE = 'FALSE' THEN ' NOT NULL' ELSE ' ' END), ',' ORDER BY CAST(attribute_ordinal AS INT) ASC),
      ')'
    )
