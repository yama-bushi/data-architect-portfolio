# GCP BigQuery Ingestion Framework

This project demonstrates a flexible, metadata-driven approach for **automating table and pipeline creation in Google BigQuery**, using dynamic SQL and stored procedures.

## What’s Included

- `process_entity_creation.sql`:  
  A parameterized BigQuery stored procedure that takes a database/schema/entity name and dynamically:
  - Builds a staging table (for new data ingestion)
  - Builds a base table (with valid-from, valid-to, and change tracking columns)
  - Generates the appropriate `MERGE` statement for upserts (including business key and hash checks)
  - Creates a secondary procedure for handling entity-level processing and upserts

- `postgres_to_bigquery_attribute.sql`:  
  A Postgres query that extracts column definitions from a Postgres database, mapping Postgres data types to BigQuery types and generating metadata for use in your framework.

## How It Works

1. **Metadata Table:**  
   - Attribute definitions (columns, types, nullability, business keys, etc.) are managed centrally in a metadata table.
   - Extract column info from your source Postgres DB using the provided SQL and ingest into your BigQuery metadata table.

2. **Dynamic Table/Procedure Creation:**  
   - Call the `process_entity_creation` procedure with your database, schema, and entity names (plus a flag to drop/recreate).
   - The procedure reads your metadata and auto-generates all DDL for stage and base tables, and writes a dynamic merge/upsert process as its own procedure.
   - Handles business key logic, MD5 hashing for change detection, and maintains valid-from/valid-to timestamps for slowly changing dimensions.

3. **Usage:**  
   - All DDL and pipeline logic is auto-generated—minimizing manual SQL work and supporting rapid schema evolution.

## Best Practices

- All table/entity/attribute naming is parameterized.
- Replace `dotcomtherapy.framework.attribute` and project-specific references with your own metadata location.
- Use with dbt or orchestration tools for robust pipeline automation.
- Never hard-code credentials—use service accounts and environment variables as shown in other projects.

## Example Use Case

This pattern is ideal for:
- Automated ingestion from relational sources (like Postgres) into a BigQuery data warehouse
- Scalable onboarding of new data domains or business entities
- Data platform teams seeking a scalable, metadata-driven approach to DDL and ETL

---

**Author:** Justin Lowe  
