# AWS Snowflake Ingestion Framework

This project provides a reusable framework for **automating the creation and management of Snowflake tables** using external tables, metadata-driven schema definitions, and dynamic table/procedure creation via Snowflake Scripting (JavaScript).

## What’s Included
- `Create_Tables.sql`: Defines an external table from CSV-based schema metadata.
- `framework_attribute_ext_table.sql`: Alternate schema for the attribute external table.
- `PROCESS_ENTITY_CREATION.sql`: JavaScript stored procedure to automate creation and updating of entity tables, including stage/base tables and a dynamic merge process.

## How It Works
1. Load your schema metadata (CSV or database-driven) into the external table.
2. Run the stored procedure to auto-generate tables and merge logic for new entities.
3. Supports automation and rapid schema evolution for scalable ingestion.

## Usage
- Replace all external stage, file format, and schema/database names with those from your environment.
- No sensitive data or credentials are present—add your own config via `.env` or environment variables as needed.

## Best Practices
- Use version control for all SQL and metadata.
- Parameterize your environment and secrets—do not hard-code any keys or passwords.

---

**Author:** Justin Lowe  
