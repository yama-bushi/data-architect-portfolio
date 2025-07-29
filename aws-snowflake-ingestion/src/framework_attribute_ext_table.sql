create or replace external table ENTERPRISEDATAHUB.FRAMEWORK.ATTRIBUTE
(
  schema_name varchar as (value:c1::varchar)
  ,entity_name varchar as (value:c2::varchar)
  ,attribute_name varchar as (value:c3::varchar)
  ,data_type varchar as (value:c4::varchar)
  ,business_key varchar as (value:c5::varchar)
  ,nullable varchar as (value:c6::varchar)
  ,source_database varchar as (value:c7::varchar)
  ,source_schema varchar as (value:c8::varchar)
  ,source_source_attribute varchar as (value:c9::varchar)
  ,source_data_type varchar as (value:c10::varchar)
)
WITH LOCATION = @INGESTION_FRAMEWORK/attribute/
file_format = (format_name = 'FORMAT_CSV')

REFRESH_ON_CREATE = TRUE
AUTO_REFRESH=TRUE;