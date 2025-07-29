create or replace PROCEDURE ENTERPRISEDATAHUB.FRAMEWORK.PROCESS_ENTITY_CREATION(DB_NAME string,SCHEMA_NAME string,ENTITY_NAME string, DROP_FLAG FLOAT)
returns string
language javascript
as
$$
var db = DB_NAME
var schema = SCHEMA_NAME
var entity = ENTITY_NAME
if (DROP_FLAG = 1.0)
{
var table_strat = "create or replace table "
}
else 
{
var table_strat = "alter table "
}
var table = db + "." + schema + "." + entity
var create_stage_sql = "select 'create or replace table '|| '"+DB_NAME+"' || '.\"' || SCHEMA_NAME || '_STAGE\".' || ENTITY_NAME || ' (FRAMEWORK_MD5 VARCHAR(32),' || listagg(UPPER(ATTRIBUTE_NAME) || ' ' || DATA_TYPE || case when NULLABLE = 'FALSE' then ' NOT NULL' else ' NULL' END  ,  ',') within group (order by CAST(attribute_ordinal AS INT) asc) || ') COPY GRANTS' from ENTERPRISEDATAHUB.FRAMEWORK.ATTRIBUTE WHERE SCHEMA_NAME = '"+ SCHEMA_NAME +"' AND ENTITY_NAME = '"+ENTITY_NAME+"'GROUP BY ENTITY_NAME,SCHEMA_NAME"
var create_base_sql = "select 'create or replace table ' || '"+DB_NAME+"' || '.' || SCHEMA_NAME || '.' || ENTITY_NAME || ' (FRAMEWORK_VALID_FROM DATETIME,FRAMEWORK_VALID_TO DATETIME,FRAMEWORK_DML_ACTION VARCHAR(1),' || listagg(UPPER(ATTRIBUTE_NAME) || ' ' || DATA_TYPE || case when NULLABLE = 'FALSE' then ' NOT NULL' else ' NULL' END  ,  ',') within group (order by CAST(attribute_ordinal AS INT) asc) || ',FRAMEWORK_MD5 VARCHAR(32)) COPY GRANTS' from ENTERPRISEDATAHUB.FRAMEWORK.ATTRIBUTE WHERE SCHEMA_NAME = '"+ SCHEMA_NAME +"' AND ENTITY_NAME = '"+ENTITY_NAME+"'GROUP BY ENTITY_NAME,SCHEMA_NAME"
var select_stage_sql  = "select 'select ' || listagg(UPPER(ATTRIBUTE_NAME),  ',') within group (order by CAST(attribute_ordinal AS INT) asc) || ',MD5(' || listagg('IFNULL(CAST(' || UPPER(ATTRIBUTE_NAME) || ' AS VARCHAR),\''\'\')',  '||') within group (order by CAST(attribute_ordinal AS INT) asc) || ') AS FRAMEWORK_MD5,GETDATE() AS FRAMEWORK_VALID_FROM FROM '|| 'ENTERPRISEDATAHUB'||'.'||SCHEMA_NAME||'_STAGE.'||ENTITY_NAME from "+DB_NAME+".FRAMEWORK.ATTRIBUTE WHERE SCHEMA_NAME = '"+ SCHEMA_NAME +"' AND ENTITY_NAME = '"+ENTITY_NAME+"'GROUP BY ENTITY_NAME,SCHEMA_NAME"
var update_clause_sql = "select listagg('a.'||UPPER(ATTRIBUTE_NAME)||'=b.'||UPPER(ATTRIBUTE_NAME),  ',') within group (order by CAST(attribute_ordinal AS INT) asc) || ',a.FRAMEWORK_MD5=b.FRAMEWORK_MD5,a.FRAMEWORK_VALID_FROM=b.FRAMEWORK_VALID_FROM'from ENTERPRISEDATAHUB.FRAMEWORK.ATTRIBUTE WHERE SCHEMA_NAME = '"+ SCHEMA_NAME +"' AND ENTITY_NAME = '"+ENTITY_NAME+"'GROUP BY ENTITY_NAME,SCHEMA_NAME"
var insert_p1_sql = "select listagg(UPPER(ATTRIBUTE_NAME),  ',') within group (order by CAST(attribute_ordinal AS INT) asc) || ',FRAMEWORK_MD5,FRAMEWORK_VALID_FROM'from ENTERPRISEDATAHUB.FRAMEWORK.ATTRIBUTE WHERE SCHEMA_NAME = '"+ SCHEMA_NAME +"' AND ENTITY_NAME = '"+ENTITY_NAME+"'GROUP BY ENTITY_NAME,SCHEMA_NAME"
var insert_p2_sql = "select listagg('b.'|| UPPER(ATTRIBUTE_NAME),  ',') within group (order by CAST(attribute_ordinal AS INT) asc) || ',b.FRAMEWORK_MD5,b.FRAMEWORK_VALID_FROM'from ENTERPRISEDATAHUB.FRAMEWORK.ATTRIBUTE WHERE SCHEMA_NAME = '"+ SCHEMA_NAME +"' AND ENTITY_NAME = '"+ENTITY_NAME+"'GROUP BY ENTITY_NAME,SCHEMA_NAME"
var biz_keys_sql = "select listagg('a.'||UPPER(ATTRIBUTE_NAME)||'=b.'||UPPER(ATTRIBUTE_NAME),  ' AND ') within group (order by CAST(attribute_ordinal AS INT) asc) from ENTERPRISEDATAHUB.FRAMEWORK.ATTRIBUTE WHERE SCHEMA_NAME = '"+ SCHEMA_NAME +"' AND ENTITY_NAME = '"+ENTITY_NAME+"' AND BUSINESS_KEY = 'TRUE' GROUP BY ENTITY_NAME,SCHEMA_NAME"
var stream_sql = "CREATE STREAM IF NOT EXISTS "+DB_NAME+"."+SCHEMA_NAME+".\""+ENTITY_NAME+".STREAM\" ON TABLE "+DB_NAME+"."+SCHEMA_NAME+"."+ENTITY_NAME

var create_stage_stmt = snowflake.createStatement(
      {
      sqlText: create_stage_sql
      }
);
var create_base_stmt = snowflake.createStatement(
      {
      sqlText: create_base_sql
      }
);

var select_stage_stmt = snowflake.createStatement(
      {
      sqlText: select_stage_sql
      }
);

var update_clause_stmt = snowflake.createStatement(
      {
      sqlText: update_clause_sql
      }
);

var insert_p1_stmt = snowflake.createStatement(
      {
      sqlText: insert_p1_sql
      }
);

var insert_p2_stmt = snowflake.createStatement(
      {
      sqlText: insert_p2_sql
      }
);

var biz_keys_stmt = snowflake.createStatement(
      {
      sqlText: biz_keys_sql
      }
);

var end_result = "";
end_result += table_strat
try {
    var result = create_stage_stmt.execute();
    result.next();
    snowflake.execute(
    {
        sqlText: result.getColumnValue(1)
    }
    )
    end_result += "Stage creation succeeded.";
}
catch (err)  {
          end_result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
          end_result += "\n  Message: " + err.message;
          end_result += "\nStack Trace:\n" + err.stackTraceTxt; 
          }

try {
    var result = create_base_stmt.execute();
    result.next();
    snowflake.execute(
    {
        sqlText: result.getColumnValue(1)
    }
    )
    end_result += "\nBase creation succeeded.";
}
catch (err)  {
          end_result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
          end_result += "\n  Message: " + err.message;
          end_result += "\nStack Trace:\n" + err.stackTraceTxt; 
          }
          
try {
    var select_result = select_stage_stmt.execute();
	select_result.next();
	var select_concat = select_result.getColumnValue(1);
    var update_result = update_clause_stmt.execute();
	update_result.next();
	var update_concat = update_result.getColumnValue(1);
    var insert1_result = insert_p1_stmt.execute();
	insert1_result.next();
	var insert1_concat = insert1_result.getColumnValue(1);
    var insert2_result = insert_p2_stmt.execute();
	insert2_result.next();
	var insert2_concat = insert2_result.getColumnValue(1);
    var biz_keys_result = biz_keys_stmt.execute();
	biz_keys_result.next();
	var biz_keys_concat = biz_keys_result.getColumnValue(1);
    
    var process_procedure_sql = ("create or replace PROCEDURE "+DB_NAME+"."+ SCHEMA_NAME+ "." + ENTITY_NAME +"_PROCESS()\n"
          +" returns string\n"
          +" language javascript\n"
          +" as\n"
          +" \$\$\n"
          +" var end_result = \"\"\n"
          +" var create_process_sql = \"MERGE INTO " + DB_NAME+"."+SCHEMA_NAME+"."+ENTITY_NAME+" a USING (" + select_concat + ")b ON " + biz_keys_concat + " WHEN MATCHED AND a.FRAMEWORK_MD5 <> b.FRAMEWORK_MD5 THEN UPDATE SET "+ update_concat + " WHEN NOT MATCHED THEN INSERT (" + insert1_concat +") VALUES (" + insert2_concat + ")\" \n"
          +" try { \n"
          +" snowflake.execute( \n"
          +" { \n"
          +" sqlText: create_process_sql\n"
          +" }\n"
          +" ) \n"
          +" } \n"
          +" catch (err)  {\n"
          +" }\n"
          +" return create_process_sql;\n"
          +" \$\$;")
	snowflake.execute(
    {
        sqlText: process_procedure_sql
    }
    )
    end_result += "\nPROCESS procedure creation succeeded.";
}
catch (err)  {
          end_result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
          end_result += "\n  Message: " + err.message;
          end_result += "\nStack Trace:\n" + err.stackTraceTxt; 
          }
try {
    snowflake.execute(
    {
        sqlText: stream_sql
    }
    )
    end_result += "\nSTREAM creation succeeded.";
    
}
catch (err)  {
          end_result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
          end_result += "\n  Message: " + err.message;
          end_result += "\nStack Trace:\n" + err.stackTraceTxt; 
          }


return end_result;
$$;