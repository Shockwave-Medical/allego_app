-- Procedure to create a history table for a source table with extra columns
CREATE OR REPLACE PROCEDURE CREATE_HISTORY_TABLE(
    SCHEMA_NAME STRING,
    SOURCE_TABLE_NAME STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'run'
PACKAGES = ('snowflake-snowpark-python')
EXECUTE AS CALLER
AS
$$
def run(session, SCHEMA_NAME, SOURCE_TABLE_NAME):
    columns_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{SCHEMA_NAME}'
          AND TABLE_NAME = '{SOURCE_TABLE_NAME}'
        ORDER BY ORDINAL_POSITION
    """
    columns_result = session.sql(columns_query).collect()
    columns = [f"{row['COLUMN_NAME']} {row['DATA_TYPE']}" for row in columns_result]
    columns += [
        "LAST_UPDATED_BY STRING",
        "ACTION_FLAG STRING",
        "UPDATE_DATA STRING",
        "CREATION_DATE TIMESTAMP",
        "HISTORY_CREATION_DATE TIMESTAMP",
        "LAST_UPDATE_DATE TIMESTAMP"
    ]
    columns_ddl = ",\n    ".join(columns)
    create_table_sql = f"""
        CREATE OR REPLACE TABLE {SCHEMA_NAME}.{SOURCE_TABLE_NAME}_HISTORY (
            {columns_ddl}
        );
    """
    session.sql(create_table_sql).collect()
    return f'History table created: {SOURCE_TABLE_NAME}_HISTORY'
$$;
