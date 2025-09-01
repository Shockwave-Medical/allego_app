CREATE OR REPLACE PROCEDURE DEVELOPMENT.STREAMLIT_DATA_EDITOR_GENERATOR.SP_ADD_CREATION_DATE("TABLE_NAME" VARCHAR, "SCHEMA_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS '
def run(session, table_name, schema_name):

    # Check if CREATION_DATE column exists
    check_sql = f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ''{table_name}''
          AND COLUMN_NAME = ''CREATION_DATE''
    """
    column_count = session.sql(check_sql).collect()[0][0]
    if column_count == 0:
        alter_sql = f''ALTER TABLE "{table_name}" ADD COLUMN CREATION_DATE TIMESTAMP_LTZ''
        session.sql(alter_sql).collect()
        return ''CREATION_DATE column added.''
    else:
        return ''CREATION_DATE column already exists.''
';