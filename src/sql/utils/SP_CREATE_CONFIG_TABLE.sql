-- Procedure to create STREAMLIT_APP_CONFIG table
CREATE OR REPLACE PROCEDURE CREATE_CONFIG_TABLE(SCHEMA_NAME STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'run'
PACKAGES = ('snowflake-snowpark-python')
EXECUTE AS CALLER
AS
$$
def run(session, SCHEMA_NAME):
    sql = f"""
        CREATE OR REPLACE TABLE {SCHEMA_NAME}.STREAMLIT_APP_CONFIG (
            APP_NAME STRING,
            TABLE_NAME STRING,
            KEY_COLUMNS STRING,
            FILTERED_FIELDS STRING,
            REQUIRED_COLUMNS STRING,
            ALLOWED_USERS STRING
        );
    """
    session.sql(sql).collect()
    return f"Config table created in schema: {SCHEMA_NAME}"
$$;
