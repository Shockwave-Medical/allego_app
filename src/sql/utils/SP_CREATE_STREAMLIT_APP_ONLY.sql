-- Procedure to create Streamlit app only
CREATE OR REPLACE PROCEDURE CREATE_STREAMLIT_APP_ONLY(
    SCHEMA_NAME STRING,
    APP_NAME STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
def run(session, SCHEMA_NAME, APP_NAME):
    sql = f"""
        CREATE OR REPLACE STREAMLIT {APP_NAME}
        ROOT_LOCATION = 'development.streamlit_data_editor_generator.streamlit_data_editor_generator/branches/main/src'
        TITLE = '{APP_NAME}'
        MAIN_FILE = 'main.py'
        QUERY_WAREHOUSE = IT_DEVELOPER_WH;
        GRANT OWNERSHIP ON STREAMLIT {APP_NAME} TO ROLE HR_DEVELOPER;
        GRANT USAGE ON STREAMLIT {APP_NAME} TO ROLE STREAMLIT_HR;
    """
    session.sql(sql).collect()
    return f'Streamlit app created: {APP_NAME}'
$$;
