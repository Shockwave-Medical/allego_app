CREATE OR REPLACE PROCEDURE CREATE_STREAMLIT_APP(
	SCHEMA_NAME STRING,
	APP_NAME STRING,
	TABLE_NAME STRING,
	KEY_COLUMNS STRING,
	FILTERED_FIELDS STRING,
	REQUIRED_COLUMNS STRING,
	ALLOWED_USERS STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
def run(
	session,
	SCHEMA_NAME: str,
	TABLE_NAME: str,
	APP_NAME: str,
	KEY_COLUMNS: str,
	REQUIRED_COLUMNS: str,
	FILTERED_FIELDS: str,
	ALLOWED_USERS: str
):
	# Call procedure to create config table
	session.call("DEVELOPMENT.STREAMLIT_DATA_EDITOR_GENERATOR.CREATE_CONFIG_TABLE", SCHEMA_NAME)

	# Call procedure to create history table
	session.call("DEVELOPMENT.STREAMLIT_DATA_EDITOR_GENERATOR.CREATE_HISTORY_TABLE", SCHEMA_NAME, TABLE_NAME)

	# Add CREATION_DATE column if it doesn't exist
	session.call("DEVELOPMENT.STREAMLIT_DATA_EDITOR_GENERATOR.SP_ADD_CREATION_DATE", TABLE_NAME)

	# Store config in a table
	config_sql = f"""
		INSERT INTO {SCHEMA_NAME}.STREAMLIT_APP_CONFIG (
			APP_NAME,
			TABLE_NAME,
			KEY_COLUMNS,
			FILTERED_FIELDS,
			REQUIRED_COLUMNS,
			ALLOWED_USERS
		)
		VALUES (
			'{APP_NAME}',
			'{TABLE_NAME}',
			'{KEY_COLUMNS}',
			'{FILTERED_FIELDS}',
			'{REQUIRED_COLUMNS}',
			'{ALLOWED_USERS}'
		)
	"""
	session.sql(config_sql).collect()

	# Create Streamlit app (split statements for each SQL)
	streamlit_create_sql = f"""
		CREATE OR REPLACE STREAMLIT {APP_NAME}
		ROOT_LOCATION = '@streamlit_data_editor_generator.streamlit_data_editor_generator/branches/main/src'
		TITLE = '{APP_NAME}'
		MAIN_FILE = 'main.py'
		QUERY_WAREHOUSE = IT_DEVELOPER_WH
	"""
	session.sql(streamlit_create_sql).collect()

	return f'Streamlit app, config, and history table created: {APP_NAME}'
$$;
