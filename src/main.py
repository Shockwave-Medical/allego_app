import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
from editor.editor import generate_editor
from snowflake.snowpark.context import get_active_session


# Initialize Snowflake session (assumes Streamlit Snowflake integration)
session = get_active_session()

def get_column_config_generic(table_df):
	return {
		col: st.column_config.Column(
			label=col.replace('_', ' ').title(),
			disabled=(col == "creation_date")
		) for col in table_df.columns
	}

def create_filter_definitions(filtered_fields):
	filter_definitions = []
	if filtered_fields:
		for col in filtered_fields.split(","):
			col = col.strip()
			if col:
				filter_definitions.append({
					"label": col.replace("_", " ").title(),
					"column": col
				})
	return filter_definitions

# Read all app configs
def get_all_app_configs(session, schema_name):
	query = f"SELECT * FROM {schema_name}.STREAMLIT_APP_CONFIG"
	return session.sql(query).to_pandas()

def main():
	schema_name = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
	app_configs = get_all_app_configs(session, schema_name)
	
	if not app_configs.empty:
		config = app_configs.iloc[0]
		filter_definitions = create_filter_definitions(config.get('FILTERED_FIELDS', None))
        
		required_columns = config.get('REQUIRED_COLUMNS', None)
		if required_columns:
			required_columns = required_columns.split(",")
		else:
			required_columns = []

		generate_editor(
			session=session,
			main_table_address=config['TABLE_NAME'],
			app_name=config['APP_NAME'],
			get_column_config=lambda: get_column_config_generic(session.sql(f"SELECT * FROM {config['TABLE_NAME']}").to_pandas()),
			key_columns=config['KEY_COLUMNS'].split(","),
			required_columns=required_columns,
			required_roles=[],
			filter_definitions=filter_definitions,
			allowed_users=config.get('ALLOWED_USERS', "").split(",") if config.get('ALLOWED_USERS', "") else []
		)

if __name__ == "__main__":
	main()
