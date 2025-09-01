# Streamlit Data Editor Generator

## Overview
This project provides a modular framework for generating Streamlit data editor applications that interact with Snowflake tables. It automates the creation of configuration tables, history tracking, and user access management, allowing rapid deployment and management of data editing interfaces in Snowflake using Streamlit.

## Features
- **Automated App Creation:** SQL and Python procedures to create Streamlit apps based on dynamic configuration.
- **Config Table Management:** Procedures to create and manage app configuration tables.
- **History Tracking:** Automated creation of history tables to track changes and edits.
- **User Access Control:** Management of allowed users for editing data.
- **Case-Insensitive Column Handling:** Ensures robust data operations regardless of column name casing.
- **Modular SQL Scripts:** All major database objects and procedures are created via modular, reusable SQL scripts.
- **MTP Automation:** Includes an MTP (Migration/Transformation/Provisioning) script to automate repository fetch and object creation.

## How It Works
1. **Repository Fetch:** The MTP script fetches the latest repository and executes all necessary SQL scripts to set up the environment.
2. **App Configuration:** Configuration tables define which columns are editable, required, or filtered in the Streamlit app.
3. **App Generation:** Python and SQL procedures generate the Streamlit app and supporting tables based on config.
4. **Data Editing:** Users interact with the Streamlit app to edit Snowflake tables, with all changes tracked in history tables.

## File Structure
- `src/editor/editor.py`: Main logic for Streamlit data editor generation and data operations.
- `src/main.py`: Entry point for launching the Streamlit app.
- `src/sql/`: SQL scripts for creating tables, procedures, and MTP automation.
- `test/`: Test scripts for validating app and table creation.

## Usage
1. Run the MTP script to set up all required database objects:
   ```sql
   USE ROLE IT_DEVELOPER;
   ALTER GIT REPOSITORY HR_INTEGRATION_STREAMLIT FETCH;
   -- Execute all EXECUTE IMMEDIATE statements in mtp.sql
   ```
2. Launch the Streamlit app using `main.py`.
3. Edit data in Snowflake tables via the Streamlit interface.

## Requirements
- Snowflake account with Streamlit integration
- Python 3.11+
- Streamlit
- Snowflake Snowpark

## License
MIT
