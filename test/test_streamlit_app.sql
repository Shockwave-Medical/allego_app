-- Test SQL for Streamlit Data Editor Generator
-- 1. Create source table from CSV

CREATE OR REPLACE TABLE DEVELOPMENT.SAMPLE_TABLE (
    id INT,
    name STRING,
    age INT,
    department STRING,
    salary INT,
    start_date DATE
);

-- Load data (use Snowflake UI or COPY INTO for CSV upload)
-- Example:
-- COPY INTO DEVELOPMENT.SAMPLE_TABLE
-- FROM '@~/sample_table.csv'
-- FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- 2. Create config table
CALL DEVELOPMENT.STREAMLIT_DATA_EDITOR_GENERATOR.CREATE_CONFIG_TABLE('DEVELOPMENT');

-- 3. Create history table
CALL DEVELOPMENT.STREAMLIT_DATA_EDITOR_GENERATOR.CREATE_HISTORY_TABLE('DEVELOPMENT', 'SAMPLE_TABLE');


-- CREATE_STREAMLIT_APP(
--   schema_name,           -- The schema where tables/configs are stored
--   app_name,              -- The name of the Streamlit app
--   source_table,          -- The source table for the app
--   filtered_fields,       -- Comma-separated fields to filter on
--   required_columns,      -- Comma-separated required columns
--   primary_key_columns    -- Comma-separated primary key columns
--   allowed_users          -- Comma-separated allowed users
-- )

CALL DEVELOPMENT.STREAMLIT_DATA_EDITOR_GENERATOR.CREATE_STREAMLIT_APP(
    'STREAMLIT_DATA_EDITOR_TEST',       -- schema_name
    'TEST_STREAMLIT_APP',               -- app_name
    'TEST_TABLE',                     -- source_table
    'id,name',                          -- filtered_fields
    'age,department,salary,start_date', -- required_columns
    'id,name,age',                      -- primary_key_columns
    'SBANERJEEE,SBANERJEEE_JNJ'         -- allowed_users
);

-- 4b. Create Streamlit app config with only REQUIRED_COLUMNS (FILTERED_FIELDS optional)
CALL DEVELOPMENT.STREAMLIT_DATA_EDITOR_GENERATOR.CREATE_STREAMLIT_APP(
    'DEVELOPMENT',
    'SAMPLE_STREAMLIT_APP2',
    'SAMPLE_TABLE',
    'id',
    NULL,
    'id,name'
);

-- 4c. Create Streamlit app config with only FILTERED_FIELDS (REQUIRED_COLUMNS optional)
CALL DEVELOPMENT.STREAMLIT_DATA_EDITOR_GENERATOR.CREATE_STREAMLIT_APP(
    'DEVELOPMENT',
    'SAMPLE_STREAMLIT_APP3',
    'SAMPLE_TABLE',
    'id',
    'department',
    NULL
);

-- 5. Create Streamlit app only
CALL DEVELOPMENT.STREAMLIT_DATA_EDITOR_GENERATOR.CREATE_STREAMLIT_APP_ONLY('DEVELOPMENT', 'SAMPLE_STREAMLIT_APP');

-- 6. Query results for verification
SELECT * FROM DEVELOPMENT.STREAMLIT_APP_CONFIG;
SELECT * FROM DEVELOPMENT.SAMPLE_TABLE_HISTORY;
SELECT * FROM DEVELOPMENT.SAMPLE_TABLE;
