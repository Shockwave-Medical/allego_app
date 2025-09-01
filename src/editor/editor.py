import streamlit as st
from snowflake.snowpark.functions import when_matched, when_not_matched, current_timestamp
import pandas as pd
import time
from functools import reduce
import pytz

# Get the current credentials


st.set_page_config(layout = 'wide')
# captions

def generate_editor(
    session,
    main_table_address: str,
    app_name: str,
    get_column_config,
    key_columns: list,
    required_columns: list,
    required_roles: list = [],
    filter_definitions: list = [],
    allowed_users: list = []

):
    SUCCESS_MSG_UPDATE = "Records updated successfully!"
    SUCCESS_MSG_INSERT = "Records added successfully!"
    ERROR_MSG = "Error updating records. Please try again."
    SUCCESS_MSG_DELETE = "Records deleted successfully!"
    ERROR_MSG_DELETE = "Error while deleting records. Delete might not have been successful."
    ERROR_MSG_HISTORY = "Error while updating the history table. Changes might not have been saved in history."
    NO_RECORDS_INSERTED = "No records were inserted!"
    DATA_EDITOR_KEY = "edited_data_key"
    
    #for temp_prod_pointing_branch
    DATABASE = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    schema_name = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
    history_table_address = f"{main_table_address}_HISTORY"
    current_user_name  = st.user["user_name"]
    app_name = app_name.replace("_", " ").title()
    user_lacks_required_access = False if len(allowed_users) == 0 else current_user_name not in allowed_users

    def get_source_table_data():
        query = f"SELECT * FROM {DATABASE}.{schema_name}.{main_table_address}"
        result = session.sql(query).collect()
        return result

    #setups
    source_table_data = session.create_dataframe(get_source_table_data())

    def create_add_row_df(source_table_data):
        column_names = source_table_data.to_pandas().columns.tolist()
        new_df = pd.DataFrame([{col: "" for col in column_names} for _ in range(10)])
        st.session_state.add_row_df = new_df
        
    if 'is_add_row_open' not in st.session_state:
        st.session_state.is_add_row_open = False

    if 'is_delete_enabled' not in st.session_state:
        st.session_state.is_delete_enabled = False

    if 'is_history_open' not in st.session_state:
        st.session_state.is_history_open = False

    if 'add_row_df' not in st.session_state:
        create_add_row_df(source_table_data)

    #if delete it enabled then make sure 

    st.session_state["HISTORY_TABLE"] = session.table(f"{DATABASE}.{schema_name}.{history_table_address}").to_pandas()
    st.session_state["MAIN_TABLE_ADDRESS"] = session.table(f"{DATABASE}.{schema_name}.{main_table_address}").to_pandas()
    history_df = session.table(f"{DATABASE}.{schema_name}.{history_table_address}").to_pandas()
    main_table = session.table(f"{DATABASE}.{schema_name}.{main_table_address}").to_pandas()
    edited_df = None

    def refresh_update_df(main_table=main_table):
        main_table= session.table(f"{DATABASE}.{schema_name}.{main_table_address}").to_pandas()

    def submit_data(edited_data):
        updated_indexes = st.session_state.get(DATA_EDITOR_KEY).get("edited_rows", []).keys()
        added_indexes = st.session_state.get(DATA_EDITOR_KEY).get("added_rows", [])

        if len(added_indexes) > 0:
            close_add_row_and_submit(pd.DataFrame(st.session_state.get(DATA_EDITOR_KEY).get("added_rows", [])))
            return
        if len(updated_indexes) == 0:
            st.warning("No changes made to the data.")
            return

        edited_data.loc[updated_indexes, "LAST_UPDATE_DATE"] = pd.Timestamp.now(tz=pytz.timezone("US/Pacific"))
        edited_data.loc[updated_indexes, "LAST_UPDATED_BY"] = current_user_name
        dataset = session.table(f"{DATABASE}.{schema_name}.{main_table_address}")
        updated_dataset = session.create_dataframe(edited_data)
        try:
            submit_edited_data_to_table(dataset, updated_dataset)
            insert_update_rows_to_history_table(edited_data)
            st.success(SUCCESS_MSG_UPDATE)
        except Exception as e:
            st.error(f"{ERROR_MSG}: {e}")
            st.stop()




    def get_column_config_history():
        history_columns = {
            'LAST_UPDATED_BY': st.column_config.TextColumn(label="LAST UPDATED BY", disabled=True),
            'ACTION_FLAG': st.column_config.TextColumn(label="ACTION FLAG", disabled=True),
            'UPDATE_DATA': st.column_config.TextColumn(label="UPDATED DATA", disabled=True),
            'CREATION_DATE': st.column_config.DatetimeColumn(label="CREATION DATE", disabled=True),
            'HISTORY_CREATION_DATE': st.column_config.DatetimeColumn(label="HISTORY CREATION DATE", disabled=True),
            'LAST_UPDATE_DATE': st.column_config.DatetimeColumn(label="LAST UPDATE DATE", disabled=True),
        }

        # add the main table columns to the history columns in the front
        main_table_columns = get_column_config()
        main_table_columns.update(history_columns)  # merge the two dictionaries
        return main_table_columns # merge the two dictionaries

    column_order = get_column_config_history().keys()  # use the keys from the history column config to set the order


    # dynamically apply filters for sales and pending order tabs
    def apply_filters(df, filter_definitions, col_layout=None):
        selected_filters = {}
        # Create a mapping of lowercase column names to actual column names
        col_map = {col.lower(): col for col in df.columns}
        if col_layout is None:
            col_layout = st.columns(len(filter_definitions) + 2)
        for i, filter_def in enumerate(filter_definitions):
            with col_layout[i + 1]:
                col_key = filter_def['column'].lower()
                actual_col = col_map.get(col_key, filter_def['column'])
                if filter_def['type'] == 'selectbox':
                    selected_filters[actual_col] = st.selectbox(
                        filter_def['label'],
                        options=['All'] + filter_def['options'],
                        index=0
                    )
                elif filter_def['type'] == 'date_range':
                    start_date, end_date = st.date_input(
                        filter_def['label'],
                        value=[filter_def['options'][0], filter_def['options'][1]]
                    )
                    selected_filters[actual_col] = (start_date, end_date)

        # apply the selected filters
        for column, value in selected_filters.items():
            if isinstance(value, tuple):  # for date ranges
                start_date, end_date = value
                df = df[
                    (df[column] >= pd.to_datetime(start_date)) &
                    (df[column] <= pd.to_datetime(end_date))
                ]
            elif value != 'All':  # for selectboxes
                df = df[df[column] == value]

        return df



    def find_already_existing_records(new_rows, existing_records):
    # Make key_columns case-insensitive by mapping to actual columns
        col_map = {col.lower(): col for col in existing_records.columns}
        key_cols_actual = [col_map.get(col.lower(), col) for col in key_columns]
        common_rows = existing_records.merge(new_rows, on=key_cols_actual, how='inner')
        return common_rows

    def find_key_column_duplicates(df):
        # Make key_columns case-insensitive by mapping to actual columns
        col_map = {col.lower(): col for col in df.columns}
        key_cols_actual = [col_map.get(col.lower(), col) for col in key_columns]

        # filter out "" rows
        df = df[df[key_cols_actual].ne("").any(axis=1)]
        # filter out none from rows
        df = df[df[key_cols_actual].notna().any(axis=1)]
        # Find duplicated keys (keep all occurrences of duplicates)
        duplicates = df[df.duplicated(subset=key_cols_actual, keep=False)]

        # Group by key columns to show each duplicate group
        grouped = duplicates.sort_values(key_cols_actual).groupby(key_cols_actual)

        return [group for _, group in grouped]


        
    
    def check_valid_columns_and_remove_empty_rows(df):
        # Check if all key columns are present in the DataFrame
        # return valid_df and no_missing_values
        no_missing_values = df[required_columns].notna().all().all()
        # filter out "" rows
        # Make key_columns case-insensitive by mapping to actual columns
        col_map = {col.lower(): col for col in df.columns}
        key_cols_actual = [col_map.get(col.lower(), col) for col in key_columns]
        valid_rows = df[df[key_cols_actual].ne("").any(axis=1)]
        # filter out none from rows
        valid_rows = valid_rows[valid_rows[key_cols_actual].notna().any(axis=1)]
        valid_rows = valid_rows[valid_rows[required_columns].notna().all(axis=1)]
        # invalid_rows are those rows that have missing values in some of the key columns but not all of them
        invalid_rows = df[~df[required_columns].notna().all(axis=1)]
        return valid_rows, invalid_rows, no_missing_values

    def insert_into_history_table(event_type, rows):
        try:
            history_table_columns = history_df.columns.tolist()

            # Find the intersection of columns between the history table and the new rows
            common_columns = list(set(history_table_columns).intersection(rows.columns))

            # Filter rows to include only the common columns
            rows = rows[common_columns]
            # Convert the pandas datetime to a plain string (else saying invalid date in history table - not sure why)
            rows["LAST_UPDATE_DATE"] = rows["LAST_UPDATE_DATE"].astype(str)
            rows["CREATION_DATE"] = rows["CREATION_DATE"].astype(str)

            if event_type == "INSERT":
                rows["ACTION_FLAG"] = "I"
            elif event_type == "UPDATE":
                rows["ACTION_FLAG"] = "U"
            elif event_type == "DELETE":
                rows["ACTION_FLAG"] = "D"


            rows["HISTORY_CREATION_DATE"] =  pd.Timestamp.now(tz=pytz.timezone("US/Pacific"))

            session.write_pandas(
                df= rows,
                table_name = history_table_address,
                database = DATABASE,
                schema = schema_name,
                auto_create_table = False,
                create_temp_table = False,
                overwrite = False
            )
        except Exception as e:
            st.error(f"{ERROR_MSG_HISTORY}: {e}")

    def create_join_expr(keys, dataset, updated_dataset):
        expr = (dataset[keys[0]] == updated_dataset[keys[0]])
        for key in keys[1:]:
            expr = expr & (dataset[key] == updated_dataset[key])
        return expr

    # merging the changes to original table
    def submit_edited_data_to_table(dataset, updated_dataset):
        # st.warning("Attempting to update dataset")
        dataset.merge(
            source=updated_dataset,
            join_expr= create_join_expr(key_columns, dataset, updated_dataset),
            clauses=[
                when_matched().update({
                    col: updated_dataset[col] for col in updated_dataset.columns
                    if col in dataset.columns
                }),
                when_not_matched().insert({
                    col: updated_dataset[col] for col in updated_dataset.columns
                    if col in dataset.columns
                })
            ]
        )
        time.sleep(.5)
        
    def insert_update_rows_to_history_table(updated_dataset=edited_df):
        edited = st.session_state.get(DATA_EDITOR_KEY).get("edited_rows", {})

        updated_row_indices = list(edited.keys())

        if len(updated_row_indices) > 0:
            updated_rows_df = updated_dataset.loc[updated_row_indices].copy()

            # Add UPDATE_DATA column with corresponding updated values
            updated_rows_df["UPDATE_DATA"] = updated_rows_df.index.map(
                lambda idx: str(edited[idx])
            )
            # if creation date is null then set it to current time
            updated_rows_df["CREATION_DATE"] = pd.Timestamp.now(tz=pytz.timezone("US/Pacific"))

            insert_into_history_table(event_type="UPDATE", rows=updated_rows_df)
            
    def close_add_row():
        st.session_state.update({"is_add_row_open": False})
        create_add_row_df(source_table_data)

    def apply_search(curren_df, search_text, all_data_df):
        filterd_df = all_data_df

        if search_text:
            filterd_df = curren_df[
                curren_df.apply(
                    lambda row: any(
                        search_text.lower() in str(value).lower() for value in row.astype(str).values
                    ),
                    axis=1,
                )
            ]
        else:
            filterd_df = all_data_df
        
        return filterd_df.reset_index(drop=True)

    @st.dialog("Submission blocked - Records already exist", width="large")
    def row_already_exist_dialog(rows):
        st.write(f"The following row(s) could not be inserted because they already exist. Please clear them to proceed with submission. You can update them using the 'Update or Delete Existing Records' grid below:")
        st.write(rows)
        if st.button("Ok", type="primary"):
            st.rerun()

    @st.dialog("Submission blocked - Duplicate Records", width="large")
    def duplicate_row_dialog(duplicate_groups):
        primary_keys_str = ", ".join(key_columns)
        st.write(f'The following row(s) could not be inserted because they have the same values in the unique keys: {primary_keys_str}')
        for i, group in enumerate(duplicate_groups, 1):
            st.write(f"Duplicate Group {i}:\n", group, "\n")
        if st.button("Ok", type="primary"):
            st.rerun()

    @st.dialog("Specific Records Saved", width="large")
    def valid_rows_dialog(rows):
        st.write(f"Only following rows are saved:")
        st.write(rows)
        if st.button("Ok", type="primary"):
            st.rerun()

    @st.dialog("Submission blocked - Invalid records", width="large")
    def invalid_rows_dialog(rows):
        unique_keys = list(set(required_columns + key_columns))
        unique_keys_str = ", ".join(unique_keys)
        st.write(f'The following row(s) could not be inserted because some unique keys such as {unique_keys_str} are empty:')
        st.write(rows)
        if st.button("Ok", type="primary"):
            st.rerun()

    @st.dialog("Submission blocked - Has unauthorized values", width="large")
    def unauthorized_values_dialog():
        st.write("You have unauthorized values in the new rows. Please check and try again.")
        if st.button("Ok", type="primary"):
            st.rerun()
            
    @st.dialog("Submission blocked - Invalid records", width="large")
    def invalid_rows_dialog(rows):
        unique_keys = list(set(required_columns + key_columns))
        unique_keys_str = ", ".join(unique_keys)
        st.write(f'The following row(s) could not be inserted because some unique keys such as {unique_keys_str} are empty:')
        st.write(rows)
        if st.button("Ok", type="primary"):
            st.rerun()


    def close_add_row_and_submit(new_add_rows_df):

        duplicate_rows_groups = find_key_column_duplicates(new_add_rows_df)
        if(len(duplicate_rows_groups)):
            duplicate_row_dialog(duplicate_rows_groups)
            return
        already_exsting_rows = find_already_existing_records(new_add_rows_df, source_table_data.to_pandas())
        if(len(already_exsting_rows)):
            row_already_exist_dialog(already_exsting_rows)
            return
        new_add_rows_df, invalid_rows, no_missing_values = check_valid_columns_and_remove_empty_rows(new_add_rows_df)
        if(len(invalid_rows)):
            invalid_rows_dialog(invalid_rows)
            return
        
        try:
            if(len(new_add_rows_df)):
                new_add_rows_df["CREATION_DATE"] = pd.Timestamp.now(tz=pytz.timezone("US/Pacific"))
                new_add_rows_df["LAST_UPDATE_DATE"] = pd.Timestamp.now(tz=pytz.timezone("US/Pacific"))
                new_add_rows_df["LAST_UPDATED_BY"] = current_user_name
                submit_edited_data_to_table(
                    session.table(f"{DATABASE}.{schema_name}.{main_table_address}"), 
                    session.create_dataframe(new_add_rows_df)
                )
                insert_into_history_table(event_type="INSERT", rows=new_add_rows_df)
                st.success(SUCCESS_MSG_INSERT)
                # if(not no_missing_values and len(new_add_rows_df)):
                #     valid_rows_dialog(new_add_rows_df)
                refresh_update_df()
            else:        
                st.warning(NO_RECORDS_INSERTED)
        except Exception as e:
            st.error(f"{ERROR_MSG}: {e}")
            st.stop()
        finally:

            close_add_row()

    def open_history_and_clear_filter():
        st.session_state.is_history_open = True

    def on_data_change(change_obj, df):
        deleted_indices = change_obj.get("deleted_rows", [])
        if len(deleted_indices) > 0:
            rows_delete_confirmation(deleted_indices, df)
        
    @st.dialog("Deleting Records", width="large")
    def rows_delete_confirmation(deleted_indices, df):
        st.write(f"Are you sure you want to delete the following records?")
        st.write(df.loc[deleted_indices])
        if st.button(
            "Confirm Delete", 
            type="primary", 
            on_click=lambda: delete_rows_in_snowflake(
            session.table(f"{DATABASE}.{schema_name}.{main_table_address}"), 
            df.loc[deleted_indices])
        ):
            # st.rerun()
            time.sleep(1)

    def delete_rows_in_snowflake(table, deleted_row_df: pd.DataFrame, key_columns=key_columns):
        try:
            if deleted_row_df.empty or len(deleted_row_df) == 0:
                return
            # Make key_columns case-insensitive by mapping to actual columns
            col_map = {col.lower(): col for col in deleted_row_df.columns}
            key_cols_actual = [col_map.get(col.lower(), col) for col in key_columns]
            # extract unique key‐tuples from pandas
            key_rows = deleted_row_df[key_cols_actual].drop_duplicates().to_dict(orient="records")
            # lift into Snowpark dataframe
            source_df = table.session.create_dataframe(key_rows, schema=key_cols_actual)

            # build compound match condition: AND over all key columns
            conditions = [table[col_name] == source_df[col_name] for col_name in key_cols_actual]
            compound = reduce(lambda a, b: a & b, conditions)
            table.delete(compound, source_df)
            deleted_row_df[ "LAST_UPDATE_DATE"] = pd.Timestamp.now(tz=pytz.timezone("US/Pacific"))
            deleted_row_df["LAST_UPDATED_BY"] = current_user_name
            try:
                insert_into_history_table(event_type="DELETE", rows=deleted_row_df)
            except Exception as e:
                st.error(f"Error while inserting delete action into history table: {e}")
            st.success(SUCCESS_MSG_DELETE)
        except Exception as e:
            # st.rerun()
            st.error(f"{ERROR_MSG_DELETE}: {e}")
    
    def enrich_filter_definitions(filter_definitions):
        df = st.session_state["MAIN_TABLE_ADDRESS"]
        # Create a mapping of lowercase column names to actual column names
        col_map = {col.lower(): col for col in df.columns}
        for filter in filter_definitions:
            filter["type"] = "selectbox"
            col_key = filter["column"].lower()
            if col_key in col_map:
                filter["options"] = list(df[col_map[col_key]].unique())
            else:
                filter["options"] = []
        return filter_definitions

    if user_lacks_required_access:
        st.info(f'  You are not a part of user group allowed submit changes. Add your name to ALLOWED_USERS in the STREAMLIT_APP_CONFIG table to submit changes.', icon="ℹ️")

    filter_definitions = enrich_filter_definitions(filter_definitions)

    if st.session_state.is_history_open:
        st.title("")
        history_search_col, col_empty_history, back_button = st.columns([1,3, 1], gap="small", vertical_alignment="bottom")
        with history_search_col:
            search_text_history = st.text_input("Search:", placeholder="Search by any field")
            if search_text_history:
                history_df = apply_search( 
                    history_df, 
                    search_text_history,
                    st.session_state["HISTORY_TABLE"]
                )
                
        back_button.button(
            f"Return to {app_name}", 
            icon=":material/chevron_left:",
            on_click=lambda: st.session_state.update({"is_history_open": False}), 
            disabled=not st.session_state.is_history_open
        )
        st.data_editor(
            data= history_df,
            column_config=get_column_config_history(),
            use_container_width=True,
            column_order=column_order,
            hide_index=True,
        )

    if not st.session_state.is_history_open:
        st.title(app_name)

        col_layout = st.columns(len(filter_definitions) + 2, gap="small", vertical_alignment="bottom")
        
        with col_layout[0]:
            search_text_defination = st.text_input("Search:", placeholder="Search by any field", key="search_text")
            if search_text_defination:
                main_table = apply_search(
                    main_table, 
                    search_text_defination,
                    st.session_state.get(f"{schema_name}.{main_table_address}"),
                )
        main_table = apply_filters(
            main_table, filter_definitions, col_layout
        )
        # remove index column
        main_table = main_table.reset_index(drop=True)
        edited_df = st.data_editor(
            data= main_table,
            column_config=get_column_config(),
            use_container_width=True,
            num_rows="dynamic",
            column_order=get_column_config().keys(),
            hide_index=True,
            on_change =lambda: on_data_change(st.session_state.get(DATA_EDITOR_KEY), main_table),
            key=DATA_EDITOR_KEY

        )



    submit_button_col, col_empty2, veiw_history_col = st.columns([1,4, 1], gap="small", vertical_alignment="bottom")
    if not st.session_state.is_history_open:
        submit_button_col.button("Submit Changes", on_click=lambda: submit_data(edited_df), disabled=user_lacks_required_access)
        veiw_history_col.button("View History", type="tertiary",  icon=":material/history:", on_click=open_history_and_clear_filter, use_container_width=True)
    
