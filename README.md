# Handy scripts
These are listed some of my every-day scripts I was using to complete certain tasks.
!Note that some of them might be outdated as they were written some years ago, nevertheless - they are useful :)

Here is the little description of each of the files [in the order of appearences]:
1. clickhouse_table_as_dataframe.py - Clickhouse-driver doesn't have native function to return the query result as pandas dataframe, that's why it needs manual implementation.
   - function *query_as_df*:
     - Input - query (e.g 'select * from table where 1=1'), clickhouse client (your database and user credentials, see connect_to_databases.py for example)
     - Output - pandas DataFrame object
   - function *insert_df_to_clickhouse_table*: #maybe it is easy for someone to use functions instead of straight implementation
     - Input - pandas DataFrame to be inserted into CH table, name of the table, CH client
     - Output - None
3. connect_to_databases.py - scripts to access the Vault to get the credentials for database connection. Includes Clickhouse Client initialization
4. dataframe_into_excel_sheets.py - simple example of how to gather two or more dataframes into one Excel file sheet by sheet. Useful for products analysis.
5. get_useful_relative_date.py -
   - function *take_table_info_from_last_months* - calculates the correct last date of previous N months from now, e.g if today is Feb 6, 2025, if N = 3, previous_day would be October   31, 2024. Useful for correct retrieve of data between dates. There might be more effective implementation
     - Input - month_delta (integer)
     - Output - pandas dataframe with data between month_delta and first day of this month (both are not included) - if N=3 -> data would be retrieved between >2024.10.31 and <2025.02.01
   - function *last_day_of_next_months* - returns the last day of the next month. If month_delta is 1, function would return the last day for current month (Feb 28, 2025)
   - There is also a simple example of usage of first function in a query
6. scheduler.py - simple scheduler with the usage of which it is possible to schedule simple or complex tasks that has short period
7. simple_AIRFLOW_dag.py - example usage of scheduling regular table operations using powerful Apache Airflow tool. Task order could be specified.
8. simple_FastAPI_app.py - example usage of developing fastapi app that interacts with PostgreSQL based databases. Useful for task completion given by clients.
9. table_schema_creation_addition.py - scripts to easily create table in database just from dataframe, inserting this dataframe easily, creating schema and deleting the content of the tables.
10. yandex_appmetrica_downloads.py - useful script to download the app activity logged by Yandex AppMetrica (events, installations, clicks)
