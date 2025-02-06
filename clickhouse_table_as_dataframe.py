from connect_to_databases import *

def query_as_df(query = '', client=client):
    """Returns proper representation of clickhouse table with correct column names"""
    client.execute("""set distributed_product_mode = 'global' 
""") 
    new_query = client.execute(f""" describe table ({query} )""")
    col_names = []
    for i in new_query:
        col_names.append(i[0])
    query = client.execute(f"{query}")
    df = pd.DataFrame(query, columns = col_names)
    return df

def insert_df_to_clickhouse_table(dataframe, table_name, schema_name, client):
    """Inserts dataframe into clickhouse table"""
    client.insert_dataframe(f"INSERT INTO {schema_name}.{table_name} VALUES", dataframe)