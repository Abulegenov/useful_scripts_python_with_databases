from connect_to_databases import *
from datetime import date

today = date.today()
df_table_1 = psql.read_sql(f"""
    SELECT 
    product, price
    FROM table_1
    WHERE date = '{today}'
     """, connection_db)  

df_table_2 = psql.read_sql(f"""
    SELECT 
    product, price
    FROM table_2
    WHERE date = '{today}'
     """, connection_db)  


print(today)
#Create the EXCEL file with different sheet for each of tables
with pd.ExcelWriter('all_products_'+str(today)+'.xlsx') as writer:
    df_table_1.to_excel(writer, sheet_name = 'Table_1')
    df_table_2.to_excel(writer, sheet_name = 'Table_2')