from connect_to_databases import *
import datetime

def create_schema_psql(schema_name,curs,connection):
    schema_query = 'CREATE SCHEMA IF NOT EXISTS '+ schema_name + ' AUTHORIZATION user'
    curs.execute(schema_query)
    connection.commit() #To commit changes

#script to quickly create table from just the dataframe with the same column names
def create_table_psql(df, table_name, schema_name, connection,curs):
    """Input is any dataframe, any table name, schema to store table, connection to db and its cursor"""
    columnName = list(df.columns.values)
    def getColumnDtypes(dataTypes):
        dataList = []
        for x in dataTypes:
            if x == 'int64':
                dataList.append('bigint')
            elif x == 'float64':
                dataList.append('double precision')
            elif x == 'bool':
                dataList.append('boolean')
            elif x == 'uint64':
                dataList.append('numeric')
            else:
                dataList.append('varchar')  
        return dataList
    columnDataType = getColumnDtypes(df.dtypes)
    sql = 'CREATE TABLE IF NOT EXISTS '+schema_name+'.' + table_name +' ('
    for i in range(len(columnDataType)):
        sql = sql +'\n' + columnName[i] + ' ' + columnDataType[i] + ','
    sql = sql[:-1] + ' );'
    try:
        curs.execute(sql)
        print('TABLE HAS BEEN SUCCESSFULLY CREATED')
        curs.close()
    except Exception as e:
        print(f"{type(e).__name__}:{e}")
        print(f"Query: {curs.query}")
        connection.rollback()
        curs.close()
    else:
        connection.commit()

#script to quickly insert dataframe to table in db
def add_table_psql(df,table_name_sql,schema_name,connection,curs):
    """Inserting of any dataframe to any table as long as types and number of columns match"""
    try:
        now=datetime.datetime.now()
        query_initial ='INSERT into ' + schema_name +'.'+table_name_sql+' values' 
        list_s = ['%s']*len(list(df.columns.values) )
        add_query = list_s[0]
        for i in range(0,len(list_s)-1):
            add_query = add_query + ', ' + list_s[i]
        query_final = query_initial + '(' + add_query + ')'
        args = [tuple(x) for x in df.values]
        psycopg2.extras.execute_batch(curs, query_final, args)
        now_final=datetime.datetime.now()
        print('Time taken to insert dataframe into table:', now_final - now)
        curs.close()
        print("Table successfully inserted to " + schema_name +'.'+table_name_sql) 
    except Exception as e:
        print('ERROR in inserting to ' + schema_name +'.'+table_name_sql, e)
        connection.rollback()
        curs.close()
    connection.commit()

def delete_table_psql(table_name, schema_name, connection, curs):
    """Safe deletion of table rows (can be rolled back)"""
    sql =  'delete from '+schema_name+'.' + table_name
    try:
        curs.execute(sql)
        print('TABLE CONTENT HAS BEEN SUCCESSFULLY DELETED')
        curs.close()
    except Exception as e:
        print(f"{type(e).__name__}:{e}")
        print(f"Query: {curs.query}")
        connection.rollback()
        curs.close()
    connection.commit()

