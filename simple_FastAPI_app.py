from fastapi import FastAPI
import uvicorn
import uuid
from random import randint, randrange
from fastapi import HTTPException
import asyncio, asyncpg
from table_schema_creation_addition import *





app = FastAPI() 

@app.get("/")
async def starter():
    return "Please insert client information or request status"

@app.get("/project/")
async def create_request(client_info_1: str = "", client_info_2: str = "", year: str = ""):
    connection_dwh = gp_connect_to_db(secret_db)
    curs_dwh = connection_dwh.cursor()
    df_check_exists_query = f"""select * from table
                        where client_info_1 = '{client_info_1}'
                        and client_info_2 = '{client_info_2}'
                        and status = 'Completed'
                        and rep_date >= NOW() - INTERVAL '1 days'
                        and data::jsonb->'year'::varchar = '{year}'
                        """
    df_check_exists = psql.read_sql(df_check_exists_query
                , connection_dwh)
            
    if len(df_check_exists)>0:
        response_message = {"status": 'COMPLETED', 'data': df_check_exists['data'], 
        'time' : df_check_exists['time']}
        
        return ('the request exists', response_message)

    request_id = randint(1, 999999999)
        
    application_number = request_id
    print(application_number)
    curs_dwh.execute(f"""INSERT INTO table (time, application_number, status, client_info_1, client_info_2, data)
                        VALUES (NOW(), '{application_number}', 'queued', '{client_info_1}', '{client_info_2}', '{{"year":{year}}}')                      
                    """)
    connection_dwh.commit()
    connection_dwh.close()


@app.get("/project_check_status/{request_id}")
async def check_status(request_id):
    connection_dwh = gp_connect_to_db(secret_db)
    application = psql.read_sql(f"""select * from table
        where application_number = '{request_id}' 
        limit 1
        """, connection_dwh)
    
    if len(application)<1:
        raise HTTPException(status_code = 404, detail = "Application with such request id is not found")

    if application.status.iloc[0] == 'Completed':
        response_message = {"status": application['status'], 'data': 'new_data', 'time' : application['time']}
    else: 
        response_message = {"status": application['status'], 'time' : application['time']}

    return response_message
    
if __name__ == "__main__":
    uvicorn.run('project_apis:app', port=80, host="0.0.0.0")


