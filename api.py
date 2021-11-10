from fastapi import FastAPI #restapi
import psycopg2 #postgresql db
import pandas as pd #excel function
from typing import List, Optional, Set, Any, Dict
from pydantic import BaseModel
import uvicorn #server implementation

app = FastAPI() 

class DB_conn(BaseModel):
    query: str 
    host: str 
    port: str 
    database_name: str 
    username: str 
    password: str
    # dbparams: List
    
def DB_connect(host, port, database_name, username, password):
    conn=psycopg2.connect(
    host = host,
    port = port,
    database = database_name,
    user = username,
    password = password)
    cur = conn.cursor()    
    return cur


@app.get('/read/')
def reada(data: str):
    print(data)
    return
    
    
@app.post('/readall')
def readall(data: DB_conn):
    cur = DB_connect(data.host, data.port, data.database_name, data.username, data.password)
    cur.execute(DB_conn.query)
    tmp = cur.fetchall()
    try:
        col_names = []
        for elt in cur.description:
            col_names.append(elt[0])
        df = pd.DataFrame(tmp, columns=col_names)
        print("extracted")
        return df.to_json()
    
    except Exception as e:
        return "Table not Present: ", e
    
@app.post('/create')
def create(data: DB_conn):
    try:
        cur = DB_connect(data.host, data.port, data.database_name, data.username, data.password)
        cur.execute(DB_conn.query)
        return "successfully created"
    except Exception as e:
        return "Table not Present: ", e
    
@app.post('/insert')
def insert(data: DB_conn):
    try:
        cur = DB_connect(data.host, data.port, data.database_name, data.username, data.password)
        cur.execute(DB_conn.query)
        return "successfully inserted"
    except Exception as e:
        return "Table not Present: ", e

@app.post('/update')
def update(data: DB_conn):
    try:
        cur = DB_connect(data.host, data.port, data.database_name, data.username, data.password)
        cur.execute(DB_conn.query)
        return "successfully inserted"
    except Exception as e:
        return "Table not Present: ", e
    
@app.post('/delete')
def delete(data: DB_conn):
    try:
        cur = DB_connect(data.host, data.port, data.database_name, data.username, data.password)
        cur.execute(DB_conn.query)
        return "successfully inserted"
    except Exception as e:
        return "Table not Present: ", e
    
if __name__ == "__main__":
    uvicorn.run(app='api:app', host="127.0.0.1", port=4004, reload=True)
