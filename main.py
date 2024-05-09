from fastapi import FastAPI 
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import os

app = FastAPI()

@app.get("/recommendations/{ADV}/{Modelo}") 
def recommendations(ADV: int, Modelo: str):

    #me conecto a la base de datos
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='conesacolegiales',
        host='programacion.cpusky0oqvsv.us-east-2.rds.amazonaws.com',
        port=5432
    )
    cur = conn.cursor()

    today_date = datetime.now().strftime('%Y-%m-%d')

    if Modelo=="TopCTR":
        table="top_ctr_table"
    else:
        table="top_products_table"
    
    recomm_table=cur.execute(f'select product_id from {table} where advertiser_id={ADV} and date={today_date}')

    return recomm_table

@app.get("/stats/") 
def stats(): 

    return f'Random number: {np.random.randint(min+1, max+1)}'

@app.get("/history/{ADV}") 
def history(): 

    return f'Random number: {np.random.randint(min+1, max+1)}'

