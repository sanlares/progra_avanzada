import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from psycopg2 import pool
import os

app = FastAPI()

class Database:
    #clase singleton para manejar el pool de conexiones
    _connection_pool = None

    @staticmethod
    def initialize():
        Database._connection_pool = pool.SimpleConnectionPool(
            1, # mínimo número de conexiones
            10, # máximo número de conexiones
            user="postgres"#os.getenv("DB_USER", "postgres")
            ,
            password="conesacolgiales"#os.getenv("DB_PASSWORD", "conesacolegiales")
            ,
            host="programacion.cpusky0oqvsv.us-east-2.rds.amazonaws.com"#os.getenv("DB_HOST", "programacion.cpusky0oqvsv.us-east-2.rds.amazonaws.com")
            ,
            port="5432"#os.getenv("DB_PORT", "5432")
            ,
            database="postgres"#os.getenv("DB_NAME", "postgres")
        )

    @staticmethod
    def get_connection():
        return Database._connection_pool.getconn()

    @staticmethod
    def return_connection(connection):
        Database._connection_pool.putconn(connection)

    @staticmethod
    def close_all_connections():
        Database._connection_pool.closeall()

# inicializa el pool al iniciar la aplicación
Database.initialize()

@app.get("/recommendations/{ADV}/{Modelo}")
def recommendations(ADV: str, Modelo: str):
    print("Connecting to DB")
    conn = Database.get_connection()
    try:
        cur = conn.cursor()
        print("Connected to DB")

        today_date = datetime.now().strftime('%Y-%m-%d')
        table = "top_ctr_table" if Modelo == "TopCTR" else "top_products_table"
        
        print("Executing Query")
        query = f"SELECT product_id FROM {table} WHERE advertiser_id = %s AND date = %s;"
        cur.execute(query, (ADV, today_date))
        recomm_table = cur.fetchall()
        print("Query Executed")

        return {"data": recomm_table}
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        cur.close()
        Database.return_connection(conn)

@app.get("/stats/") 
def stats(): 

    return f''

@app.get("/history/{ADV}") 
def history(): 

    return f''

