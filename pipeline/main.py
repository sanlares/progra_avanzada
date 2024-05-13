import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
import os
from datetime import timedelta
import psycopg2.extras 
import psycopg2
from datetime import timedelta
import pandas as pd
from io import StringIO
import boto3

app = FastAPI()

class Database:
    #clase singleton para manejar el pool de conexiones
    _connection_pool = None

    @staticmethod
    def initialize(): #se conecta
        Database._connection_pool = pool.SimpleConnectionPool(
            1, # mínimo número de conexiones
            10, # máximo número de conexiones
        dbname='postgres',
        user='postgres',
        password='conesacolegiales',
        host='tpprogramacion.cpusky0oqvsv.us-east-2.rds.amazonaws.com',
        port=5432
        )

    @staticmethod
    def get_connection(): #devuelve la conexion
        return Database._connection_pool.getconn()

    @staticmethod
    def return_connection(connection):
        Database._connection_pool.putconn(connection)

    @staticmethod
    def close_all_connections():
        Database._connection_pool.closeall()

# inicializa el pool al iniciar la aplicación
Database.initialize()

@app.get("/recommendations/{advertiser_id}/{Modelo}")
def recommendations(advertiser_id: str, Modelo: str):
    print("Connecting to DB")
    conn = None
    cur = None
    try:
        conn = Database.get_connection()
        print("Connected to DB")
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        print("Executing Query")
        yesterday_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        if Modelo == 'TopCTR':
                table = 'top_ctr_table'
                query = f"SELECT  product_id FROM {table} WHERE date = %s AND advertiser_id = %s;"
        elif Modelo =='Topproducts':
                table = 'top_products_table'
                query=f'SELECT  product_id FROM {table} WHERE date = %s AND advertiser_id = %s;'
        else:
            raise HTTPException(status_code=400, detail=f"Modelo desconocido: {Modelo}")
        
        cur.execute(query, (yesterday_date, advertiser_id))
        recomm_table = cur.fetchall()
               
        print("Query Executed")
        return {"data": recomm_table}
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        if cur:
            cur.close()
        if conn:
            Database.return_connection(conn)
            
@app.get("/stats/") 
def statstistics_summary():
    conn = None
    cur = None
    try: 
        conn = Database.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

#query para contar la cantidad de advertisers unicos en ambas tablas
        cur.execute(f"SELECT COUNT(DISTINCT advertiser_id) AS total_advertisers_for_ctr_and_products FROM top_ctr_table WHERE date ='{yesterday_date}' UNION ALL SELECT COUNT(DISTINCT advertiser_id) FROM top_products_table WHERE date='{yesterday_date}'")
        total_advertisers_for_ctr_and_products = cur.fetchall()
        
#query para estimar el top 5  de ctr 
        cur.execute("""
                    SELECT advertiser_id, product_id, CTR AS max_ctr
                    FROM top_ctr_table
                    ORDER BY CTR DESC
                    LIMIT 5
                    """)
        results = cur.fetchall()

#query para estimar el top 5  de productos
        cur.execute("""
                    SELECT advertiser_id, product_id, visitas AS max_products
                    FROM top_products_table
                    ORDER BY visitas DESC
                    LIMIT 5
                    """)
        results_products = cur.fetchall()

#promedio de ctr y visitas esto nos da una idea de que tan bien esta realizando los anunciantes y los productos. 
        cur.execute("""
                    SELECT advertiser_id, product_id, AVG(CTR) AS average_ctr
                    FROM top_ctr_table
                    GROUP BY advertiser_id, product_id
                    ORDER BY average_ctr DESC
                    LIMIT 5
                    """)
        average_ctr = cur.fetchall()
        
        cur.execute("""
                    SELECT advertiser_id, product_id, AVG(visitas) AS visitas_average
                    FROM top_products_table
                    GROUP BY advertiser_id, product_id
                    ORDER BY visitas_average DESC
                    LIMIT 5
                    """)
        visitas_average= cur.fetchall()
        
        return{
            "total_advertisers_for_ctr_and_products":[total['total_advertisers_for_ctr_and_products'] for total in total_advertisers_for_ctr_and_products],
            "top5_ctr":results,
            "top5_products":results_products,
            "average_ctr":average_ctr,
            "visitas_average":visitas_average
        }
    except Exception as e:
        print(f"An error occurred:{e}")
        raise HTTPException(status_code=500,detail="Internal Server Error")
    finally:
        if cur:
            cur.close()
        if conn:
            Database.return_connection(conn)
            
@app.get("/history/{advertiser_id}")
def get_history(advertiser_id:str):
    conn=None
    cur=None
    try:
        conn =Database.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        yesterday_date = (datetime.now() - timedelta(days=1))
        
        #calcular la fecha de gace 7 dias
        seven_days_ago= yesterday_date - timedelta(days=7)
        yesterday_date= yesterday_date.strftime('%Y-%m-%d')
        formatted_date = seven_days_ago.strftime('%Y-%m-%d')
        
        #ejecuto la consulta SQL
        
        cur.execute("""
                    SELECT * FROM  top_ctr_table
                    WHERE advertiser_id = %s AND date >= %s
                    ORDER BY date DESC
                    """, (advertiser_id,formatted_date))
        ctr_data = cur.fetchall()
        
        cur.execute("""
                    SELECT * FROM  top_products_table
                    WHERE advertiser_id = %s AND date >= %s
                    ORDER BY date DESC
                    """, (advertiser_id,formatted_date))
        products_data = cur.fetchall()
        
        return{
            "ctr_data":ctr_data,
            "products_data":products_data
        }
    except Exception as e:
        print(f'An error occurred:{e}')
        raise HTTPException(status_code=500,detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            Database.return_connection(conn)        

        
 
