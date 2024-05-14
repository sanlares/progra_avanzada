# routers/recommendations.py
from fastapi import APIRouter
from database import Database
from datetime import datetime
from fastapi import FastAPI, HTTPException
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
import os
from datetime import timedelta
import psycopg2.extras 
import psycopg2

router = APIRouter()

@router.get("/recommendations/{advertiser_id}/{Modelo}")
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
        elif Modelo =='TopProduct':
                table = 'top_products_table'
                query=f'SELECT  product_id FROM {table} WHERE date = %s AND advertiser_id = %s;'
        else:
            raise HTTPException(status_code=400, detail=f"Modelo desconocido: {Modelo}")
        
        cur.execute(query, (yesterday_date, advertiser_id))
        recomm_table = cur.fetchall()
               
        print("Query Executed")
        return {"data": recomm_table}
    except psycopg2.DatabaseError as e:
        print(f"Database error occurred: {e}")
        raise HTTPException(status_code=500, detail="Database Error")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        if cur:
            cur.close()
        if conn:
            Database.return_connection(conn)