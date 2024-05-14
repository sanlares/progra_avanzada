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

@router.get("/{advertiser_id}")
def get_history(advertiser_id:str):
    conn=None
    cur=None
    try:
        conn =Database.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        yesterday_date = (datetime.now() - timedelta(days=1))
        
        #calcular la fecha de hace 7 dias
        seven_days_ago= yesterday_date - timedelta(days=7)
        yesterday_date= yesterday_date.strftime('%Y-%m-%d')
        formatted_date = seven_days_ago.strftime('%Y-%m-%d')
        
        #ejecuto la consulta SQL

        cur.execute("""
                    SELECT date,product_id,ctr FROM  top_ctr_table
                    WHERE advertiser_id = %s AND date >= %s
                    ORDER BY date DESC
                    """, (advertiser_id,formatted_date))
        ctr_data = cur.fetchall()
        
        cur.execute("""
                    SELECT date,product_id,visitas FROM  top_products_table
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
