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

@router.get("/stats/") 
def statitistics_summary():
    conn = None
    cur = None
    try: 
        conn = Database.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        yesterday_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    
    except Exception as e:
        print(f"An error occurred while connecting to the database: {e}")
        raise HTTPException(status_code=500,detail="Internal Server Error")

    try:
        cur.execute(f"""SELECT COUNT(DISTINCT advertiser_id) AS total_advertisers_for_ctr_and_products 
                    FROM top_ctr_table WHERE date ='{yesterday_date}' 
                    UNION ALL 
                    SELECT COUNT(DISTINCT advertiser_id) 
                    FROM top_products_table 
                    WHERE date='{yesterday_date}'""")
        total_advertisers_for_ctr_and_products = cur.fetchall()
    except Exception as e:
        print(f"An error occurred while executing total_advertisers_for_ctr_and_products query: {e}")
        raise HTTPException(status_code=500,detail=str(e))

    try:
        cur.execute("""
                    SELECT advertiser_id, product_id, CTR AS max_ctr
                    FROM top_ctr_table
                    ORDER BY CTR DESC
                    LIMIT 5
                    """)
        results = cur.fetchall()
    except Exception as e:
        print(f"An error occurred while executing top5_ctr query: {e}")
        raise HTTPException(status_code=500,detail=str(e))

#query para estimar el top 5  de productos
    #    cur.execute("""
    #                SELECT advertiser_id, product_id, visitas AS max_products
    #                FROM top_products_table
    #                ORDER BY visitas DESC
    #                LIMIT 5
    #                """)
    #    results_products = cur.fetchall()
#
#pro#medio de ctr y visitas esto nos da una idea de que tan bien esta realizando los anunciantes y los productos. 
    #    cur.execute("""
    #                SELECT advertiser_id, product_id, AVG(CTR) AS average_ctr
    #                FROM top_ctr_table
    #                GROUP BY advertiser_id, product_id
    #                ORDER BY average_ctr DESC
    #                LIMIT 5
    #                """)
    #    average_ctr = cur.fetchall()
    #    
    #    cur.execute("""
    #                SELECT advertiser_id, product_id, AVG(visitas) AS visitas_average
    #                FROM top_products_table
    #                GROUP BY advertiser_id, product_id
    #                ORDER BY visitas_average DESC
    #                LIMIT 5
    #                """)
    #    visitas_average= cur.fetchall()"""
        
        return{
            "total_advertisers_for_ctr_and_products":[total['total_advertisers_for_ctr_and_products'] for total in total_advertisers_for_ctr_and_products],
            "top5_ctr":results,
            #"top5_products":results_products,
            #"average_ctr":average_ctr,
            #"visitas_average":visitas_average
        }
    except Exception as e:
        print(f"An error occurred:{e}")
        raise HTTPException(status_code=500,detail="Internal Server Error")
    finally:
        if cur:
            cur.close()
        if conn:
            Database.return_connection(conn)