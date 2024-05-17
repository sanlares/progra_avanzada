from fastapi import APIRouter
from database import Database
from datetime import datetime
from fastapi import HTTPException
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


    try:    
        return{         
            "top5_ctr":results
        }
    except Exception as e:
        print(f"An error occurred:{e}")
        raise HTTPException(status_code=500,detail="Internal Server Error")
    finally:
        if cur:
            cur.close()
        if conn:
            Database.return_connection(conn)