from fastapi import FastAPI
from routers import recommendations, stats, history
from dotenv import load_dotenv
from database import Database

app = FastAPI()

load_dotenv()

Database.initialize()

app.include_router(recommendations.router)
app.include_router(stats.router)
app.include_router(history.router)

        
        
 
