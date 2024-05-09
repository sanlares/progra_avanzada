from fastapi import FastAPI 
import numpy as np

app = FastAPI()

@app.get("/recommendations/{ADV}/{Modelo}") 
def recommendations():
    
    return f'Random number: {np.random.randint(min+1, max+1)}'

@app.get("/stats/") 
def stats(): 

    return f'Random number: {np.random.randint(min+1, max+1)}'

@app.get("/history/{ADV}") 
def history(): 

    return f'Random number: {np.random.randint(min+1, max+1)}'

