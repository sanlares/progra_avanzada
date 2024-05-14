# routers/recommendations.py
from fastapi import APIRouter
from ..database import Database

router = APIRouter()

@router.get("/{advertiser_id}/{Modelo}")
def recommendations(advertiser_id: str, Modelo: str):
    # tu código aquí