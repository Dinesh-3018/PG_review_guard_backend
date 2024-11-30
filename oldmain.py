from fastapi import FastAPI, Body, HTTPException
from pydantic import BaseModel
import base64
import json
import requests
from typing import List, Optional

app = FastAPI()

class LocationData(BaseModel):
    lat: Optional[float] = None
    lon: Optional[float] = None
    placeId: str
    placeName: Optional[str] = None
    showMap: bool

@app.post("/generate-base64")
async def generate_base64(data: List[LocationData] = Body(...)):
    json_string = json.dumps([location.dict() for location in data])
    base64_encoded = base64.b64encode(json_string.encode('utf-8')).decode('utf-8')

    return base64_encoded

@app.get("/google-reviews")
async def get_google_reviews(place_name: str):
    api_key = "28f70a1441055019d267fb1119675c668c4d63c5cf11c7235b0802d1aef42b38"  
    url = "https://serpapi.com/search"
    params = {
        "engine": "google_maps",
        "api_key": api_key,
        "q": place_name,
        "type": "search",
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status() 
        
        data = response.json()
        return data
    
    except requests.exceptions.HTTPError as http_err:
        raise HTTPException(status_code=response.status_code, detail=str(http_err))
    except Exception as err:
        raise HTTPException(status_code=500, detail=str(err))
