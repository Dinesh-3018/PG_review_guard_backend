from fastapi import FastAPI, HTTPException, Query, Depends
from pymongo import MongoClient
from bson import ObjectId
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr
from typing import Optional, List
import os
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],  
)
load_dotenv()
database_url = os.getenv("MONGO_URL")
client = MongoClient(database_url)
db = client["Property_db"]
property_collection = db["properties_collections"]
user_collection = db["users"]

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class Amenity(BaseModel):
    name: str
    imageUrl: str
    description: Optional[str] = None

class Asset(BaseModel):
    name: str
    imageUrl: str
    description: Optional[str] = None
class Review(BaseModel):
    user: str
    comment: str

class Property(BaseModel):
    id: str
    property_name: str
    city: str
    locality: str
    allowed_gender: str
    property_type: str
    pricing_plan: str
    amenities: Optional[List[Amenity]]
    assets: Optional[List[Asset]]
    site_name: str
    current_price: float
    reviews: Optional[List[Review]] = []  # Add reviews
    address: str  # Add address field

    class Config:
        orm_mode = True

class UserSignup(BaseModel):
    email: EmailStr
    password: str

class UserLogin(BaseModel):
    email: EmailStr
    password: str

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

@app.get("/properties", response_model=List[Property])
def get_properties(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1)):
    skip = (page - 1) * page_size
    cursor = property_collection.find().skip(skip).limit(page_size)
    properties = []
    
    for prop in cursor:
        try:
            property_item = Property(
                id=str(prop["_id"]),
                property_name=prop.get("Property_Name", ""),
                city=prop.get("City", ""),
                locality=prop.get("Locality", ""),
                allowed_gender=prop.get("Allowed_Gender", ""),
                property_type=prop.get("Property_type", ""),
                pricing_plan=prop.get("Pricing_Plan", ""),
                amenities=[
                    Amenity(
                        name=amenity.get("name", "") or "", 
                        imageUrl=amenity.get("imageUrl", "") or "",  
                        description=amenity.get("description", "")
                    ) for amenity in prop.get("Amitnies", [])
                ],
                assets=[
                    Asset(
                        name=asset.get("name", "") or "",  
                        imageUrl=asset.get("imageUrl", "") or "", 
                        description=asset.get("description", "")
                    ) for asset in prop.get("Assets", [])
                ],
                site_name=prop.get("site_name", ""),
                current_price=prop.get("Current_Price", 0.0) ,
                reviews=prop.get("Reviews", []),  # Include reviews
                address=prop.get("Address", "") 
            )
            properties.append(property_item)
        except Exception as e:
            print(f"Error processing property {prop}: {e}")

    return properties

@app.get("/properties/{property_id}", response_model=Property)
def get_property_details(property_id: str):
  
    try:
        property_data = property_collection.find_one({"_id": ObjectId(property_id)})

        if not property_data:
            raise HTTPException(status_code=404, detail="Property not found")

        property_details = Property(
            id=str(property_data["_id"]),
            property_name=property_data["Property_Name"],
            city=property_data["City"],
            locality=property_data["Locality"],
            allowed_gender=property_data["Allowed_Gender"],
            property_type=property_data["Property_type"],
            pricing_plan=property_data["Pricing_Plan"],
            amenities=[
                Amenity(
                    name=amenity.get("name", ""),
                    imageUrl=amenity.get("imageUrl", ""),
                    description=amenity.get("description", "")
                ) for amenity in property_data.get("Amitnies", [])
            ],
            assets=[
                Asset(
                    name=asset.get("name", ""),
                    imageUrl=asset.get("imageUrl", ""),
                    description=asset.get("description", "")
                ) for asset in property_data.get("Assets", [])
            ],
            site_name=property_data["site_name"],
            current_price=property_data["Current_Price"],
            reviews=property_data.get("Reviews", []),  # Include reviews
            address=property_data.get("Address", "") 
        )

        return property_details

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/properties/search/city", response_model=List[dict])
async def search_properties(
    city: str, 
    page: int = Query(1, gt=0), 
    page_size: int = Query(10, gt=0)
):
    try:
        skip = (page - 1) * page_size

        # Query the database
        results = list(
            property_collection.find(
                {"City": {"$regex": city, "$options": "i"}},  
                {"_id": 0, "Property_Name": 1, "City": 1, "Address": 1,"Locality":1,"Allowed_Gender":1,"Property_type":1,"Pricing_Plan":1,"Current_Price":1,"Amitnies":1,"Assets":1,"site_name":1,"Reviews": 1,} 
            )
            .skip(skip)
            .limit(page_size)
        )

        if not results:
            raise HTTPException(status_code=404, detail="No properties found in the specified city.")

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/properties/search", response_model=List[Property])
def search_properties(
    query: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1),
):
    skip = (page - 1) * page_size
    cursor = property_collection.find(
        {"Property_Name": {"$regex": query, "$options": "i"}}
    ).skip(skip).limit(page_size)
    
    properties = [
        Property(
            id=str(prop["_id"]),
            property_name=prop["Property_Name"],
            city=prop["City"],
            locality=prop["Locality"],
            allowed_gender=prop["Allowed_Gender"],
            property_type=prop["Property_type"],
            pricing_plan=prop["Pricing_Plan"],
            amenities=[Amenity(
                name=amenity.get("name", ""),
                imageUrl=amenity.get("imageUrl", ""),
                description=amenity.get("description", "")
            ) for amenity in prop.get("Amitnies", [])],
            assets=[Asset(
                name=asset.get("name", ""),
                imageUrl=asset.get("imageUrl", ""),
                description=asset.get("description", "")
            ) for asset in prop.get("Assets", [])],
            site_name=prop["site_name"],
            current_price=prop["Current_Price"],
            reviews=prop.get("Reviews", []),  
            address=prop.get("Address", "") 
        )
        for prop in cursor
    ]
    
    return properties

@app.post("/auth/signup")
def signup(user: UserSignup):
    if user_collection.find_one({"email": user.email}):
        raise HTTPException(status_code=400, detail="Email already registered")
    hashed_password = hash_password(user.password)
    user_collection.insert_one({"email": user.email, "password": hashed_password})
    return {"message": "User signed up successfully"}

@app.post("/auth/login")
def login(user: UserLogin):
    db_user = user_collection.find_one({"email": user.email})
    if not db_user or not verify_password(user.password, db_user["password"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return {"message": "Login successful"}
