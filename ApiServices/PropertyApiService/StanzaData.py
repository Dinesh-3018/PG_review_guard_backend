import requests
from pymongo import *
import pymongo
from datetime import datetime ,timezone

client = MongoClient("mongodb+srv://DineshG:1234@cluster0.flovz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")


def PropertyData():
    db = client["Cities_db"]
    collection = db["cities_collections"]
    db1 = client["Property_db"]  
    collection1= db1["properties_collections"]  
    city_ids = [city.get("city_id") for city in collection.find({}, {"city_id": 1, "_id": 0})]
    url = "https://www.stanzaliving.com/api/residence/search?"
    req_count = 0
    db_count = 0
    current_date = datetime.now(timezone.utc)

    for city_id in city_ids:
        params = {
            "city": str(city_id),  
            "pageNo": "1",
            "pageSize": "100"
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status() 
            req_count += 1
            data = response.json()
            properties = data.get("residenceResponseShortDTOs", [])

            for property in properties:
                amenities_facility = property.get("facilities", [])
                amenities_features = property.get("features", [])
                
                formatted_result_for_amenity_1 = [
                    {"name": facility['name'], "image_url": facility['iconImageUrl']}
                    for facility in amenities_facility
                ]
                formatted_result_for_amenity_2 = [
                    {"name": facility['name'], "image_url": facility['iconImageUrl']}
                    for facility in amenities_features
                ]
                amenities = formatted_result_for_amenity_1 + formatted_result_for_amenity_2

                image_on_api = property.get("images", [])
                image_for_db = [
                    {"name": imageData['imageTag'], "image_url": imageData['imageUrl']}
                    for imageData in image_on_api
                ]

                property_data = {
                    "property_id": property.get("residenceId"),
                    "Property_Name": property.get("name"),
                    "City": property.get("cityName"),
                    "Locality": property.get("micromarketName"),
                    "Allowed_Gender": property.get("genderName"),
                    "Property_type": property.get("propertyEntityType"),
                    "Pricing_Plan": property.get("pricingPlan"),
                    "Current_Price": property.get("startingPrice"),
                    "Location_coordinates": {
                        "latitude": property.get("latitude"),
                        "longitude": property.get("longitude"),
                    },
                    "Address": (
                        property.get("addressResponseDTO", {}).get("line1", "") + " " +
                        property.get("addressResponseDTO", {}).get("line2", "")
                    ).strip(),
                    "Amitnies": amenities,
                    "Asserts": image_for_db,
                      "added_date": current_date,
                    "updated_date": current_date,
                    "site_name":"stanza",
                    "contact":"08046007672",
                    "owner_name":"stanza official",
                    "owner_email":"digital@stanzaliving.com"
                }

                collection1.update_one(
                    {"property_id": property_data["property_id"]},  
                    {"$set": property_data},  
                    upsert=True  
                )
                db_count += 1

            print(f"Data successfully inserted/updated for city {city_id} in MongoDB.")
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while making the API request for city {city_id}: {e}")
        except Exception as e:
            print(f"An error occurred for city {city_id}: {e}")
    print("req count", req_count)
    print("DB count", db_count)
def Testing():
    client = pymongo.MongoClient(
        "mongodb+srv://DineshG:1234@cluster0.flovz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    )
    db = client["Cities_db"]
    collection = db["cities_collections"]

    city_id = [city.get("city_id") for city in collection.find({}, {"city_id": 1, "_id": 0})]
    print(city_id)
PropertyData()
# Testing() 
