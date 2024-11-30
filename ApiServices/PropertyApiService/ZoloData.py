import requests
from pymongo import MongoClient
from datetime import datetime, timezone
from uuid import uuid4

def PropertyData():
    client = MongoClient("mongodb+srv://DineshG:1234@cluster0.flovz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = client["Cities_db"]
    collection = db["cities_collections"]
    db1 = client["Property_db"]
    collection1 = db1["properties_collections"]
    log_collection = db1["api_log"]
    
    current_date = datetime.now(timezone.utc)
    session_id = str(uuid4())

    session_log = {
        "session_id": session_id,
        "start_time": current_date,
        "site_name": "zolo",
        "total_properties_fetched": 0,
        "cities_processed": [],
        "status": "in_progress"
    }
    log_collection.insert_one(session_log)

    city_ids = [city.get("city_name") for city in collection.find({}, {"city_name": 1, "_id": 0})]
    url = "https://api.zolostays.com/api/v7/centers/search?"
    req_count = 0
    db_update_count = 0
    for city_id in city_ids:
        if not city_id: 
            print(f"Skipping invalid city_id: {city_id}")
            continue

        params = {
            "cityKey": city_id.lower(),
            "offset": "20",
            "limit": "100",
        }

        try:
            # API request
            response = requests.get(url, params=params)
            response.raise_for_status()
            req_count += 1 
            base_url = "https://play-zelo-production.s3.ap-south-1.amazonaws.com/uploads/amenities/"
            data = response.json()
            properties = data.get("result", [])[0].get("centers", [])

            log_collection.update_one(
                {"session_id": session_id},
                {
                    "$inc": {"total_properties_fetched": len(properties)},
                    "$push": {"cities_processed": {"city_name": city_id, "status": "success", "properties_fetched": len(properties)}}
                }
            )
            db_update_count += 1
            for property in properties:
                basic_data = property.get("basicData", {})
                amenities = [
                    {
                        "name": item["displayName"],
                        "imageUrl": f"{base_url}{item['imageName']}.png",
                    }
                    for item in basic_data.get("amenities", [])
                ]
                images = property.get("images", [])
                assets = [
                    {
                        "name": image.get("category", ""),
                        "imageUrl": image.get("url", "")
                    }
                    for image in images
                ]
                
                property_data = {
                    "property_id": basic_data.get("id"),
                    "Property_Name": basic_data.get("name", ""),
                    "City": basic_data.get("city"),
                    "Locality": basic_data.get("locality"),
                    "Allowed_Gender": basic_data.get("gender"),
                    "Property_type": basic_data.get("propertyCategory"),
                    "Pricing_Plan": "Monthly",
                    "Current_Price": basic_data.get("minRent"),
                    "Location_coordinates": {
                        "latitude": basic_data.get("location", [])[0] if len(basic_data.get("location", [])) > 0 else None,
                        "longitude": basic_data.get("location", [])[1] if len(basic_data.get("location", [])) > 1 else None,
                    },
                    "Address": basic_data.get("addressLine1", "").strip(),
                    "Amitnies": amenities,  
                    "Assets": assets,
                    "added_date": current_date,
                    "updated_date": current_date,
                    "site_name": "zolo",
                     "contact":"+91-8884518010",
                    "owner_name":"zolo official",
                    "owner_email":'info@zolostays.com'

                }

                existing_property = collection1.find_one({"property_id": property_data["property_id"]})

                if existing_property:
                    collection1.update_one(
                        {"property_id": property_data["property_id"]},
                        {"$set": property_data},
                    )
                    print(f"Property {property_data['property_id']} updated in MongoDB.")
                else:
                    collection1.insert_one(property_data)
                    print(f"Property {property_data['property_id']} inserted into MongoDB.")

        except requests.exceptions.RequestException as e:
            log_collection.update_one(
                {"session_id": session_id},
                {"$push": {"cities_processed": {"city_name": city_id, "status": f"API error: {e}"}}}
            )
            print(f"API error for city {city_id}: {e}")

        except Exception as e:
            log_collection.update_one(
                {"session_id": session_id},
                {"$push": {"cities_processed": {"city_name": city_id, "status": f"General error: {e}"}}}
            )
            print(f"General error for city {city_id}: {e}")

    # Finalize session log
    log_collection.update_one(
        {"session_id": session_id},
        {"$set": {"status": "completed", "end_time": datetime.now(timezone.utc)}}
    )
    print("DB count", db_update_count)
    print("req count", req_count)

PropertyData()
