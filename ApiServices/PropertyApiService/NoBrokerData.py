import re
import requests
from pymongo import MongoClient
import base64
import json
from datetime import datetime, timezone
import os

from config.header_cookie_manager import (
    initialize_headers_cookies,
    get_headers_with_cookies,
)

client = MongoClient(os.getenv("MONGO_URL"))


def property_data():
    initialize_headers_cookies()

    db = client["Cities_db"]
    collection = db["cities_collections"]
    db1 = client["Property_db"]
    collection1 = db1["properties_collections"]

    current_date = datetime.now(timezone.utc)
    req_count = 0
    db_count = 0

    city_data = collection.find({}, {"localities": 1, "_id": 0})

    default_mapping = {
        "AB": {
            "imagename": "Attached Bathroom",
            "imageurl": "https://assets.nobroker.in/nb-new/public/Property-Details/amenities.png",
            "position": "-12px -555px",
        },
        "TV": {
            "imagename": "Television",
            "imageurl": "https://assets.nobroker.in/nb-new/public/Property-Details/amenities.png",
            "position": "-12px -861px",
        },
        "AC": {
            "imagename": "Air Conditioner",
            "imageurl": "https://assets.nobroker.in/nb-new/public/Property-Details/amenities.png",
            "position": "-12px -1010px",
        },
        "CUPBOARD": {
            "imagename": "Cupboard",
            "imageurl": "https://assets.nobroker.in/nb-new/public/Property-Details/amenities.png",
            "position": "-12px -410px",
        },
        "BEDDING": {
            "imagename": "Bedding",
            "imageurl": "https://assets.nobroker.in/nb-new/public/Property-Details/amenities.png",
            "position": "-12px -712px",
        },
        "GEASER": {
            "imagename": "Geyser",
            "imageurl": "https://assets.nobroker.in/nb-new/public/Property-Details/amenities.png",
            "position": "-12px -961px",
        },
        "ROOM_CLEANING": {
            "imagename": "Room Cleaning",
            "imageurl": "https://assets.nobroker.in/nb-new/public/Property-Details/amenities.png",
            "position": "-12px -712px",
        },
        "COOKING": {
            "imagename": "Cooking",
            "imageurl": "https://assets.nobroker.in/nb-new/public/Property-Details/amenities.png",
            "position": "-12px -400px",
        },
        "WIFI": {
            "imagename": "Wifi",
            "imageurl": "https://assets.nobroker.in/nb-new/public/Property-Details/amenities.png",
            "position": "-12px -555px",
        },
    }

    for city in city_data:
        localities = city.get("localities", [])
        for locality in localities:
            headers = get_headers_with_cookies("main_headers")

            city_for_id = locality.get("city_Key")
            locality_for_city = locality.get("locality_Key")

            hit_url = f"https://www.nobroker.in/places/api/v1/autocomplete?hint={locality_for_city}&city={city_for_id}&params=location"
            response = requests.get(hit_url, headers=headers)
            data = response.json()
            results = data.get("predictions")

            if isinstance(results, list) and results:
                place_id = results[0].get("placeId")
            else:
                place_id = None
                print(f"No place ID found for {locality_for_city} in {city_for_id}")
                continue

            loca_data = [{"placeId": place_id, "showMap": False}]
            json_string = json.dumps(loca_data)
            base_64 = base64.b64encode(json_string.encode("utf-8")).decode("utf-8")

            hit_url_2 = f"https://www.nobroker.in/api/v3/multi/property/PG/filter/nearby?searchParam={base_64}&city={city_for_id}"

            headers_for_url_2 = get_headers_with_cookies("property_headers")

            response = requests.get(hit_url_2, headers=headers_for_url_2)
            data = response.json()
            final_data = data.get("data", [])

            for property in final_data:
                if property is not None:
                    detail_url = property.get("detailUrl")
                    match = re.search(r"[a-f0-9]{32}", detail_url)
                    final_owner_id = match.group(0) if match else None
                    print(f"Found property with ID: {final_owner_id}")

                    transformed_amenities = [
                        {
                            "imagename": default_mapping.get(
                                key, {"imagename": key.capitalize()}
                            )["imagename"],
                            "imageurl": default_mapping.get(
                                key,
                                {
                                    "imageurl": "https://assets.nobroker.in/default-image.png"
                                },
                            )["imageurl"],
                            "position": default_mapping.get(key, {}).get(
                                "position", "0px 0px"
                            ),
                        }
                        for key in default_mapping
                    ]

                    photos = property.get("photos", [])
                    updated_photos = [
                        {
                            "name": photo["name"],
                            "thumbnailImage": f"https://assets.nobroker.in/images/{property.get('id')}/{photo['imagesMap']['medium']}",
                        }
                        for photo in photos
                    ]

                    property_data = {
                        "property_id": property.get("id"),
                        "Property_Name": property.get("propertyTitle"),
                        "City": property.get("city"),
                        "Locality": property.get("locality"),
                        "Allowed_Gender": property.get("gender"),
                        "Property_type": property.get("propertyType"),
                        "Pricing_Plan": "Monthly",
                        "Current_Price": property.get("minRent"),
                        "Location_coordinates": {
                            "latitude": property.get("latitude"),
                            "longitude": property.get("longitude"),
                        },
                        "Address": property.get("address"),
                        "Amitnies": transformed_amenities,
                        "Asserts": updated_photos,
                        "added_date": current_date,
                        "updated_date": current_date,
                        "site_name": "no_broker",
                        "contact": "No Contact Found",
                        "owner_name": "No OwnerName Found",
                        "owner_email": "No Email Found",
                    }

                    # Save to database
                    collection1.update_one(
                        {"property_id": property_data["property_id"]},
                        {"$set": property_data},
                        upsert=True,
                    )

                    print(f"Updated DB with property in {property.get('city')}")
                    db_count += 1

                req_count += 1

            break

    print(f"Total API requests: {req_count}, Total DB updates: {db_count}")


if __name__ == "__main__":
    property_data()
