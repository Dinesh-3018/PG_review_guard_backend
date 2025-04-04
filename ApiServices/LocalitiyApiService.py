import requests
import pymongo
import os


client = pymongo.MongoClient(os.getenv("MONGO_URL"))
db = client["Cities_db"]
collection = db["cities_collections"]


def StanzaDetails():

    url = "https://www.stanzaliving.com/api/cities?latitude=&longitude="
    headers = {
        "Host": "www.stanzaliving.com",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Referer": "https://www.stanzaliving.com/chennai",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Connection": "keep-alive",
        "Cookie": 'abTestingPlatformConfig={"listing_pg_city":{"default":[16]},"map":"G","details_pg_city":{"default":[16]}}; currentPageExp=default; stanza_build_id=1731330372500; residence-details-id={"residenceIds":[162],"propertyLocationIds":[],"entireFlatpropertyLocationIds":[]}',
    }

    response = requests.get(url, headers=headers)
    res = response.json()

    if res.get("status") and res.get("data"):
        for city in res["data"]:
            city_data = {
                "apartmentCitySlug": city.get("apartmentCitySlug"),
                "cityImgUrl": city.get("cityImgUrl"),
                "longitude": city.get("longitude"),
                "latitude": city.get("latitude"),
                "city_name": city.get("name"),
                "city_code": city.get("code"),
                "city_id": city.get("cityId"),
            }

            collection.insert_one(city_data)
            print(f"Inserted: {city_data}")
    else:
        print("Failed to retrieve data or data field is empty")


def ZoloStaysDetails():
    client = pymongo.MongoClient(os.getenv("MONGO_URL"))
    db = client["Cities_db"]
    collection = db["cities_collections"]

    city_names = [
        city["city_name"] for city in collection.find({}, {"city_name": 1, "_id": 0})
    ]

    url = "https://zolostays.com/api/sitemap"
    response = requests.get(url)
    zolo_data = response.json()

    if not zolo_data or "data" not in zolo_data:
        print("Failed to retrieve valid data from ZoloStays API")
        return

    cities_data = zolo_data["data"]

    for city_name, city_info in cities_data.items():
        db_city_name = "Bangalore" if city_name == "Bengaluru" else city_name

        if db_city_name in city_names:
            localities = city_info.get("locality", [])
            for locality in localities:
                if locality.get("cityName") == city_name:
                    locality_data = {
                        "city_Key": locality.get("cityKey"),
                        "locality_Name": locality.get("localityName"),
                        "locality_Key": locality.get("localityKey"),
                    }

                    existing_locality = collection.find_one(
                        {
                            "city_name": db_city_name,
                            "localities": {"$elemMatch": locality_data},
                        }
                    )
                    if not existing_locality:
                        collection.update_one(
                            {"city_name": db_city_name},
                            {"$push": {"localities": locality_data}},
                            upsert=True,
                        )

            properties = city_info.get("property", [])
            for property_item in properties:
                if property_item.get("cityName") == city_name:
                    property_data = {
                        "is_Exclusive": property_item.get("isExclusive"),
                        "locality_Name": property_item.get("localityName"),
                        "property_Name": property_item.get("propertyName"),
                        "property_Url": property_item.get("propertyUrl"),
                    }

                    collection.update_one(
                        {"city_name": db_city_name},
                        {"$push": {"properties": property_data}},
                        upsert=True,
                    )


ZoloStaysDetails()
# StanzaDetails()
