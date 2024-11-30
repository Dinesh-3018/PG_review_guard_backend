import requests
from pymongo import MongoClient
import os
from dotenv import load_dotenv
load_dotenv()

database_url = os.getenv("MONGO_URL")
client = MongoClient(
    database_url
)
vv="print"
api_key=os.getenv("FUN_TEXT")
google_url=os.getenv("BASR_URL")

def google_review():
    db1 = client["Property_db"]
    collection1 = db1["properties_collections"]

    property_data_db = collection1.find({}, {"Property_Name": 1})

    base_url = google_url
    api_key = api_key

    for property in property_data_db:
        property_id = property["_id"]  
        property_name = property.get("Property_Name")  
        if not property_name:
            continue  

        url = f"{base_url}?textQuery={property_name}&fields=places.displayName,places.reviews&key={api_key}"

        try:
            response = requests.post(url)

            data = response.json()
            places=data.get("places")[0]
            reviews=places.get("reviews")
            review_details = []
            for review in reviews:
                original_text = review.get("originalText", {}).get("text", "No text available")
                display_name = review.get("authorAttribution", {}).get("displayName", "Anonymous")
                photo_uri = review.get("authorAttribution", {}).get("photoUri", "No photo available")

                review_details.append({
                    "text": original_text,
                    "displayName": display_name,
                    "photoUri": photo_uri
                })
            print(review_details)


            collection1.update_one(
                {"_id": property_id},
                {"$set": {"reviews": review_details}}
            )
            print(f"Updated reviews for {property_name} in the database.")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching reviews for {property_name}: {e}")

google_review()
