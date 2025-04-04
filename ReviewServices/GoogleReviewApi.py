import requests
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()


class GoogleReviewFetcher:
    def __init__(self):
        self.database_url = os.getenv("MONGO_URL")
        self.client = MongoClient(self.database_url)
        self.db = self.client["Property_db"]
        self.property_collection = self.db["properties_collections"]
        self.api_key = ""
        self.google_url = os.getenv("BASR_URL")

    def fetch_reviews(self, property_name_filter):
        property_data_db = self.property_collection.find_one(
            {"Property_Name": property_name_filter},
            {"Property_Name": 1, "Locality": 1, "City": 1, "_id": 1, "reviews": 1},
        )

        if not property_data_db:
            print(f"No property found with name: {property_name_filter}")
            return None, None, None, "No property found"

        property_id = property_data_db["_id"]

        if "reviews" in property_data_db:
            print(
                f"Property '{property_name_filter}' with ID {property_id} already has reviews. Skipping API call."
            )
            return None, None, None, "Reviews already exist"

        property_name = property_data_db.get("Property_Name")
        locality = property_data_db.get("Locality")
        city = property_data_db.get("City")

        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": "*",
        }

        data = {"textQuery": f"{property_name} {locality} {city}", "pageSize": 20}
        url = self.google_url

        try:
            response = requests.post(url, headers=headers, json=data)
            response_data = response.json()
            status_code = response.status_code

            places = response_data.get("places", [])
            if not places:
                print(f"No places found for {property_name}")
                return data, response_data, status_code, "No places found"

            reviews = places[0].get("reviews", [])
            review_details = [
                {
                    "starRating": review.get("rating", "No rating available"),
                    "displayName": review.get("authorAttribution", {}).get(
                        "displayName", "Anonymous"
                    ),
                    "photoUri": review.get("authorAttribution", {}).get(
                        "photoUri", "No photo available"
                    ),
                    "text": review.get("originalText", {}).get(
                        "text", "No text available"
                    ),
                    "relativePublishTime": review.get(
                        "relativePublishTimeDescription", "No time available"
                    ),
                    "ReviewUri": review.get("authorAttribution", {}).get(
                        "uri", "No URI available"
                    ),
                }
                for review in reviews
            ]

            self.property_collection.update_one(
                {"_id": property_id}, {"$set": {"reviews": review_details}}
            )
            print(f"Reviews fetched successfully for {property_name}")

            return data, response_data, status_code, None

        except requests.exceptions.RequestException as e:
            print(f"Error fetching reviews for {property_name}: {e}")
            return data, None, "Request Failed", str(e)
