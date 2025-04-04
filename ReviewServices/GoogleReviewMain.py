from pymongo import MongoClient
import os
from dotenv import load_dotenv
from GoogleReviewApi import GoogleReviewFetcher
from logs.api_logger import APILogger

load_dotenv()


class PropertyFetcher:
    def __init__(self):
        database_url = os.getenv("MONGO_URL")
        self.client = MongoClient(database_url)
        self.db = self.client["Property_db"]
        self.property_collection = self.db["properties_collections"]
        self.review_fetcher = GoogleReviewFetcher()
        self.logger = APILogger()

    def fetch_all_properties(self):
        properties = self.property_collection.find({}, {"Property_Name": 1, "_id": 0})

        for property_entry in properties:
            property_name = property_entry.get("Property_Name")
            if property_name:
                print(f"Fetching reviews for: {property_name}")
                request_data, response_data, status_code, error_message = (
                    self.review_fetcher.fetch_reviews(property_name)
                )

                if request_data:
                    self.logger.log_request(
                        property_name,
                        request_data,
                        response_data,
                        status_code,
                        error_message,
                    )


if __name__ == "__main__":
    fetcher = PropertyFetcher()
    fetcher.fetch_all_properties()
