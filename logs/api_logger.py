from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


class APILogger:
    def __init__(self):
        database_url = os.getenv("MONGO_URL")
        client = MongoClient(database_url)
        db = client["Property_db"]
        self.log_collection = db["google_review_api_log"]

    def log_request(
        self,
        property_name,
        request_data,
        response_data,
        status_code,
        error_message=None,
    ):
        log_entry = {
            "property_name": property_name,
            "request_data": request_data,
            "response_data": response_data,
            "status_code": status_code,
            "error_message": error_message,
            "timestamp": datetime.utcnow(),
        }
        self.log_collection.insert_one(log_entry)
        print(f"API request logged for {property_name}")
