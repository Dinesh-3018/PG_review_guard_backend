import re
import requests
from pymongo import MongoClient
import base64
import json
from datetime import datetime, timezone
import os

client = MongoClient(os.getenv("MONGO_URL"))
config_db = client["Config_db"]
headers_cookies_collection = config_db["headers_cookies"]


def initialize_headers_cookies():
    if headers_cookies_collection.count_documents({}) > 0:
        print("Headers and cookies already exist in database")
        return

    config_data = {
        "main_headers": {
            "Accept": "*/*",
            "Host": "www.nobroker.in",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Referer": "https://www.nobroker.in/pg-in-adyar_chennai",
            "X-Tenant-Id": "NOBROKER",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Connection": "keep-alive",
        },
        "property_headers": {
            "Accept": "application/json",
            "Host": "www.nobroker.in",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Referer": "https://www.nobroker.in/property/pg/chennai/Adyar",
            "Access-Control-Allow-Origin": "*",
            "X-User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
            "canonicalUrl": "pg-in-adyar_chennai",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Connection": "keep-alive",
        },
        "owner_headers": {
            "Accept": "application/json",
            "Host": "www.nobroker.in",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Referer": "https://www.nobroker.in/pg-in-adyar_chennai",
            "X-User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Connection": "keep-alive",
        },
        "cookies": {
            "nbSource": "direct",
            "nbMedium": "direct",
            "nbCampaign": "direct",
            "nbDevice": "desktop",
            "mbTrackID": "3fd4029d2fed4851a5ebfdaf965d0f1f",
            "__zlcmid": "1NUmynU0iXVYnUt",
            "nbFr": "list-pg",
            "headerFalse": "false",
            "isMobile": "false",
            "deviceType": "web",
            "js_enabled": "true",
            "nbcit": "OGE5ZjhjODM4ZTI4NzcxNDAxOGUyOGZkZWNlNzU0NTJmZmJkfGFjZDIwMjQtMDMtMTAgMjE6MDE6NTQuMA==",
        },
    }

    headers_cookies_collection.insert_one(config_data)
    print("Headers and cookies initialized in database")


def get_headers(header_type):
    config = headers_cookies_collection.find_one({})
    if not config:
        initialize_headers_cookies()
        config = headers_cookies_collection.find_one({})

    return config.get(header_type, {})


def get_cookies():
    config = headers_cookies_collection.find_one({})
    if not config:
        initialize_headers_cookies()
        config = headers_cookies_collection.find_one({})

    return config.get("cookies", {})


def update_header(header_type, key, value):
    headers_cookies_collection.update_one({}, {"$set": {f"{header_type}.{key}": value}})
    print(f"Updated {header_type}.{key} to {value}")


def update_cookie(key, value):
    headers_cookies_collection.update_one({}, {"$set": {f"cookies.{key}": value}})
    print(f"Updated cookie {key} to {value}")


def format_cookie_string(cookies_dict):
    return "; ".join([f"{key}={value}" for key, value in cookies_dict.items()])


def get_headers_with_cookies(header_type):
    headers = get_headers(header_type)
    cookies = get_cookies()

    headers["Cookie"] = format_cookie_string(cookies)
    return headers


def test_headers_cookies():
    initialize_headers_cookies()

    headers = get_headers_with_cookies("main_headers")
    print("Headers with cookies:", headers)
    update_header("main_headers", "User-Agent", "New User Agent String")
    update_cookie("nbDevice", "mobile")
    updated_headers = get_headers_with_cookies("main_headers")
    print("Updated headers:", updated_headers)


# initialize_headers_cookies()
