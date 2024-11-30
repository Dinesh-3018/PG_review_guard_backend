import requests
from pymongo import MongoClient
import base64
import json
from datetime import datetime, timezone

client = MongoClient(
    "mongodb+srv://DineshG:1234@cluster0.flovz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)


def PropertyData():
    db = client["Cities_db"]
    collection = db["cities_collections"]
    db1 = client["Property_db"]
    collection1 = db1["properties_collections"]
    current_date = datetime.now(timezone.utc)
    req_count = 0
    db_count = 0
    city_data = collection.find({}, {"localities": 1, "_id": 0})
    property_data_db = collection.find({}, {"localities": 1, "_id": 0})

    for city in city_data:
        localities = city.get("localities", [])
        for locality in localities:
            headers = {
                "Accept": "*/*",
                "Host": "www.nobroker.in",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Referer": "https://www.nobroker.in/pg-in-adyar_chennai",
                "X-Tenant-Id": "NOBROKER",
                "sentry-trace": "e65444aa34ba4e18aa1b03af0c1e8826-a2856e97c57a3901",
                "baggage": "sentry-environment=production,sentry-release=02102023,sentry-public_key=826f347c1aa641b6a323678bf8f6290b,sentry-trace_id=e65444aa34ba4e18aa1b03af0c1e8826",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "Connection": "keep-alive",
                "Cookie": (
                    "nbSource=direct; nbMedium=direct; nbCampaign=direct; nbDevice=desktop; "
                    "mbTrackID=3fd4029d2fed4851a5ebfdaf965d0f1f; __zlcmid=1NUmynU0iXVYnUt; "
                    "nbcit=ZmY4MDgxODE1NTc4NjVkNzAxNTU3YzUxMzIyNjFkMTJmZmJkfGFjZDIwMTYtMDYtMjMgMTM6NDI6MjMuMA==; "
                    "nbFr=list-pg; cloudfront-viewer-address=49.249.121.122%3A40856; cloudfront-viewer-country=IN; "
                    "cloudfront-viewer-latitude=12.89960; cloudfront-viewer-longitude=80.22090; headerFalse=false; "
                    "isMobile=false; deviceType=web; js_enabled=true; nbcr=chennai; nbpt=PG; dummy=foo; "
                    "nbccc=51505853d00741c99fbc00fb4cc62298"
                ),
            }
            city_for_id = locality.get("city_Key")
            locality_for_city = locality.get("locality_Key")
            hit_url = f"https://www.nobroker.in/places/api/v1/autocomplete?hint={locality_for_city}&city={city_for_id}&params=location"
            response = requests.get(hit_url, headers=headers)
            data = response.json()
            resluts = data.get("predictions")
            place_id = resluts[0].get("placeId")

            loca_data = [{"placeId": place_id, "showMap": False}]

            json_string = json.dumps(loca_data)

            base_64 = base64.b64encode(json_string.encode("utf-8")).decode("utf-8")
            hit_url_2 = f"https://www.nobroker.in/api/v3/multi/property/PG/filter/nearby?searchParam={base_64}&city={city_for_id}"
            headers_for_url_2 = {
                "city": city_for_id,
                "HTTP_version": "HTTP/1.1",
                "Host": "www.nobroker.in",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Referer": "https://www.nobroker.in/property/pg/chennai/Adyar/?searchParam=W3sibGF0IjoxMy4wMDExNzc0LCJsb24iOjgwLjI1NjQ5NTcsInBsYWNlSWQiOiJDaElKZ1JiRUZlMW5Vam9SZzU0a2VwYk9hV1UiLCJwbGFjZU5hbWUiOiJBZHlhciIsInNob3dNYXAiOmZhbHNlfV0=&gender=FEMALE",
                "Access-Control-Allow-Origin": "*",
                "X-User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
                "canonicalUrl": "pg-in-adyar_chennai",
                "userid": "8a9f8c838e287714018e28fdece75452",
                "sentry-trace": "de55aee96deb44bf888c7a8022420ac4-9de9374447e182e2",
                "baggage": "sentry-environment=production,sentry-release=02102023,sentry-public_key=826f347c1aa641b6a323678bf8f6290b,sentry-trace_id=de55aee96deb44bf888c7a8022420ac4",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "Connection": "keep-alive",
                "Cookie": "nbSource=direct; nbMedium=direct; nbCampaign=direct; nbDevice=desktop; mbTrackID=3fd4029d2fed4851a5ebfdaf965d0f1f; __zlcmid=1NUmynU0iXVYnUt; SPRING_SECURITY_REMEMBER_ME_COOKIE=RU5lUzF0U00rSUE0TExaOEtwZUxxZz09OjlBQlN0elJMQlBXVkZqWlBNQUVHcnc9PQ; _ud_basic=true; _ud_check=true; _ud_login=true; _last_cp=8a9fb0838bb4759c018bb49ee83216f9; nbcit=OGE5ZjhjODM4ZTI4NzcxNDAxOGUyOGZkZWNlNzU0NTJmZmJkfGFjZDIwMjQtMDMtMTAgMjE6MDE6NTQuMA==; nbccc=b3989c2f10234a809a1578b580868c0b; loggedInUserStatus=existing; nbFr=list-pg; cloudfront-viewer-address=49.249.121.122%3A34404; cloudfront-viewer-country=IN; cloudfront-viewer-latitude=12.89960; cloudfront-viewer-longitude=80.22090; headerFalse=false; isMobile=false; deviceType=web; js_enabled=true; userInfoCalled=true; nbcr=chennai; nbpt=PG; dummy=foo; nb_swagger=%7B%22list_page_sorting%22%3A%22default%22%7D; _f_au=eyJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJodHRwczovL2lkZW50aXR5dG9vbGtpdC5nb29nbGVhcGlzLmNvbS9nb29nbGUuaWRlbnRpdHkuaWRlbnRpdHl0b29sa2l0LnYxLklkZW50aXR5VG9vbGtpdCIsImV4cCI6MTczMjA4NjcwMywiaWF0IjoxNzMyMDgzMTAzLCJpc3MiOiJub2Jyb2tlci1maXJlYmFzZUBuby1icm9rZXIuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCJzdWIiOiJub2Jyb2tlci1maXJlYmFzZUBuby1icm9rZXIuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCJ1aWQiOiI4YTlmOGM4MzhlMjg3NzE0MDE4ZTI4ZmRlY2U3NTQ1MiJ9.UFnTRUX1qLTxR3xlgl703PKItj-2-PY2MIaU6GKjBzTc2ybiHVK6nhPykr9tbpcSjUxet4BCOugJSShsLYFYpvy10Wxj3ae4nQj-n8olZv1lGNnWUM58Mp4ZeBC1xCW5LrOEZaQIRxclLQiQwPzrDne4otW27dTwhV8y_YnloqeESbtx-10zs2s5OwEz7gYbdRp8VDs-QCGpvSTQcFZkQmTDE8v0hIUvY1nlMbB-SKaBeoood_4xuRkEcg0ogkg1UZRhge6-GN1SJ26VSq6EOVtIFjBjzCnVJaeBNBwEZ6E1r5p_YW1npuoDWHUXo9Cqp7y_UhwfbdczD_oe2oPP_w; JSESSION=5ecf183e-a66f-419a-8aa8-abdf1a05c69d",
            }

            response = requests.get(hit_url_2, headers=headers_for_url_2)
            data = response.json()
            final_data = data.get("data", [])
            print(type(final_data))
            for property in final_data:
                if property is not None:

                    owner_id = property.get("ownerId", [])
                    request_data = {
                        "Host": "www.nobroker.in",
                        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
                        "Accept": "application/json",
                        "Accept-Language": "en-US,en;q=0.5",
                        "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Referer": "https://www.nobroker.in/pg-in-adyar_chennai",
                        "X-User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
                        "sentry-trace": "9d82c904c23840419546e360ef1be92d-a13f569fafa43bad",
                        "baggage": "sentry-environment=production,sentry-release=02102023,sentry-public_key=826f347c1aa641b6a323678bf8f6290b,sentry-trace_id=9d82c904c23840419546e360ef1be92d",
                        "Sec-Fetch-Dest": "empty",
                        "Sec-Fetch-Mode": "cors",
                        "Sec-Fetch-Site": "same-origin",
                        "Connection": "keep-alive",
                        "Cookie": "nbSource=direct; nbMedium=direct; nbCampaign=direct; nbDevice=desktop; mbTrackID=3fd4029d2fed4851a5ebfdaf965d0f1f; __zlcmid=1NUmynU0iXVYnUt; nbcit=OGE5ZjhjODM4ZTI4NzcxNDAxOGUyOGZkZWNlNzU0NTJmZmJkfGFjZDIwMjQtMDMtMTAgMjE6MDE6NTQuMA==; nbFr=list-pg; SPRING_SECURITY_REMEMBER_ME_COOKIE=RU5lUzF0U00rSUE0TExaOEtwZUxxZz09Om5oTThkeXYwSk0zK2p2ZVhBdWx5TVE9PQ; loggedInUserStatus=existing; _ud_basic=true; _ud_check=true; _ud_login=true; _last_cp=8a9f90438ff039fd018ff09c9c270984; cloudfront-viewer-address=49.249.121.122%3A59150; cloudfront-viewer-country=IN; cloudfront-viewer-latitude=12.89960; cloudfront-viewer-longitude=80.22090; headerFalse=false; isMobile=false; deviceType=web; js_enabled=true; userInfoCalled=true; nbcr=chennai; nbpt=PG; dummy=foo; nbccc=b9d7e322bbfa4a79893bd548c2daf9fb; _f_au=eyJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJodHRwczovL2lkZW50aXR5dG9vbGtpdC5nb29nbGVhcGlzLmNvbS9nb29nbGUuaWRlbnRpdHkuaWRlbnRpdHl0b29sa2l0LnYxLklkZW50aXR5VG9vbGtpdCIsImV4cCI6MTcyNTUyMDMxNywiaWF0IjoxNzI1NTE2NzE3LCJpc3MiOiJub2Jyb2tlci1maXJlYmFzZUBuby1icm9rZXIuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCJzdWIiOiJub2Jyb2tlci1maXJlYmFzZUBuby1icm9rZXIuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCJ1aWQiOiI4YTlmOGM4MzhlMjg3NzE0MDE4ZTI4ZmRlY2U3NTQ1MiJ9.HDEkY6v3kjcPDmZVWt6gYcDtkS1ENO8ROzOepaWAisarpDADRA7ZupvApaBZZidUQ4gkNJb_utl9A3qcsGpDwli5GkVrLDwkRDhTwWPQFjZotb9l-HzwxE6rINt-VtMhjcpLLDtwdmAc8FRNO6Sr7nka8eFskFXdw5_X_gF4GYKk3n-expVPI5C_BGzMRRkqzNcRkNFEUV2hRPbifFt12IYLGEm5WK3821Xrow6OYKMrhIrfNKuMJz2HXsfCtySXd1d630xwBQQz11Pjhuck_qGOnL58hhJ0QEKrKb0EROHZjarh5K-qtOC9c9HaZVXKoBuWhtMN9Cz43_ZDKAzZHw; JSESSION=961074ec-6ecf-4a85-8610-e2ff287d3cdf",
                    }
                    hit_url_3_for_owner = (
                        f"https://www.nobroker.in/api/v3/user/contacted/{owner_id}"
                    )
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

                    transformed_amenities = [
                        {
                            "imagename": default_mapping.get(
                                key,
                                {
                                    "imagename": key.capitalize(),
                                    "imageurl": "https://assets.nobroker.in/default-image.png",
                                },
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
                            "thumbnailImage": f"https://assets.nobroker.in/images/{property.get("id")}/{photo['imagesMap']['medium']}",
                        }
                        for photo in photos
                    ]

                    res_owner_data = requests.get(
                        hit_url_3_for_owner, headers=request_data
                    )
                    owner_data = res_owner_data.json()
                    owner_contact_data = owner_data.get("data")
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
                        "contact": owner_contact_data.get("owner_phone"),
                        "owner_name": owner_contact_data.get("owner_name"),
                        "owner_email": owner_contact_data.get("owner_email"),
                    }
                    collection1.update_one(
                        {"property_id": property_data["property_id"]},
                        {"$set": property_data},
                        upsert=True,
                    )
                    print(f"update on Db with {property.get("city")},")
                break


PropertyData()
