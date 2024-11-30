import requests
from pymongo import MongoClient

def fetch_and_save_states():
    mongo_uri = "mongodb+srv://DineshG:1234@cluster0.flovz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    db_name = "Location_Data"

    api_key = 'UTFsQnY3NXJlV0tkTzljUW1KTjFHNGlRT2lKY0ZZR1NQVzJsZmN6VQ=='
    url = "https://api.countrystatecity.in/v1/countries/IN/states/"
    headers = {
        'X-CSCAPI-KEY': api_key
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch data: {response.status_code}")
        return
    states_data = response.json()

    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        state_collection = db['States']

        state_collection.create_index("id", unique=True)

        for state in states_data:
            state_id = state.get('id')
            state_name = state.get('name')
            state_iso2 = state.get('iso2')

            exists = state_collection.find_one({"id": state_id})

            if not exists:
                state_collection.insert_one({
                    "id": state_id,
                    "name": state_name,
                    "iso2": state_iso2
                })
                print(f"State {state_name} added to the database.")

                FetchCities(state_iso2)
            else:
                print(f"State {state_name} already exists in the database.")

        print("Data fetched and saved to the database.")

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        client.close()


def FetchCities():
    mongo_uri = "mongodb+srv://DineshG:1234@cluster0.flovz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"   
    db_name = "Location_Data"
    api_key = 'UTFsQnY3NXJlV0tkTzljUW1KTjFHNGlRT2lKY0ZZR1NQVzJsZmN6VQ=='
    url = f"https://api.countrystatecity.in/v1/countries/IN/cities"
    headers = {
        'X-CSCAPI-KEY': api_key
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch cities for state {StateIso2}: {response.status_code}")
        return
    cities_data = response.json()
    print(cities_data)
    
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        cities_collection = db['Cities']

        cities_collection.create_index("id", unique=True)

        for city in cities_data:
            city_id = city.get('id')
            city_name = city.get('name')
            city_latitude = city.get('latitude')
            city_longitude = city.get('longitude')

            exists = cities_collection.find_one({"id": city_id})

            if not exists:
                cities_collection.insert_one({
                    "id": city_id,
                    "city_name": city_name,
                    "city_latitude": city_latitude,
                    "city_longitude": city_longitude,
                    "state_code": StateIso2
                })
                print(f"City {city_name} added to the database.")
            else:
                print(f"City {city_name} already exists in the database.")

        print(f"Cities for state {StateIso2} fetched and saved to the database.")

    except Exception as e:
        print(f"An error occurred while fetching cities for {StateIso2}: {e}")
    
    finally:
        client.close()


# fetch_and_save_states()
FetchCities()
