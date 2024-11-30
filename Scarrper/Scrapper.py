from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from pymongo import MongoClient
import uuid

def run(playwright):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    mongo_uri = "mongodb+srv://DineshG:1234@cluster0.flovz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    db_name = "Location_Data"
    client = MongoClient(mongo_uri)
    db = client[db_name]
    state_collection = db['States']
    Districts_collection=db['Districts']
    Districts_collection.create_index("id", unique=True)

    states_and_uts = {
        "Andhra Pradesh",
        "Arunachal Pradesh",
        "Bihar",
        "Chhattisgarh",
        "Goa",
        "Gujarat",
        "Haryana",
        "Himachal Pradesh",
        "Jharkhand",
        "Karnataka",
        "Kerala",
        "Assam",
        "Madhya Pradesh",
        "Maharashtra",
        "Manipur",
        "Meghalaya",
        "Mizoram",
        "Nagaland",
        "Odisha",
        "Punjab",
        "Rajasthan",
        "Sikkim",
        "Tamil Nadu",
        "Telangana",
        "Tripura",
        "Uttarakhand",
        "Uttar Pradesh",
        "West Bengal",
        "Andaman and Nicobar Island",
        "Chandigarh",
        "Dadra and Nagar Haveli and Daman and Diu",
        "Delhi",
        "Jammu and Kashmir",
        "Ladakh",
        "Lakshadweep",
        "Puducherry",
    }

    url = "https://vikaspedia.in/education/current-affairs/states-and-districts-of-india"
    page.goto(url)  
    page.wait_for_load_state("networkidle")

    content = page.content()

    browser.close()

    soup = BeautifulSoup(content, 'html.parser')

    district_headers = soup.find_all('h3')

    for header in district_headers:
        header_text = header.get_text().strip()

        if any(skip_text in header_text for skip_text in ["States of India (28)", "Union Territories of India (8)", "States and Districts of India"]):
            continue

        start_index = header_text.find("Districts of ") + len("Districts of ")
        end_index = header_text.find(" (")
        district_name = header_text[start_index:end_index]
        State_Iso2 = state_collection.find_one(
            {"name": {"$regex": district_name, "$options": "i"}},
            {"iso2": 1, "_id": 0}
        )
        State_Name = state_collection.find_one(
            {"name": {"$regex": district_name, "$options": "i"}},
        )
        table_body = header.find_next('tbody')
        if table_body:
            for row in table_body.find_all('tr'):
                districts = row.find_all('a')
                for district in districts:
                    district_name = district.get_text().strip()
                    
                    if district_name not in states_and_uts:
                       
                       exits= Districts_collection.find_one({"districts_name":district_name})
                       if not exits:
                            district_id = str(uuid.uuid4()) 
                            Districts_collection.insert_one({
                                    "id": district_id,
                                    "districts_name": district_name,
                                    "state_iso": State_Iso2.get('iso2') if State_Iso2 else None,
                                    "state_name": State_Name.get('name') if State_Iso2 else None
                                })
                            print(f"District {district_name} added to the database.")

def FindingLocalities(playwright):
    # browser = playwright.chromium.launch(headless=False)
    # context = browser.new_context()
    # page = context.new_page()
    mongo_uri="mongodb+srv://DineshG:1234@cluster0.flovz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    Db_name="Location_Data"
    client=MongoClient(mongo_uri)
    Db_Data=client[Db_name]
    District_collection=Db_Data['Cities']
    Distric_name=District_collection.find_one("city_name")
    print(Distric_name,Distric_name.get('city_name'))

    
    # url = f"https://www.mapsofindia.com/{Distric_name.get('city_name') if Distric_name else None}/localities/"
    # page.goto(url)  
    # page.wait_for_load_state("networkidle")
    # browser.close()

    # div_class = '.dir_listing'
    # try:
    #     page.wait_for_selector(div_class, timeout=30000)

    #     html_content = page.inner_html(div_class)
    #     soup = BeautifulSoup(html_content, 'html.parser')
    #     links = soup.find_all('a')
    #     print(html_content)

    #     localities = []
    #     for link in links:
    #         text = link.get_text()
    #         localities.append({'name': text})

    #     for locality in localities:
    #         print(f"Locality: {locality['name']}")

    # except Exception as e:
    #     print(f"An error occurred: {e}")

    # browser.close()

    

with sync_playwright() as playwright:
    FindingLocalities(playwright)


