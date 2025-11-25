import requests
import json

API_KEY = "xxxxxxxxxxxxxxxxxxxxxx"
LOCATION_ID = "-----------------"

url = "https://services.leadconnectorhq.com/contacts/"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Version": "2021-07-28",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

params = {
    "locationId": LOCATION_ID,
    "limit": 10   # important : on récupère juste 10 !
}

response = requests.get(url, headers=headers, params=params)

print("Status:", response.status_code)
print()
print(json.dumps(response.json(), indent=2))
