import requests
import json

API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
LOCATION_ID = "---------------------------"

url = f"https://services.leadconnectorhq.com/locations/{LOCATION_ID}/customFields/"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Version": "2021-07-28",
    "Accept": "application/json",
}

resp = requests.get(url, headers=headers)

print("Status:", resp.status_code)

if resp.status_code != 200:
    print(resp.text)
    exit()

data = resp.json()

print(json.dumps(data, indent=2, ensure_ascii=False))

print("\nTotal custom fields :", len(data.get("customFields", [])))