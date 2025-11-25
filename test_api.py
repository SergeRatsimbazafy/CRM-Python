import requests

API_KEY = "xxxxxxxxxxxxxxxxxxxxxxx"
LOCATION_ID = "------------------"

url = "https://services.leadconnectorhq.com/contacts/"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Version": "2021-07-28",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

payload = {
    "firstName": "Test",
    "lastName": "Contact",
    "email": "test.contact@example.com",
    "phone": "+33600000000",
    "locationId": LOCATION_ID,
}

response = requests.post(url, headers=headers, json=payload)

print("Status code:", response.status_code)
print("Réponse :")
print(response.text)
