import requests

API_KEY = "fxxxxxxxxxxxxxxxxxxxxx"   # remplace

session = requests.Session()
session.auth = (API_KEY, "")
session.headers.update({
    "Accept": "application/json"
})

resp = session.get("https://api.followupboss.com/v1/tasks")

print("status:", resp.status_code)
print(resp.text)
