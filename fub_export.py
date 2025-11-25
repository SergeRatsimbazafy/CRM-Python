import os
import json
import requests
from typing import Dict, Any, List, Optional

#############################################################
# CONFIG
#############################################################

FUB_API_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# OPTION 2 : ou utilise variable d’env : $env:FUB_API_KEY
if os.getenv("FUB_API_KEY"):
    FUB_API_KEY = os.getenv("FUB_API_KEY")

FUB_BASE_URL = "https://api.followupboss.com/v1"
OUTPUT_FILE = "fub_export_by_contact.json"

# LIMITATION (pour tester)
MAX_TASKS = 500   # mettre None pour tout aspirer


#############################################################
# HTTP CLIENT
#############################################################

session_fub = requests.Session()
session_fub.auth = (FUB_API_KEY, "")     # username = clé API, password vide
session_fub.headers.update({
    "Accept": "application/json",
    "Content-Type": "application/json"
})


#############################################################
# API CALLS
#############################################################

def fetch_tasks() -> List[Dict[str, Any]]:
    tasks = []
    params = {"limit": 100}

    while True:
        resp = session_fub.get(FUB_BASE_URL + "/tasks", params=params)
        resp.raise_for_status()

        data = resp.json()
        page = data.get("tasks", [])

        tasks.extend(page)

        if MAX_TASKS and len(tasks) >= MAX_TASKS:
            tasks = tasks[:MAX_TASKS]
            break

        nxt = data.get("_metadata", {}).get("next")
        if not nxt:
            break

        params = {"next": nxt}

    return tasks


_people_cache = {}

def get_person(id: int):
    if id in _people_cache:
        return _people_cache[id]

    resp = session_fub.get(FUB_BASE_URL + f"/people/{id}")
    resp.raise_for_status()

    result = resp.json()
    _people_cache[id] = result
    return result


#############################################################
# HELPERS
#############################################################

def primary_email(person):
    e = person.get("emails") or []
    if not e: return None
    return e[0].get("value")

def primary_phone(person):
    p = person.get("phones") or []
    if not p: return None
    return p[0].get("value")


#############################################################
# MAIN EXPORT
#############################################################

def main():

    if not FUB_API_KEY:
        print("ERREUR: aucune clé API FUB")
        return

    print("Chargement des tâches...")
    tasks = fetch_tasks()
    print(f"{len(tasks)} tâches récupérées.")

    index = {}

    for t in tasks:
        pid = t.get("personId") or t.get("contactId")
        if not pid:
            continue

        person = get_person(pid)

        email = primary_email(person)
        phone = primary_phone(person)

        if email:
            key = f"email:{email}"
        elif phone:
            key = f"phone:{phone}"
        else:
            continue

        if key not in index:
            index[key] = {
                "person": person,
                "tasks": []
            }

        index[key]["tasks"].append(t)

    with open(OUTPUT_FILE, "w", encoding="utf8") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)

    print("")
    print("== TERMINÉ ==")
    print(f"Fichier généré : {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
