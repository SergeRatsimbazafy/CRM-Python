import csv
import json
import time
from typing import Dict, Any, Optional, Tuple

import requests

#################################################
# CONFIG
#################################################

API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxx"   
BASE = "https://services.leadconnectorhq.com"

FUB_JSON = "fub_export_by_contact.json"   # export FUB
GHL_CSV = "contacts_ghl.csv"              # export GHL (renomme-le si besoin)

SLEEP = 0.2  # pause anti rate-limit


#################################################
# HTTP SESSION
#################################################

session = requests.Session()
session.headers.update({
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}",
    "Version": "2021-07-28",
})


#################################################
# HELPERS
#################################################

def normalize_email(email: Optional[str]) -> Optional[str]:
    if not email:
        return None
    return email.strip().lower()


def normalize_phone(phone: Optional[str]) -> Optional[str]:
    if not phone:
        return None
    # on garde seulement chiffres + +
    digits = "".join(ch for ch in phone if ch.isdigit() or ch == "+")
    return digits or None


def load_ghl_contacts_csv(path: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Lit le CSV exporté de GHL (entêtes connus) et construit :
      - index_email[email_lower] = contactId
      - index_phone[phone_norm] = contactId

    Entêtes attendues :
      "Contact Id","First Name","Last Name","Name","Phone","Email","Created","Last Activity","Tags"
    """
    index_email: Dict[str, str] = {}
    index_phone: Dict[str, str] = {}

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        print("[GHL CSV] Colonnes trouvées :", reader.fieldnames)

        for row in reader:
            cid = row.get("Contact Id")
            if not cid:
                continue

            email = normalize_email(row.get("Email"))
            phone = normalize_phone(row.get("Phone"))

            if email:
                index_email[email] = cid
            if phone:
                index_phone[phone] = cid

    print(f"[GHL CSV] index_email = {len(index_email)}")
    print(f"[GHL CSV] index_phone = {len(index_phone)}")
    return index_email, index_phone


def make_due(task: Dict[str, Any]) -> Optional[str]:
    """Transforme la date FUB en dueDate pour GHL."""
    if task.get("dueDateTime"):
        return task["dueDateTime"]
    if task.get("dueDate"):
        return task["dueDate"] + "T00:00:00.000Z"
    return None


def create_task(contact_id: str, task: Dict[str, Any]):
    """Crée une tâche dans GHL pour un contact donné."""
    url = f"{BASE}/contacts/{contact_id}/tasks"

    payload = {
        "title": task.get("name") or "Imported from FUB",
        "body": f"Imported FUB task #{task.get('id')}",
        "dueDate": make_due(task),
        "completed": False,
    }

    print(f"[GHL] CREATE TASK → contact {contact_id} / '{payload['title']}'")
    resp = session.post(url, json=payload)
    print("   →", resp.status_code, resp.text[:200])
    time.sleep(SLEEP)


#################################################
# MAIN
#################################################

def main():
    # 1) Charger le CSV GHL
    index_email, index_phone = load_ghl_contacts_csv(GHL_CSV)

    # 2) Charger l’export FUB
    try:
        with open(FUB_JSON, "r", encoding="utf-8") as f:
            fub_data = json.load(f)
    except FileNotFoundError:
        print(f"❌ Fichier JSON introuvable : {FUB_JSON}")
        return

    imported = 0
    skipped = 0
    not_found = 0

    # 3) Pour chaque clé FUB (email:... ou phone:...)
    for key, entry in fub_data.items():
        tasks = entry.get("tasks", [])

        contact_id = None

        if key.startswith("email:"):
            email = normalize_email(key.replace("email:", "").strip())
            print("====================")
            print("EMAIL:", email)
            contact_id = index_email.get(email)

        elif key.startswith("phone:"):
            raw_phone = key.replace("phone:", "").strip()
            phone = normalize_phone(raw_phone)
            print("====================")
            print("PHONE:", raw_phone, "→", phone)
            contact_id = index_phone.get(phone)

        else:
            skipped += len(tasks)
            continue

        if not contact_id:
            print("❌ Pas de contactId trouvé dans le CSV pour cette clé, tâches ignorées.")
            not_found += len(tasks)
            continue

        for t in tasks:
            create_task(contact_id, t)
            imported += 1

    print("\n===== FIN =====")
    print("Tâches importées :", imported)
    print("Tâches ignorées (pas de match dans CSV) :", not_found)
    print("Tâches ignorées (clé non email/phone) :", skipped)


if __name__ == "__main__":
    main()
