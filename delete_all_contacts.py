import requests
import time

API_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXX"
LOCATION_ID = "-----------------------"

BASE_URL = "https://services.leadconnectorhq.com/"
URL_CONTACTS = BASE_URL + "contacts/"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Version": "2021-07-28",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# ------------------------------------
# MODE SÉCURITÉ
# True  = simulation (ne supprime rien)
# False = suppression réelle
DRY_RUN = False   # ⚠️ mettre True si pour tester, False pour effacer pour de vrai
# ------------------------------------

TOTAL = 0


def delete_contact(contact_id):
    url = URL_CONTACTS + contact_id
    resp = requests.delete(url, headers=HEADERS)

    if 200 <= resp.status_code < 300:
        print(f"[OK] Contact supprimé id={contact_id}")
        return True
    else:
        print(f"[ERREUR] {resp.status_code} — {resp.text}")
        return False


def process_page(url):
    global TOTAL

    print("\n=== Récupération ===")
    print(url)

    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print("ERREUR GET contacts :", resp.status_code, resp.text)
        return None

    data = resp.json()
    contacts = data.get("contacts", [])
    meta = data.get("meta", {})

    print(f"Contacts récupérés sur cette page : {len(contacts)}")

    if not contacts:
        return None

    for c in contacts:
        cid = c.get("id")
        email = c.get("email")
        phone = c.get("phone")

        print(f"→ Traitement id={cid} email={email} phone={phone}")

        if DRY_RUN:
            print(f"[DRY RUN] Je supprimerais {cid}")
        else:
            delete_contact(cid)
            # petite pause pour éviter le rate limit
            time.sleep(0.05)  # tu peux remettre 0.1 si ça rate-limit

        TOTAL += 1

    next_url = meta.get("nextPageUrl")
    return next_url


def run():
    # 🟦 ici on passe sur 500 contacts par page
    url = f"{URL_CONTACTS}?locationId={LOCATION_ID}&limit=100"
    while url:
        url = process_page(url)

    print("\n\n===== FIN =====")
    print(f"Contacts traités : {TOTAL}")
    if DRY_RUN:
        print("Mode DRY RUN : rien n'a été supprimé.")
    else:
        print("Tous ces contacts ont été supprimés pour de vrai.")


if __name__ == "__main__":
    run()
