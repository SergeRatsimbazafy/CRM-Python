import pandas as pd
import requests
import time
import json
import os

# ========= CONFIG =========

FILE_PATH = "contacts.csv"  # ou "contacts_sample.csv" pour tester

API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxx"
LOCATION_ID = "----------------------"

CONTACTS_URL = "https://services.leadconnectorhq.com/contacts/"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Version": "2021-07-28",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

CHECKPOINT_FILE = "import_checkpoint.json"

# 👉 pour cette exécution seulement : on reprend à la ligne 2900
START_AT_INDEX = 2900  # après 2900, tu pourras remettre ça à 0

SLEEP_BETWEEN_REQUESTS = 0.01  # tu peux ajuster (0.01, 0.05, etc.)

# ========= MAPPING CSV → CHAMPS STANDARDS GHL =========

RENAME_MAP = {
    "First Name": "firstName",
    "Last Name": "lastName",

    "Email 1": "email",
    "Phone 1": "phone",

    # lus mais pas envoyés en standard (on pourrait les utiliser plus tard)
    "Email 2": "additional_email",
    "Phone 2": "additional_phone",

    "Address 1 - Street": "address1",
    "Address 1 - City": "city",
    "Address 1 - State": "state",
    "Address 1 - Zip": "postalCode",
    "Address 1 - Country": "country",

    "Website": "website",
}

# ========= MAPPING CSV → CUSTOM FIELDS GHL =========

CUSTOM_FIELD_MAP = {
    # ----- FUB / pipeline -----
    "Lead Source": "wQJA1YTpIHMH93m4fDOz",          # *Lead Source FUB
    "Stage": "NyPerfWZxx2quGb1spyA",                # * Stage FUB
    "Listing Price": "ezsbYgAHuHWJ54ckJk3V",        # *Listing Price FUB
    "Is Contacted": "d6V1mkhHesKBOqDPQYdS",         # Is Contacted
    "Type de client": "9mJWvriT21beZ9jOlOPr",       # *Type de client FUB
    "Matrix a Faire": "cz3iw2A5xYee0fTpK0xP",       # *Matrix a Faire FUB
    "Assigned To": "ykhczVRiGitAZTeBlGDS",          # *Assignation FUB

    # ----- Notes & Texts FUB -----
    "Notes": "q7OOVTvfPPeUoFo3oW8p",               # Note FUB (LARGE_TEXT)
    "Texts": "3wVuw4RygbTr9tyQuDvK",               # Texts FUB (LARGE_TEXT)

    # ----- Relations -----
    "Relationship 1 First Name": "jojOS7TJdm27Y3Yln92z",
    "Relationship 1 Last Name": "8I3p90AqU3JxyTHpflWS",
    "Relationship 1 Phone 1": "40FTH7wX2Dk13qim9pRH",
    "Relationship 1 Type": "AoARQxmYYF0yDkx6tJ0Q",

    "Relationship 2 First Name": "8QY9AjwEqEmfHw0FKpzo",
    "Relationship 2 Last Name": "UGnR7wrcWFKFXaWekd1K",
    "Relationship 2 Phone 1": "YPVinwOyvTEs73IGgxGD",
    "Relationship 2 Type": "KlRL3SwvAWdYAfilmRLD",

    "Nom conjoint(e)": "An5bgO5IuLqsKl442saB",
    "Prénom conjoint(e)": "FdElWrOzpb39clab5aZo",

    # ----- Infos client -----
    "Langue": "QCvPLEYEnH5CtwpwK2tw",
    "Budget": "5qKZllLXA1EuOZrIvAdV",

    # ----- Property -----
    "Property Address": "BkoorOhDLPeoWuDfItR0",
    "Property City": "uANNqiX9yN9VuEjd9H5e",
    "Property State": "A7YpVnzaDVahEg4U9ewG",
    "Property Postal Code": "H6AlUuV57jClUfyRW7qj",
    "Property MLS Number": "CgpGV7dfJHnQNd0J4b2j",
    "Property Price": "4ACqGmztkMiH0M1IK598",
    "Property Beds": "AXGNAkYINFQWqHVmhm8p",
    "Property Baths": "S9LTtnpZGxjLr7naTqsU",
    "Property Area": "mUOfo7MN1Lf02e0KUyE1",
    "Property Lot": "7Y2A6Y6yEP2hcjdoZfQ3",

    # ----- Campagnes -----
    "Campaign Source": "TqJHogBYhfEZRnYzl4Mk",
    "Campaign Medium": "O5b4PvNKlRyj6Sc1UosL",
    "Campaign Term": "bFqeugalK3wXCKJkLwRP",
    "Campaign Content": "pePsTMAyXEVmONyIaZOx",
    "Campaign Name": "uEuQ5yws4q7wrKSsfJAo",

    # ----- Dates / deal -----
    "Anniversaire d'achat": "Stu1ePLum3ImpUmllOuf",
    "Deal Close Date": "FYFS9DoFQIMxqcs1D8Lz",
}

# Si plus tard tu veux traiter certains champs comme "MULTIPLE_OPTIONS"
# tu pourras les ajouter ici (ex: "Smart List", "Stage Acheteur", etc.)
MULTI_VALUE_COLUMNS = set()


# ========= CHARGEMENT / NETTOYAGE =========

def load_and_prepare():
    df = pd.read_csv(FILE_PATH, low_memory=False)

    # Renommer les colonnes standards
    df = df.rename(columns=RENAME_MAP)

    # Colonnes standard envoyées sur /contacts
    standard_cols = [
        "firstName", "lastName",
        "email", "phone",
        "additional_email", "additional_phone",
        "address1", "city", "state", "postalCode", "country",
        "website",
        "Tags",   # les tags sont envoyés dans payload["tags"]
    ]

    cols_to_keep = set(standard_cols) | set(CUSTOM_FIELD_MAP.keys())
    existing_cols = [c for c in df.columns if c in cols_to_keep]
    df = df[existing_cols]

    # Enlever les lignes où email ET phone sont vides
    if "email" in df.columns and "phone" in df.columns:
        df = df.dropna(how="all", subset=["email", "phone"])

    print("Colonnes utilisées :", df.columns.tolist())
    print("Nombre de lignes après nettoyage :", len(df))

    return df


# ========= CUSTOM FIELDS =========

def build_custom_fields(row):
    custom_fields = []

    for csv_col, field_id in CUSTOM_FIELD_MAP.items():
        if csv_col not in row.index:
            continue

        value = row[csv_col]
        if pd.isna(value):
            continue

        if csv_col in MULTI_VALUE_COLUMNS and isinstance(value, str):
            parts = [v.strip() for v in value.replace(";", ",").split(",") if v.strip()]
            if not parts:
                continue
            custom_fields.append({"id": field_id, "value": parts})
        else:
            custom_fields.append({"id": field_id, "value": value})

    return custom_fields


# ========= CONTACTS =========

def create_contact(row):
    payload = {
        "locationId": LOCATION_ID,
        "firstName": row.get("firstName"),
        "lastName": row.get("lastName"),
        "email": row.get("email"),
        "phone": row.get("phone"),
        "address1": row.get("address1"),
        "city": row.get("city"),
        "state": row.get("state"),
        "postalCode": row.get("postalCode"),
        "country": row.get("country"),
        "website": row.get("website"),
    }

    # --- TAGS ---
    tags_raw = row.get("Tags", "")
    if isinstance(tags_raw, str) and tags_raw.strip():
        tags = [t.strip() for t in tags_raw.split(",") if t.strip()]
        if tags:
            payload["tags"] = tags

    # Nettoyage : supprimer les None / NaN
    clean_payload = {}
    for k, v in payload.items():
        if v is None:
            continue
        if isinstance(v, float) and pd.isna(v):
            continue
        clean_payload[k] = v

    # Custom fields
    custom_fields = build_custom_fields(row)
    if custom_fields:
        clean_payload["customFields"] = custom_fields

    resp = requests.post(CONTACTS_URL, headers=HEADERS, json=clean_payload)

    try:
        data = resp.json()
    except Exception:
        data = None

    if 200 <= resp.status_code < 300:
        contact_id = None
        if isinstance(data, dict):
            contact_id = (
                data.get("id")
                or (data.get("contact") or {}).get("id")
            )
        return True, None, contact_id
    else:
        return False, f"{resp.status_code} - {resp.text}", None


# ========= CHECKPOINTS =========

def load_checkpoint():
    """
    Retourne l'index à partir duquel on doit commencer.
    Combine :
      - START_AT_INDEX (config)
      - dernier index stocké dans import_checkpoint.json (si existe)
    """
    start_at = START_AT_INDEX

    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            last_index = int(data.get("last_index", -1))
            if last_index >= 0:
                # On reprend à la ligne juste après la dernière traitée
                start_at = max(start_at, last_index + 1)
                print(f"Checkpoint trouvé, reprise à partir de la ligne {start_at}.")
                return start_at
        except Exception as e:
            print(f"Impossible de lire le checkpoint ({e}), on utilise START_AT_INDEX={START_AT_INDEX}.")

    print(f"Aucun checkpoint trouvé, démarrage à partir de la ligne {start_at}.")
    return start_at


def save_checkpoint(idx):
    data = {"last_index": int(idx)}
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ========= MAIN =========

def main():
    df = load_and_prepare()
    total = len(df)

    start_at = load_checkpoint()

    print(f"Nombre de contacts à envoyer : {total}")

    contact_errors = []

    for idx, row in df.iterrows():
        # --- SKIP avant le point de reprise ---
        if idx < start_at:
            continue

        ok, err, contact_id = create_contact(row)

        if not ok:
            contact_errors.append({"index": int(idx), "error": err})
            print(f"[ERREUR CONTACT] ligne {idx} : {err}")

        # Sauvegarde du checkpoint après chaque ligne traitée
        save_checkpoint(idx)

        if idx % 100 == 0:
            print(f"{idx}/{total} lignes traitées...")

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    if contact_errors:
        with open("import_contact_errors.json", "w", encoding="utf-8") as f:
            json.dump(contact_errors, f, ensure_ascii=False, indent=2)
        print(f"{len(contact_errors)} erreurs contacts dans import_contact_errors.json")

    print("Import terminé.")


if __name__ == "__main__":
    main()
