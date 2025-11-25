import pandas as pd
import os

# === CONFIG ===
INPUT_FILE = "contacts.csv"          # ton fichier source
OUTPUT_FILE = "contacts_ghl.csv"     # base du nom pour les fichiers de sortie
SPLIT_SIZE = 18000                   # nombre de lignes par fichier (None = un seul fichier)

# Colonnes d'origine (du CSV) qu'on garde
COLUMNS_TO_KEEP = [
    "First Name",
    "Last Name",
    "Stage",
    "Lead Source",
    "Assigned To",
    "Is Contacted",
    "Listing Price",
    "Tags",
    "Email 1",
    "Email 2",
    "Phone 1",
    "Phone 2",
    "Address 1 - Street",
    "Address 1 - City",
    "Address 1 - State",
    "Address 1 - Zip",
    "Address 1 - Country",
    "Relationship 1 First Name",
    "Relationship 1 Last Name",
    "Relationship 1 Type",
    "Relationship 1 Phone 1",
    "Relationship 2 First Name",
    "Relationship 2 Last Name",
    "Relationship 2 Type",
    "Relationship 2 Phone 1",
    "Property Address",
    "Property City",
    "Property State",
    "Property Postal Code",
    "Property MLS Number",
    "Property Price",
    "Property Beds",
    "Property Baths",
    "Property Area",
    "Property Lot",
    "Notes",
    "Texts",
    "Anniversaire d'achat",
    "Budget",
    "Langue",
    "Matrix a Faire",
    "Nom conjoint(e)",
    "Prénom conjoint(e)",
    "Type de client",
    "Website",
]

# Renommage des colonnes pour qu'elles correspondent aux noms de champs GHL
# (standard + custom fields que tu as créés)
RENAME_FOR_GHL = {
    # Standard GHL
    "First Name": "First Name",
    "Last Name": "Last Name",
    "Email 1": "Email",
    "Email 2": "Additional Email",
    "Phone 1": "Phone",
    "Phone 2": "Additional Phone",
    "Address 1 - Street": "Street Address",
    "Address 1 - City": "City",
    "Address 1 - State": "State",
    "Address 1 - Zip": "Postal Code",
    "Address 1 - Country": "Country",
    "Website": "Website",
    "Tags": "Tags",

    # Custom fields FUB / pipeline
    "Lead Source": "*Lead Source FUB",
    "Stage": "* Stage FUB",
    "Listing Price": "*Listing Price FUB",
    "Is Contacted": "Is Contacted",  # même nom que le custom field
    "Type de client": "*Type de client FUB",  # tu peux le changer en "Type de client" si tu préfères ce champ-là
    "Matrix a Faire": "*Matrix a Faire FUB",
    "Assigned To": "*Assignation FUB",

    # Notes & Texts vers custom fields
    "Notes": "Note FUB",
    "Texts": "Texts FUB",

    # Relations
    "Relationship 1 First Name": "*Relationship 1 First Name FUB",
    "Relationship 1 Last Name": "*Relationship 1 Last Name FUB",
    "Relationship 1 Phone 1": "*Relationship 1 Phone 1 FUB",
    "Relationship 1 Type": "*Relationship 1 Type FUB",

    "Relationship 2 First Name": "*Relationship 2 First Name FUB",
    "Relationship 2 Last Name": "*Relationship 2 Last Name FUB",
    "Relationship 2 Phone 1": "*Relationship 2 Phone 1 FUB",
    "Relationship 2 Type": "*Relationship 2 Type FUB",

    "Nom conjoint(e)": "Nom conjoint(e)",
    "Prénom conjoint(e)": "Prénom conjoint(e)",

    # Infos client
    "Langue": "Langue",
    "Budget": "Budget $",
    "Anniversaire d'achat": "Date anniversaire ACHAT",

    # Property (déjà identiques aux custom fields GHL)
    "Property Address": "Property Address",
    "Property City": "Property City",
    "Property State": "Property State",
    "Property Postal Code": "Property Postal Code",
    "Property MLS Number": "Property MLS Number",
    "Property Price": "Property Price",
    "Property Beds": "Property Beds",
    "Property Baths": "Property Baths",
    "Property Area": "Property Area",
    "Property Lot": "Property Lot",
}


def main():
    print(f"Lecture de {INPUT_FILE} ...")
    df = pd.read_csv(INPUT_FILE, low_memory=False)

    print("Colonnes trouvées dans le CSV :")
    print(df.columns.tolist())
    print("------")

    # On garde uniquement les colonnes qui existent vraiment dans le fichier
    existing_cols = [c for c in COLUMNS_TO_KEEP if c in df.columns]
    missing_cols = [c for c in COLUMNS_TO_KEEP if c not in df.columns]

    print("Colonnes conservées avant renommage :")
    print(existing_cols)

    if missing_cols:
        print("\nColonnes configurées mais absentes du fichier :")
        print(missing_cols)

    df = df[existing_cols]

    # Suppression des lignes sans email ET sans téléphone
    if "Email 1" in df.columns and "Phone 1" in df.columns:
        before = len(df)
        df = df.dropna(how="all", subset=["Email 1", "Phone 1"])
        after = len(df)
        print(f"\nLignes avant nettoyage : {before}")
        print(f"Lignes après suppression des lignes sans Email 1 et sans Phone 1 : {after}")
    else:
        print("\nAttention : 'Email 1' ou 'Phone 1' manquent, aucune ligne supprimée sur cette base.")

    # Renommage des colonnes pour coller aux champs GHL
    rename_effectif = {old: new for old, new in RENAME_FOR_GHL.items() if old in df.columns}
    df = df.rename(columns=rename_effectif)

    print("\nColonnes finales (pour import GHL) :")
    print(df.columns.tolist())

    # Sauvegarde : un ou plusieurs fichiers
    if SPLIT_SIZE is None:
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"\n✅ Fichier nettoyé sauvegardé sous : {OUTPUT_FILE}")
    else:
        total_rows = len(df)
        if total_rows == 0:
            print("\n⚠️ Aucun enregistrement après filtrage, rien à sauvegarder.")
            return

        num_parts = (total_rows // SPLIT_SIZE) + (1 if total_rows % SPLIT_SIZE != 0 else 0)
        base_name, ext = os.path.splitext(OUTPUT_FILE)

        print(f"\nDécoupage en morceaux de {SPLIT_SIZE} lignes (~{num_parts} fichiers) ...")

        for i in range(num_parts):
            start = i * SPLIT_SIZE
            end = min(start + SPLIT_SIZE, total_rows)
            part_df = df.iloc[start:end]
            part_file = f"{base_name}_part{i+1}{ext}"
            part_df.to_csv(part_file, index=False, encoding="utf-8-sig")
            print(f"  -> {part_file} ({len(part_df)} lignes)")

        print("\n✅ Découpage terminé. Fichiers prêts pour import GHL.")


if __name__ == "__main__":
    main()
