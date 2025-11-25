import pandas as pd
import os

# === CONFIG ===
INPUT_FILE = "contacts_ghl_part4.csv"
OUTPUT_FILE = "contactsv2"            # sans extension
SPLIT_SIZE = 10000                    # lignes par fichier

# lecture du CSV
df = pd.read_csv(INPUT_FILE)

# calcul du nombre de morceaux
num_chunks = (len(df) // SPLIT_SIZE) + (1 if len(df) % SPLIT_SIZE != 0 else 0)

for i in range(num_chunks):
    start = i * SPLIT_SIZE
    end = start + SPLIT_SIZE
    
    chunk = df[start:end]
    
    out_path = f"{OUTPUT_FILE}_part{i+1}.csv"
    
    chunk.to_csv(out_path, index=False)   # header conservé automatiquement
    
    print(f"✔ Fichier créé : {out_path} ({len(chunk)} lignes)")
    
print("Terminé.")



