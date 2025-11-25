import pandas as pd

FILE_PATH = "contacts.csv"

# === lignes à inspecter ===
LINES = [434, 464, 469]

df = pd.read_csv(FILE_PATH, low_memory=False)

for line in LINES:
    print("\n" + "="*50)
    print(f"LIGNE {line}")
    print("="*50)

    try:
        print(df.loc[line])
    except:
        print(">> erreur : ligne inexistante")