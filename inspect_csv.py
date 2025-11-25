import pandas as pd

FILE_PATH = "contacts.csv"  # ou contacts_sample.csv si tu as un échantillon

df = pd.read_csv(FILE_PATH, low_memory=False)

print("Nombre de lignes :", len(df))
print("\nColonnes :")
for col in df.columns:
    print("-", col)