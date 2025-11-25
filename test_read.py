import pandas as pd

FILE_PATH = "contacts.csv"

df = pd.read_csv(FILE_PATH, nrows=10)

print(df.head())
print(df.columns)
