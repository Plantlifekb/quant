import os

PATH = r"C:\Quant\data\signals\weekly_selection.csv"

print("Exists:", os.path.exists(PATH))
print("Absolute path:", os.path.abspath(PATH))
print("Size (bytes):", os.path.getsize(PATH))

print("\nFirst 20 lines:")
with open(PATH, "r") as f:
    for _ in range(20):
        print(f.readline().strip())