import requests
import pandas as pd
from datetime import datetime
import math

# 1. API Config
API_URL = "https://ssd-api.jpl.nasa.gov/cad.api"
PARAMS = {
    "date-min": "1900-01-01",
    "date-max": "2025-12-31",
    "dist-max": "0.5",   # 0.5 AU (~75 million km)
    "fullname": "true"
}

print("Fetching data from NASA CAD API...")
response = requests.get(API_URL, params=PARAMS)
if response.status_code != 200:
    print(f"Error fetching data: {response.status_code}")
    exit()

data = response.json()
fields = data["fields"]
rows = data["data"]

df = pd.DataFrame(rows, columns=fields)

# 2. Convert distance AU → km
AU_TO_KM = 149_597_870.7
df["dist"] = df["dist"].astype(float) * AU_TO_KM

# 3. Estimate diameter using H magnitude
# Formula: D(km) = 1329/sqrt(p) * 10^(-H/5), assume p=0.14
p = 0.14
df["h"] = pd.to_numeric(df["h"], errors="coerce")
df["diameter_m"] = (1329 / math.sqrt(p)) * (10 ** (-df["h"] / 5)) * 1000  # in meters

# 4. Hazardous flag: diameter > 140m & distance < 0.05 AU
HAZARD_DIST_KM = 0.05 * AU_TO_KM
df["hazardous"] = df.apply(
    lambda x: 1 if (x["diameter_m"] > 140) and (x["dist"] < HAZARD_DIST_KM) else 0,
    axis=1
)

# 5. Future flag
current_date = datetime.utcnow()
df["cd_datetime"] = pd.to_datetime(df["cd"], errors="coerce")
df["is_future"] = df["cd_datetime"].apply(lambda d: 1 if d > current_date else 0)

# Save CSV
OUTPUT_FILE = "neo_close_approach_1900_2025.csv"
df.to_csv(OUTPUT_FILE, index=False)

print(f"\nDataset saved: {OUTPUT_FILE}")
print(f"Total records: {len(df)}")
print(f"Hazardous objects count: {df['hazardous'].sum()}")
print(f"Future approaches count: {df['is_future'].sum()}")
