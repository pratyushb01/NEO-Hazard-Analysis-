import requests
import pandas as pd
import time

# === 1. CONFIG ===
API_KEY = "34zcspInPylEl6cdJ4zW9Rv0VYYlA61jhIPFEZqh"   # 🔑 Replace with your NASA API key
BASE_URL = "https://api.nasa.gov/neo/rest/v1/neo/browse"
OUTPUT_FILE = "neoWs_dataset.csv"

print("🚀 Fetching data from NASA NeoWs API...")

neos = []  # store all asteroid entries
page = 0

# === 2. PAGINATED FETCH ===
while True:
    params = {"page": page, "api_key": API_KEY}
    response = requests.get(BASE_URL, params=params)
    
    if response.status_code != 200:
        print(f"⚠️ Error fetching page {page}: {response.status_code}")
        break

    data = response.json()
    neo_list = data.get("near_earth_objects", [])

    if not neo_list:
        break  # stop if no more NEOs

    for neo in neo_list:
        # Extract safe fields with default fallbacks
        name = neo.get("name", "")
        abs_mag = neo.get("absolute_magnitude_h", None)
        hazardous = neo.get("is_potentially_hazardous_asteroid", False)

        est_diam = neo.get("estimated_diameter", {}).get("kilometers", {})
        diam_min = est_diam.get("estimated_diameter_min", None)
        diam_max = est_diam.get("estimated_diameter_max", None)

        # Most recent close approach data
        approach_data = neo.get("close_approach_data", [])
        if approach_data:
            ca = approach_data[0]
            miss_dist = float(ca.get("miss_distance", {}).get("kilometers", 0.0))
            rel_vel = float(ca.get("relative_velocity", {}).get("kilometers_per_second", 0.0))
            approach_date = ca.get("close_approach_date", "")
        else:
            miss_dist, rel_vel, approach_date = None, None, None

        neos.append({
            "name": name,
            "abs_mag": abs_mag,
            "diameter_min_km": diam_min,
            "diameter_max_km": diam_max,
            "relative_velocity_km_s": rel_vel,
            "miss_distance_km": miss_dist,
            "approach_date": approach_date,
            "is_hazardous": int(hazardous)
        })

    print(f"✅ Page {page+1} fetched ({len(neo_list)} NEOs, total so far: {len(neos)})")
    
    page += 1
    # Stop if there’s no "next" page
    if not data.get("links", {}).get("next"):
        break

    time.sleep(1)  # avoid hitting NASA rate limits

# === 3. SAVE DATA ===
df = pd.DataFrame(neos)
df.to_csv(OUTPUT_FILE, index=False)
print(f"\n📦 Dataset saved as: {OUTPUT_FILE}")
print(f"Total records: {len(df)}")
print(df.head())
