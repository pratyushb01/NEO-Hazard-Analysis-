import requests
import pandas as pd
import time

# === 1. CONFIG ===
API_KEY = "34zcspInPylEl6cdJ4zW9Rv0VYYlA61jhIPFEZqh"  # Replace with your NASA API key
BASE_URL = "https://api.nasa.gov/neo/rest/v1/neo/browse"
OUTPUT_FILE = "neoWs_16columns_dataset.csv"

print("🚀 Fetching all NEO data from NASA NeoWs API...")

neos = []
page = 0

# === 2. AUTO-PAGINATED FETCH ===
while True:
    params = {"page": page, "api_key": API_KEY}
    response = requests.get(BASE_URL, params=params)
    
    if response.status_code != 200:
        print(f"⚠ Error fetching page {page}: {response.status_code}")
        break

    data = response.json()
    neo_list = data.get("near_earth_objects", [])

    if not neo_list:
        break  # stop when no more data

    for neo in neo_list:
        # === Basic Fields ===
        neo_ref_id = neo.get("neo_reference_id", "")
        abs_mag = neo.get("absolute_magnitude_h", None)
        hazardous = int(neo.get("is_potentially_hazardous_asteroid", False))

        # === Estimated Diameter (in meters) ===
        est_diam = neo.get("estimated_diameter", {}).get("meters", {})
        est_diam_min_m = est_diam.get("estimated_diameter_min", None)

        # === Orbital Data ===
        orbital = neo.get("orbital_data", {})
        jup_tisserand = orbital.get("jupiter_tisserand_invariant", None)
        asc_node_long = orbital.get("ascending_node_longitude", None)
        peri_time = orbital.get("perihelion_time", None)
        mean_anomaly = orbital.get("mean_anomaly", None)
        peri_dist = orbital.get("perihelion_distance", None)
        eccentricity = orbital.get("eccentricity", None)
        orbit_uncertainty = orbital.get("orbit_uncertainty", None)
        peri_arg = orbital.get("perihelion_argument", None)
        min_orbit_intersection = orbital.get("minimum_orbit_intersection", None)
        inclination = orbital.get("inclination", None)

        # === Close Approach Data (take first entry) ===
        ca_data = neo.get("close_approach_data", [])
        if ca_data:
            ca = ca_data[0]
            epoch_date_close_approach = ca.get("epoch_date_close_approach", None)
            rel_vel_km_s = float(ca.get("relative_velocity", {}).get("kilometers_per_second", 0.0))
            miss_dist_km = float(ca.get("miss_distance", {}).get("kilometers", 0.0))
        else:
            epoch_date_close_approach, rel_vel_km_s, miss_dist_km = None, None, None

        # === Append Row ===
        neos.append({
            "Neo Reference ID": neo_ref_id,
            "Est Dia in M (min)": est_diam_min_m,
            "Jupiter Tisserand Invariant": jup_tisserand,
            "Asc Node Longitude": asc_node_long,
            "Perihelion Time": peri_time,
            "Mean Anomaly": mean_anomaly,
            "Perihelion Dist": peri_dist,
            "Eccentricity": eccentricity,
            "Epoch Date Close Approach": epoch_date_close_approach,
            "Relative Velocity KM per sec": rel_vel_km_s,
            "Miss Dist (Kilometers)": miss_dist_km,
            "Orbit Uncertainty": orbit_uncertainty,
            "Perihelion Arg": peri_arg,
            "Minimum Orbit Intersection": min_orbit_intersection,
            "Inclination": inclination,
            "Hazardous": hazardous
        })

    print(f"✅ Page {page+1} fetched ({len(neo_list)} NEOs, total so far: {len(neos)})")
    
    page += 1
    if not data.get("links", {}).get("next"):
        break  # no next page

    time.sleep(1)  # respect NASA API limits

# === 3. SAVE TO CSV ===
df = pd.DataFrame(neos)
df.to_csv(OUTPUT_FILE, index=False)
print(f"\n📦 Dataset saved as: {OUTPUT_FILE}")
print(f"Total records: {len(df)}")
print(df.head())