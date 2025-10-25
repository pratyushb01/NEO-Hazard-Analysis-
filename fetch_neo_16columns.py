import requests

import pandas as pd
from datetime import datetime, timedelta

# NASA NeoWs API endpoint and key
API_URL = "https://api.nasa.gov/neo/rest/v1/feed"
API_KEY = "34zcspInPylEl6cdJ4zW9Rv0VYYlA61jhIPFEZqh"  # Replace with your own key for higher limits

def fetch_neo_16columns(start_date=None, end_date=None, api_key=API_KEY):
    """
    Fetches NEO data from NASA NeoWs API for the given date range.
    Returns a pandas DataFrame with 16 columns matching the training dataset.
    """
    if start_date is None:
        start_date = datetime.utcnow().date()
    if end_date is None:
        end_date = start_date + timedelta(days=1)
    params = {
        "start_date": str(start_date),
        "end_date": str(end_date),
        "api_key": api_key
    }
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    data = response.json()
    # Flatten NEOs from all dates
    neos = []
    for date in data["near_earth_objects"]:
        neos.extend(data["near_earth_objects"][date])
    # Extract relevant features
    rows = []
    for neo in neos:
        # Neo Reference ID
        neo_ref_id = neo.get("neo_reference_id", None)
        # Estimated diameter (meters, min)
        est_dia_min = neo.get("estimated_diameter", {}).get("meters", {}).get("estimated_diameter_min", None)
        # Jupiter Tisserand Invariant (not available, set None)
        jup_tiss = None
        # Ascending Node Longitude (orbital_data)
        asc_node_long = neo.get("orbital_data", {}).get("ascending_node_longitude", None)
        # Perihelion Time (orbital_data)
        peri_time = neo.get("orbital_data", {}).get("perihelion_time", None)
        # Mean Anomaly (orbital_data)
        mean_anom = neo.get("orbital_data", {}).get("mean_anomaly", None)
        # Perihelion Distance (orbital_data)
        peri_dist = neo.get("orbital_data", {}).get("perihelion_distance", None)
        # Eccentricity (orbital_data)
        eccentricity = neo.get("orbital_data", {}).get("eccentricity", None)
        # Epoch Date Close Approach (from close_approach_data)
        epoch_date_close_approach = None
        rel_vel = None
        miss_dist = None
        approach_data = neo.get("close_approach_data", [])
        if approach_data:
            ca = approach_data[0]
            # Epoch Date Close Approach (convert date to timestamp)
            try:
                epoch_date_close_approach = pd.to_datetime(ca.get("close_approach_date", None)).timestamp() * 1000
            except:
                epoch_date_close_approach = None
            # Relative Velocity KM per sec
            rel_vel = float(ca.get("relative_velocity", {}).get("kilometers_per_second", 0.0))
            # Miss Dist (Kilometers)
            miss_dist = float(ca.get("miss_distance", {}).get("kilometers", 0.0))
        # Orbit Uncertainty (orbital_data)
        orbit_uncertainty = neo.get("orbital_data", {}).get("orbit_uncertainty", None)
        # Perihelion Argument (orbital_data)
        peri_arg = neo.get("orbital_data", {}).get("perihelion_argument", None)
        # Minimum Orbit Intersection (orbital_data)
        min_orbit_intersection = neo.get("orbital_data", {}).get("minimum_orbit_intersection", None)
        # Inclination (orbital_data)
        inclination = neo.get("orbital_data", {}).get("inclination", None)
        # Hazardous
        hazardous = int(neo.get("is_potentially_hazardous_asteroid", False))
        # Compose row
        rows.append({
            "Neo Reference ID": neo_ref_id,
            "Est Dia in M (min)": est_dia_min,
            "Jupiter Tisserand Invariant": jup_tiss,
            "Asc Node Longitude": asc_node_long,
            "Perihelion Time": peri_time,
            "Mean Anomaly": mean_anom,
            "Perihelion Dist": peri_dist,
            "Eccentricity": eccentricity,
            "Epoch Date Close Approach": epoch_date_close_approach,
            "Relative Velocity KM per sec": rel_vel,
            "Miss Dist (Kilometers)": miss_dist,
            "Orbit Uncertainty": orbit_uncertainty,
            "Perihelion Arg": peri_arg,
            "Minimum Orbit Intersection": min_orbit_intersection,
            "Inclination": inclination,
            "Hazardous": hazardous
        })
    df = pd.DataFrame(rows)
    return df

if __name__ == "__main__":
    # Example usage: fetch NEOs for the next 7 days
    start_date = datetime.utcnow().date()
    end_date = start_date + timedelta(days=7)

    df = fetch_neo_16columns(start_date=start_date, end_date=end_date)
    print(df.head())
