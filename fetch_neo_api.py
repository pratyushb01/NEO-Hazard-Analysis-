import requests
import pandas as pd
from datetime import datetime, timedelta

# NASA NeoWs API endpoint and key
API_URL = "https://api.nasa.gov/neo/rest/v1/feed"
API_KEY = "DEMO_KEY"  # Replace with your own key for higher limits

def fetch_neo_data(start_date=None, end_date=None, api_key=API_KEY):
    """
    Fetches NEO data from NASA NeoWs API for the given date range.
    Returns a pandas DataFrame with relevant features for ML model.
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
        est_dia_min = neo["estimated_diameter"]["meters"]["estimated_diameter_min"]
        est_dia_max = neo["estimated_diameter"]["meters"]["estimated_diameter_max"]
        rel_vel = float(neo["close_approach_data"][0]["relative_velocity"]["kilometers_per_second"]) if neo["close_approach_data"] else None
        miss_dist = float(neo["close_approach_data"][0]["miss_distance"]["kilometers"]) if neo["close_approach_data"] else None
        orbiting_body = neo["close_approach_data"][0]["orbiting_body"] if neo["close_approach_data"] else None
        hazardous = int(neo["is_potentially_hazardous_asteroid"])
        # Add more features as needed to match your model
        rows.append({
            "Est Dia in M (min)": est_dia_min,
            "Est Dia in M (max)": est_dia_max,
            "Relative Velocity KM per sec": rel_vel,
            "Miss Dist (Kilometers)": miss_dist,
            "Orbiting Body": orbiting_body,
            "Hazardous": hazardous,
            "Name": neo["name"]
        })
    df = pd.DataFrame(rows)
    return df

if __name__ == "__main__":
    # Example usage: fetch today's NEOs
    df = fetch_neo_data()
    print(df.head())
