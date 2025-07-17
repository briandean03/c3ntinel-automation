import requests
import pandas as pd
from datetime import datetime
import time
import os

# Ceentiel credentials (use environment variables)
CLIENT_ID = os.getenv("FAULTY_CLIENT_ID")
CLIENT_SECRET = os.getenv("FAULTY_CLIENT_SECRET")
BASE_API = "https://api.c3ntinel.com/2"

DELTA_THRESHOLD = 1000000  # Flag if change between consecutive readings exceeds this

def get_token():
    url = "https://auth.c3ntinel.com/sso/oauth/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        r = requests.post(url, data=payload, headers=headers)
        r.raise_for_status()
        return r.json()["access_token"]
    except requests.RequestException as e:
        print(f"⚠️ Failed to get token: {e}")
        raise

def get_meters(token):
    url = f"{BASE_API}/meter/search"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"query": "PWR or ENG"}
    try:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()["_embedded"]["meters"]
    except requests.RequestException as e:
        print(f"⚠️ Failed to get meters: {e}")
        raise

def get_meter_readings(token, meter_id, start_date, end_date):
    url = f"{BASE_API}/meter/{meter_id}/readings"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"start_date": start_date, "end_date": end_date}
    try:
        r = requests.get(url, headers=headers, params=params)
        if r.status_code == 200:
            return r.json().get("readings", [])
        return []
    except requests.RequestException as e:
        print(f"⚠️ Failed to get readings for meter {meter_id}: {e}")
        return []

def get_site_info(token, site_id):
    url = f"{BASE_API}/site/{site_id}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers)
        return r.json() if r.status_code == 200 else {}
    except requests.RequestException as e:
        print(f"⚠️ Failed to get site info for site {site_id}: {e}")
        return {}

def main(start_date, end_date):
    token = get_token()
    print("✅ Authenticated with Ceentiel")

    meters = get_meters(token)
    print(f"🔍 Scanning {len(meters)} meters...")

    faulty_meters = []

    for i, meter in enumerate(meters, start=1):
        meter_id = meter.get("meterId")
        site_id = meter.get("siteId")
        meter_name = meter.get("name")

        if not any(tag in meter_name.upper() for tag in ["ENG", "PWR"]):
            print(f"[{i}/{len(meters)}] ❌ Skipping {meter_name}")
            continue

        site_info = get_site_info(token, site_id)
        site_name = site_info.get("name", "Unknown")

        readings = get_meter_readings(token, meter_id, start_date, end_date)
        previous_value = None
        previous_time = None

        for reading in readings:
            value = reading.get("value")
            ts = reading.get("date")

            try:
                value = float(value)
            except:
                continue

            try:
                timestamp = ts if isinstance(ts, str) else datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
            except:
                timestamp = "Invalid timestamp"

            if previous_value is not None:
                delta = abs(value - previous_value)
                if delta > DELTA_THRESHOLD:
                    faulty_meters.append({
                        "meter_name": meter_name,
                        "meter_id": meter_id,
                        "site_name": site_name,
                        "previous_value": previous_value,
                        "current_value": value,
                        "delta": delta,
                        "previous_time": previous_time,
                        "current_time": timestamp
                    })

            previous_value = value
            previous_time = timestamp

        print(f"[{i}/{len(meters)}] ✅ Checked {meter_name}")
        time.sleep(0.3)

    output_dir = "public"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "faulty_meter_deltas.csv")
    if faulty_meters:
        df = pd.DataFrame(faulty_meters)
        df.to_csv(filename, index=False)
        print(f"\n🚨 Faulty meters with abnormal jumps found! Saved to '{filename}'")
    else:
        print("\n✅ No spikes detected.")
        # Create empty CSV to indicate no faults
        pd.DataFrame().to_csv(filename, index=False)

def run():
    today = datetime.utcnow().date()
    start_date = str(today)
    end_date = str(today)
    print(f"Running faulty meters report for {start_date} to {end_date}")
    main(start_date, end_date)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Detect abnormal delta jumps in Ceentiel meters")
    parser.add_argument("--start", required=False, default="2024-06-01T00:00:00.000Z", help="Start date (ISO)")
    parser.add_argument("--end", required=False, default="2024-07-08T00:00:00.000Z", help="End date (ISO)")
    args = parser.parse_args()
    main(args.start, args.end)