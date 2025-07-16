import requests
import pandas as pd
from datetime import datetime
import argparse
import time
import os
import pickle
from tqdm import tqdm
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Ceentiel credentials
CLIENT_ID = "e394927c-1f79-4734-9e4a-a654eca6aa68"
CLIENT_SECRET = "FS0O3Chf&fd+i5g2"
BASE_API = "https://api.c3ntinel.com/2"

def get_token():
    url = "https://auth.c3ntinel.com/sso/oauth/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    r = requests.post(url, data=payload, headers=headers)
    r.raise_for_status()
    return r.json()["access_token"]

def get_meters(token, query="is:cumulative"):
    url = f"{BASE_API}/meter/search"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"query": query}
    r = requests.get(url, headers=headers, params=params)
    r.raise_for_status()
    return r.json()["_embedded"]["meters"]

def get_meter_readings(token, meter_id, start_date=None, end_date=None):
    url = f"{BASE_API}/meter/{meter_id}/readings"
    headers = {"Authorization": f"Bearer {token}"}
    params = {}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    r = requests.get(url, headers=headers, params=params)
    return r.json() if r.status_code == 200 else {}

def get_meter_properties(token, meter_id):
    url = f"{BASE_API}/meter/{meter_id}/properties/current"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers)
    return r.json() if r.status_code == 200 else {}

def get_site_info(token, site_id):
    url = f"{BASE_API}/site/{site_id}"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers)
    return r.json() if r.status_code == 200 else {}

def get_temperature_data(token, import_code, start_date, end_date):
    url = f"{BASE_API}/rawdata"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "import_code": import_code,
        "start_date": start_date,
        "end_date": end_date
    }
    r = requests.get(url, headers=headers, params=params)
    if r.status_code != 200:
        print(f"⚠️ Could not get raw temperature data for import code {import_code}, status {r.status_code}")
        return {}
    data = r.json()
    temps_by_date = {}
    for reading in data.get("readings", []):
        ts = reading.get("time")
        if ts is None:
            continue
        dt = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
        temps_by_date[dt] = reading.get("value")
    return temps_by_date

def upload_to_drive(filename, drive_filename="latest_ceentiel_report.csv", folder_id="1pZBBKGMxyk5-QEH3ef4QwkuXFx8H3vF6"):
    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    creds = None

    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials=creds)
    query = f"name = '{drive_filename}' and '{folder_id}' in parents and trashed = false"
    response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = response.get('files', [])
    media = MediaFileUpload(filename, mimetype='text/csv')

    if files:
        file_id = files[0]['id']
        updated_file = service.files().update(fileId=file_id, media_body=media).execute()
        print(f"📤 Updated existing file in Google Drive folder: {drive_filename}")
        print(f"🔗 View File: https://drive.google.com/file/d/{file_id}/view")
    else:
        file_metadata = {'name': drive_filename, 'parents': [folder_id]}
        new_file = service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
        print(f"📤 Uploaded new file to Google Drive folder: {drive_filename}")
        print(f"🔗 View File: {new_file.get('webViewLink')}")

def main(start_date, end_date):
    token = get_token()
    print("✅ Authenticated")

    meters = get_meters(token)
    print(f"✅ Found {len(meters)} meters")

    all_readings = []

    for meter in tqdm(meters, desc="Fetching meter data"):
        meter_id = meter.get("meterId")
        site_id = meter.get("siteId")
        meter_name = meter.get("name")

        meter_props = get_meter_properties(token, meter_id)
        site_info = get_site_info(token, site_id)

        import_code = meter_props.get("importCode") if meter_props else None
        temperature_map = get_temperature_data(token, import_code, start_date, end_date) if import_code else {}

        readings_resp = get_meter_readings(token, meter_id, start_date, end_date)
        if "readings" in readings_resp:
            readings = readings_resp["readings"]
            valid_readings = [r for r in readings if r.get("value") is not None]

            for r in valid_readings:
                raw_date = r.get("date")
                raw_time = r.get("time") or r.get("timestamp")
                dt_obj = None
                try:
                    if raw_date:
                        dt_obj = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                    elif isinstance(raw_time, (int, float)):
                        dt_obj = datetime.utcfromtimestamp(raw_time / 1000)
                except Exception as e:
                    print(f"⚠️ Timestamp parse error for meter {meter_name}: {e}")
                r["date"] = dt_obj.strftime("%Y-%m-%d %H:%M:%S") if dt_obj else None
                r.pop("time", None)
                r.pop("timestamp", None)

                reading_day = r["date"][:10] if r["date"] else None
                temp = temperature_map.get(reading_day)
                mdt = temp
                cdd = max(0, temp - 18) if temp is not None else None

                r["meter_id"] = meter_id
                r["meter_name"] = meter_name
                r["site_id"] = site_id
                r["meter_properties"] = meter_props
                r["site_info"] = site_info
                r["mdt"] = mdt
                r["cdd"] = cdd

                all_readings.append(r)

        time.sleep(0.5)

    if all_readings:
        df = pd.json_normalize(all_readings)

        # Save CSV inside 'public' folder
        output_dir = "public"
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, "latest_ceentiel_report.csv")
        df.to_csv(filename, index=False)
        print(f"✅ Saved {len(df)} rows to {filename}")

        upload_to_drive(filename, drive_filename="latest_ceentiel_report.csv")
    else:
        print("⚠️ No data to save")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ceentiel Meter Reading Automation with MDT/CDD")
    parser.add_argument("--start", required=False, default="2024-06-01T00:00:00.000Z", help="Start date in ISO format")
    parser.add_argument("--end", required=False, default="2025-06-01T00:00:00.000Z", help="End date in ISO format")
    args = parser.parse_args()

    main(args.start, args.end)
