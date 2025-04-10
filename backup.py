import os
import json
import time
import requests
from dotenv import load_dotenv
from utils.auth import acquire_token
import datetime
from azure.storage.blob import ContainerClient

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

def backup_aas_model():
    load_dotenv()

    token = acquire_token()

    region = os.environ["AAS_REGION"]
    server_name = os.environ["AAS_SERVER"]
    model_name = os.environ["AAS_MODEL"]
    container = os.environ["BACKUP_CONTAINER"]
    blob = os.environ["BACKUP_BLOB"]

    url = f"https://{region}.asazure.windows.net/servers/{server_name}/models/{model_name}/backup"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "backupFile": {
            "container": container,
            "blob": blob
        },
        "type": "abf"
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            if response.status_code == 202:
                print(f"[SUCCESS] Backup started for model '{model_name}'")
                return
            else:
                print(f"[ERROR] Backup failed (Attempt {attempt}): {response.text}")
        except Exception as e:
            print(f"[EXCEPTION] {e} (Attempt {attempt})")
        
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)

    raise RuntimeError("Exceeded maximum retries for AAS backup.")

def change_backup_names():
    # Date Settings
    date_format = "%d-%m-%Y"

    today = datetime.datetime.today().date()
    today_format = today.strftime(date_format)
    ## Date difference to delete old backups
    five_days_ago = today - datetime.timedelta(days=5)
    
    # Azure Blob Storage Connection Credentials & Container   
    storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=BLOB_ACCOUNT_NAME;AccountKey=ACCOUNT_KEY;EndpointSuffix=ENDPOINT_SUFFIX"
    container_name = "BACKUP_CONTAINER"
    # Azure Blob Storage Connection to Container
    azure_blob_container_client = ContainerClient.from_connection_string(storage_connection_string, container_name=container_name)
    # List of Files in the Container
    blobs = azure_blob_container_client.list_blobs()

    for blob in blobs: # iterate through files in the container
        print("Processing: ", blob['name'])
        if "_" in blob['name']:
            # Date Extraction from Backup File Name
            file_date = blob['name'].replace(".abf", "").split('_')[1]
            blob_date_extraction = datetime.datetime.strptime(file_date, date_format)
            blob_date_extraction = blob_date_extraction.date()
            # Check for Older Backups
            if blob_date_extraction < five_days_ago:
                print("Date condition checked and approved!")
                blob_client = azure_blob_container_client.get_blob_client(blob)
                blob_client.delete_blob()
                print("Deletion successful!")
            else:
                print("Date condition checked and rejected! Keep the backup file!")
        else:
            pass

if __name__ == "__main__":
    backup_aas_model()
    change_backup_names()
    print("Backup process completed successfully.")