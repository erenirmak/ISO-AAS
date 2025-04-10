import os
import datetime
import requests
from msal import ConfidentialClientApplication
from azure.storage.blob import ContainerClient
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Azure AD Credentials
TENANT_ID = os.getenv('TENANT_ID')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

# AAS and Storage Account Details
AAS_SERVER = os.getenv('AAS_SERVER')
AAS_SERVER_REST_URL = os.getenv('AAS_SERVER_REST_URL')
REGION = os.getenv('AAS_REGION')
AAS_MODELS = os.getenv('AAS_MODELS', '').split(',')
AAS_DATABASE_ID = os.getenv('DATABASE_ID')
STORAGE_ACCOUNT_NAME = os.getenv('STORAGE_ACCOUNT_NAME')
ENDPOINT_SUFFIX= os.getenv('ENDPOINT_SUFFIX')
STORAGE_ACCOUNT_KEY = os.getenv('STORAGE_ACCOUNT_KEY')
STORAGE_CONTAINER_NAME = os.getenv('STORAGE_CONTAINER_NAME')

# Authenticate with Microsoft Entra ID
app = ConfidentialClientApplication(CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}", client_credential=CLIENT_SECRET)
result = app.acquire_token_for_client(scopes=["https://*.asazure.windows.net/.default"])
access_token = result['access_token']

# Backup each database
for model in AAS_MODELS:
    backup_file_name = f"{model}_{datetime.datetime.now().strftime('%Y-%m-%d')}.abf"
    backup_url = f"{AAS_SERVER_REST_URL}/models/{model}/xmla"

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/xml'
    }

    xmla_query = f"""<Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
    <Command>
        <Backup>
            <File>{backup_file_name}</File>
            <AllowOverwrite>true</AllowOverwrite>
            <ApplyCompression>true</ApplyCompression>
        </Backup>
    </Command>
    <Properties>
        <PropertyList>
        <Format>Tabular</Format>
        </PropertyList>
    </Properties>
    </Execute>"""
    response = requests.post(backup_url, headers=headers, data=xmla_query)

    # Check the response
    if response.status_code == 200:
        print("Backup initiated successfully.")
    else:
        print(f"Error: {response.status_code} - {response.text}")
    # try:
    #     response = requests.post(backup_url, headers=headers, json=payload)
    #     if response.status_code == 202:
    #         print(f"Backup initiated for {db}")
    #     else:
    #         print(f"Failed to initiate backup for {db}: {response.text}")
    # except Exception as e:
    #     print(f"Exception occurred while backing up {db}: {e}")

# Delete old backups
# Initialize Blob Service Client
blob_service_client = BlobServiceClient(account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.{ENDPOINT_SUFFIX}", credential=STORAGE_ACCOUNT_KEY)

container_client = blob_service_client.get_container_client(STORAGE_CONTAINER_NAME)
blobs = container_client.list_blobs()
retention_period = 5
for blob in blobs:
    print(blob.name)
    # blob_date_str = blob.name.split('_')[-1].replace('.abf', '')
    # blob_date = datetime.datetime.strptime(blob_date_str, '%Y-%m-%d').date()
    # if (datetime.datetime.now().date() - blob_date).days > retention_period:
    #     blob_client = container_client.get_blob_client(blob)
    #     blob_client.delete_blob()
    #     print(f"Deleted old backup: {blob.name}")