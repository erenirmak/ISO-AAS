import os
from msal import ConfidentialClientApplication

def acquire_token():
    tenant_id = os.environ["TENANT_ID"]
    client_id = os.environ["CLIENT_ID"]
    client_secret = os.environ["CLIENT_SECRET"]

    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scopes = ["https://*.asazure.windows.net/.default"]

    app = ConfidentialClientApplication(
        client_id=client_id,
        authority=authority,
        client_credential=client_secret
    )

    result = app.acquire_token_for_client(scopes=scopes)

    if "access_token" not in result:
        raise RuntimeError(f"Token acquisition failed: {result.get('error_description')}")

    return result["access_token"]