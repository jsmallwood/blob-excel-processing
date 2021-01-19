from azure.storage.blob import BlobServiceClient
import settings


def initiateBlobServiceClient():
    url = settings.url
    storageKey = settings.storageKey

    blob_service_client = BlobServiceClient(
        account_url=url,
        credential=storageKey
    )

    return  blob_service_client

def returnContainerClient(blob_service_client,writeContainer):
    return blob_service_client.get_container_client(writeContainer)