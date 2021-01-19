
# Load libraries
import azure_blob
import pandas as pd
from datetime import datetime, timedelta
import os

blob_service_client = azure_blob.initiateBlobServiceClient()

#file names are received with dynamic date and month
yesterday = datetime.today() - timedelta(days=1)
yesterdaysdate = yesterday.strftime('%m%d%Y')

first = datetime.utcnow().replace(day=1) - timedelta(days=1)
lastMonth = first - timedelta(days=1)
lastmonthabb = lastMonth.strftime("%b").upper()

containerName = 'mckesson'

container_client_read = blob_service_client.get_container_client(containerName)

blobName = f'{yesterdaysdate}3PL Aytu  Sales Registry {lastmonthabb} 2020.xlsx'

try:
    #download blob
    downloaded_blob = container_client_read.download_blob(blobName).content_as_bytes()

    #excel file has multiple sheets and formatting that moves header to 3rd row
    df = pd.read_excel(downloaded_blob,sheet_name='Invoice Detail',header = 3,dtype = str)

    #remove spaces from headers
    df.columns = df.columns.str.replace('\n', ' ')

    #save csv locally on vm
    path = f'{yesterdaysdate}SalesRegistry.txt'
    df.to_csv(path,sep='\t',index=False)

    writeContainer = 'mckesson/SalesRegistryProcessed'
    container_client_write = blob_service_client.get_container_client(writeContainer)

    #try to write file to container client, will fail if file already exists, will delete local file if successful
    try:
        with open(path, "rb") as data:
            container_client_write.upload_blob(name = path, data=data)

        if os.path.isfile(path):
            os.remove(path)
        else:
            print("Error: %s file not found" % path)
    except:
        print('Upload or delete failed.')

    #delete source blob
    container_client_read.delete_blob(blobName)

except:
    print('Blob did not exist')
