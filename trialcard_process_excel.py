# Load libraries
import pandas as pd
import os
from datetime import datetime
import azure_blob


blob_service_client = azure_blob.initiateBlobServiceClient()

todaysdate = datetime.today().strftime('%m%d%Y')

containerName = 'trialcard'
container_client_read = blob_service_client.get_container_client(containerName)

#create dynamic file paths for blob, excel files are loaded to blob with appended date
fieldforce = f'{todaysdate}Field Force Transaction Report.xlsx'

natestoredemptions = f'{todaysdate}Natesto Redemptions Ad Hoc.xlsx'

try:
    #download blob and read into dataframe
    downloaded_blob = container_client_read.download_blob(fieldforce).content_as_bytes()
    dfFF = pd.read_excel(downloaded_blob, header=2, dtype=str)

    FFcolumns = list(dfFF.columns)

    #get column names containing master
    prescriber = [i for i in FFcolumns if 'Master' in i]

    #remove master and abbreviation from from column names
    noMprescriber = [i[i.find('Master') + 7:] for i in prescriber]
    noMAprescriber = [i.replace("Abbreviation", '').strip() for i in noMprescriber]

    #append column names not captured in lists
    prescriber.append('Pharmacy State Abreviation')
    noMAprescriber.append('Pharmacy State')

    #zip lists together
    torename = dict(zip(prescriber, noMAprescriber))

    #rename dataframe columns to match dfNR column names, needed for concat
    dfFF.rename(columns=torename, inplace=True)

    downloaded_blob = container_client_read.download_blob(natestoredemptions).content_as_bytes()
    dfNR = pd.read_excel(downloaded_blob, header=2, dtype=str)

    #union data frames
    df = pd.concat([dfFF, dfNR])

    #save file locally on vm
    path = f'{todaysdate}TrialcardWeeklyUnion.txt'
    df.to_csv(path, index=False)

    writeContainer = 'trialcard/FieldForceTransactionsProcessed'
    container_client_write = blob_service_client.get_container_client(writeContainer)

    #try to write file to container client, will fail if file already exists, will delete local file if successful
    try:
        with open(path, "rb") as data:
            container_client_write.upload_blob(name=path, data=data)

        if os.path.isfile(path):
            os.remove(path)
        else:
            print("Error: %s file not found" % path)
    except:
        print('Upload or delete failed.')

    #delete source blobs
    container_client_read.delete_blob(fieldforce)
    container_client_read.delete_blob(natestoredemptions)

except:
    print('Blob does not exist')