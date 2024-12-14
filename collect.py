import json
import tarfile
import os
import time
import bz2
import multiprocessing as mp
import urllib.request
import tqdm
import shutil
from datetime import datetime
import pandas as pd
import requests
import urllib
import io
import uuid
from google.cloud import storage

from bs4 import BeautifulSoup

MAX_PROCESSES = 8

#Google Cloud Configuration Options
GCLOUD_BUCKET_NAME = "..."

# Define a list of (year, month) tuples to iterate over
DATES = [ (2019, 1), (2019, 2), (2019, 3), (2019, 4), (2019, 5), (2019, 6), (2019, 7), (2019, 8), (2019, 9), (2019, 10), (2019, 11), (2019, 12), (2020, 1), (2020, 2), (2020, 3), (2020, 4), (2020, 5), (2020, 6), (2020, 7)]



"""
Convert a single tweet dictionary into a Pandas Series with selected fields.

Args:
    dict (dict): Dictionary representing a tweet JSON object.

Returns:
    pd.Series: Pandas Series containing extracted date/time fields, text, coordinates, and language.
"""

def process_dict(dict):
    date = datetime.strptime(dict['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

    row = pd.Series({
        'year': date.year,
        'month': date.month,
        'day': date.day,
        'hour': date.hour,
        'text': dict['text'],
        'lat': dict['geo']['coordinates'][0],
        'long': dict['geo']['coordinates'][1],
        'lang': dict['lang']
    })
    return row


"""
Class representing metadata about a tar file:
- URL to the file
- File size (in bytes)
- Year and month the tar file corresponds to
- Local path where the file would be stored or processed
"""
class TarData:
    def __init__(self, url, size, month, year):
        self.url = url
        self.size = size
        self.name = url.split('/')[-1]
        self.path = f'./{self.name}'
        self.month = month
        self.year = year

"""
Construct the base URL for the Internet Archive pages hosting Twitter stream tar files.
    
Args:
    year (int): Year of the archive (e.g., 2017).
    month (int): Month of the archive (1-12).
    
Returns:
    str: The base URL where the tar files for the specified year and month are listed.
"""

def get_url_head(year, month):
    return "https://ia800506.us.archive.org/34/items/archiveteam-twitter-stream-{}-{:02d}/".format(year, month)

"""
For a given (year, month) tuple, retrieve metadata about all tar files
stored in the Internet Archive for that month's Twitter stream.

Args:
    year_month (tuple): A tuple in the form (year, month).

Returns:
    list: A list of TarData objects containing metadata about the tar files.
"""
def get_tardata(year_month):
    year, month = year_month
    head = get_url_head(year, month)
    xml_url = head + "archiveteam-twitter-stream-{}-{:02d}_files.xml".format(year, month)
    xml = requests.get(xml_url)
    bs = BeautifulSoup(xml.text, features="xml")
    tars = []
    files = bs.find_all("file")
    for file in files:
        
        if file["name"].endswith(".tar"):
            size = int(file.find('size').contents[0])
            tars.append(TarData(head + file["name"], size, month, year))
    return tars


CHUNK_SIZE = 20* 1024 * 1024

"""
Process a single member (file) in the tar archive. Typically these are .bz2 compressed files
containing tweets in JSON format. 

Args:
    member (TarInfo): The TarInfo object representing a file within the tar archive.
    tar (TarFile): The open tarfile instance from which to extract the member.

Returns:
    pd.Series or None: If the file is a .bz2 JSON data file with geolocation info, return the processed row; else None.
"""


def process_tarmember(member, tar):
    try:
        if(not member.name.endswith(".bz2")):
            return
        with bz2.BZ2File(tar.extractfile(member), 'r') as f:
            for line in f:
                dict = json.loads(line)
                if('delete' in dict.keys()):
                    continue
                if('geo' in dict.keys()):
                    if(dict['geo'] != None):
                        return process_dict(dict)
                        
    except:
        print(f"{member.name} failed")


"""
Given a TarData object containing the URL of the tar file and associated metadata,
stream the tar file from the URL, process each compressed .bz2 member, and accumulate the results
in a DataFrame. 

Args:
    tardata (TarData): Metadata and URL for the tar file to be processed.

Returns:
    pd.DataFrame: DataFrame containing geolocation tweets from the tar file.
"""
def process_tar_url(tardata):
    try:
        df = pd.DataFrame(columns=['year', 'month', 'day', 'hour', 'text', 'lat', 'long', 'lang'])

        url = tardata.url
        name = tardata.name
        size = tardata.size
        year = tardata.year
        month = tardata.month
        
        response = urllib.request.urlopen(url)
        with tarfile.open(fileobj=response, mode= "r|") as tar:
            count = 0
            now = time.time()
            for member in tar:
                df = df.append(process_tarmember(member, tar), ignore_index=True)
                if(count % 20 == 0):
                    after = time.time()
                    dt = after - now
                    now = after
                    print(f"{((20*MAX_PROCESSES)/dt)} files/s")
                count += 1
                
        
        print(f"{name} is finished")
        return df
    except Exception as e:
        print(f"{name} failed:\n{e}")
            

if __name__ == "__main__":
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCLOUD_BUCKET_NAME)

    testblob1 = bucket.blob("./requirements.txt")
    testblob1.upload_from_filename("./requirements.txt")

    testblob2 = bucket.blob("./requirements.txt")
    testblob2.upload_from_filename("./requirements.txt")

    finished_tars = []
    for year, month in DATES:
        if os.path.exists(f'{year}-{month}'):
            finished_tars.extend([f'{year}-{month}/{tar}' for tar in os.listdir(f'{year}-{month}') if tar.endswith('.csv')])

    tardatas = []

    print("Getting TarInfos...")
    with mp.Pool(MAX_PROCESSES) as p:
        for result in p.imap_unordered(get_tardata, DATES):
            tardatas.extend(result)

        p.close()
        p.join()

    print("Recieved")

    tardatas = [tardata for tardata in tardatas if f'{tardata.year}-{tardata.month}/{tardata.name}.csv' not in finished_tars]
    free_pos = list(range(MAX_PROCESSES))

    df = pd.DataFrame(columns=['year', 'month', 'day', 'hour', 'text', 'lat', 'long', 'lang'])

    with mp.Pool(MAX_PROCESSES) as p :
        sub_dfs = p.map(process_tar_url, tardatas)
        for sub_df in sub_dfs:
            df = pd.concat([df, sub_df], ignore_index=True)

    df.to_csv("geodata.csv", index=False)

    blob = bucket.blob("geodata.csv")
    blob.upload_from_filename("geodata.csv")