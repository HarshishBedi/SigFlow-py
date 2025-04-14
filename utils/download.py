import os
import gzip
import shutil
from pathlib import Path
from urllib.request import urlretrieve
from urllib.parse import urljoin

def may_be_download(url):
    """Download & unzip ITCH data if not yet available.
    
    - Downloads the file to the data/raw folder.
    - Unzips the file into the data/unzipped folder.
    
    Returns the path to the unzipped file.
    """
    # Define paths for raw and unzipped data
    raw_data_path = Path('data') / 'raw'
    unzipped_data_path = Path('data') / 'unzipped'
    
    # Ensure the directories exist
    if not raw_data_path.exists():
        print('Creating directory:', raw_data_path)
        raw_data_path.mkdir(parents=True, exist_ok=True)
    else:
        print('Raw data directory exists:', raw_data_path)

    if not unzipped_data_path.exists():
        print('Creating directory:', unzipped_data_path)
        unzipped_data_path.mkdir(parents=True, exist_ok=True)
    else:
        print('Unzipped data directory exists:', unzipped_data_path)

    # Determine the target filename for the downloaded file
    filename = raw_data_path / url.split('/')[-1]
    
    # Download the file if it does not exist
    if not filename.exists():
        print('Downloading...', url)
        urlretrieve(url, str(filename))
    else:
        print('File exists:', filename)
    
    # Determine the path for the unzipped file (e.g., change .gz to .bin)
    unzipped = unzipped_data_path / (filename.stem + '.bin')
    
    # Unzip the file if not already unzipped
    if not unzipped.exists():
        print('Unzipping to', unzipped)
        with gzip.open(str(filename), 'rb') as f_in:
            with open(unzipped, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    else:
        print('File already unpacked:', unzipped)
    
    return unzipped

if __name__ == '__main__':
    # Set the HTTPS URL and source file name.
    HTTPS_URL = 'https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/'
    SOURCE_FILE = '10302019.NASDAQ_ITCH50.gz'

    # Download and unzip the file using may_be_download
    file_name = may_be_download(urljoin(HTTPS_URL, SOURCE_FILE))
    date = file_name.name.split('.')[0]
    
    print("Downloaded and unzipped file:", file_name)
    print("Date extracted from file name:", date)