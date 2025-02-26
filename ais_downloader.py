import os
import requests
import zipfile
from bs4 import BeautifulSoup
from google.cloud import storage
from tqdm import tqdm
import multiprocessing
import argparse

# NOAA AIS Data URL Format
BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{}/"

# Google Cloud Storage (GCS) Settings
BUCKET_NAME = "ais-noaa-data"
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

# Processed files log
PROCESSED_FILES_LOG = "processed_files.txt"

# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--year_start", type=int, required=True, help="Start year for processing")
parser.add_argument("--year_end", type=int, required=True, help="End year for processing")
parser.add_argument("--resume_from", type=str, default=None, help="File to resume from within a year")
args = parser.parse_args()

YEAR_START = args.year_start
YEAR_END = args.year_end
RESUME_FROM = args.resume_from


def list_files(year):
    """Scrape NOAA website to get all ZIP file URLs for a given year."""
    url = BASE_URL.format(year)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    file_urls = [url + a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.zip')]
    
    # Resume processing from a specific file
    if RESUME_FROM and str(year) in RESUME_FROM:
        print(f"🔄 Resuming {year} from {RESUME_FROM}...")
        file_urls = [f for f in file_urls if f.split("/")[-1] > RESUME_FROM]
    
    return file_urls


def is_file_processed(file_name):
    """Check if a file has already been processed."""
    if os.path.exists(PROCESSED_FILES_LOG):
        with open(PROCESSED_FILES_LOG, "r") as f:
            processed_files = set(f.read().splitlines())
        return file_name in processed_files
    return False


def mark_file_processed(file_name):
    """Log the file as processed to avoid reprocessing it."""
    with open(PROCESSED_FILES_LOG, "a") as f:
        f.write(file_name + "\n")


def download_file(url, save_path):
    """Download a file with a progress bar."""
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))

    with open(save_path, "wb") as file, tqdm(
        desc=save_path, total=total_size, unit="B", unit_scale=True, unit_divisor=1024
    ) as bar:
        for data in response.iter_content(chunk_size=1024):
            file.write(data)
            bar.update(len(data))


def unzip_file(zip_path, extract_folder):
    """Extract ZIP file contents."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)


def upload_to_gcs(local_path, gcs_path):
    """Upload a file to Google Cloud Storage."""
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded: {gcs_path}")


def process_file(file_url, year):
    """Download, extract, and upload a single AIS ZIP file."""
    zip_name = file_url.split("/")[-1]  # Extract filename from URL

    if is_file_processed(zip_name):
        print(f"⏭️ Skipping {zip_name}, already processed.")
        return

    local_zip_path = f"./{zip_name}"
    extract_folder = f"./unzipped/{year}/"
    os.makedirs(extract_folder, exist_ok=True)

    try:
        # Download and unzip file
        print(f"📥 Downloading {zip_name}...")
        download_file(file_url, local_zip_path)
        unzip_file(local_zip_path, extract_folder)

        # Upload extracted CSVs to GCS
        for csv_file in os.listdir(extract_folder):
            local_csv_path = os.path.join(extract_folder, csv_file)
            gcs_csv_path = f"raw_data/{year}/{csv_file}"
            upload_to_gcs(local_csv_path, gcs_csv_path)

        # Mark file as processed
        mark_file_processed(zip_name)

    except Exception as e:
        print(f"❌ Error processing {zip_name}: {e}")

    finally:
        # Clean up local files
        if os.path.exists(local_zip_path):
            os.remove(local_zip_path)
        for file in os.listdir(extract_folder):
            os.remove(os.path.join(extract_folder, file))
        os.rmdir(extract_folder)


def process_year(year):
    """Process all AIS data for a given year."""
    print(f"🚀 Processing {year}...")
    file_urls = list_files(year)

    # Use multiprocessing to download files in parallel
    with multiprocessing.Pool(processes=4) as pool:  
        pool.starmap(process_file, [(file_url, year) for file_url in file_urls])


if __name__ == "__main__":
    for year in range(YEAR_START, YEAR_END + 1):
        process_year(year)
    print("🎉 All NOAA AIS data has been extracted and uploaded to Google Cloud Storage!")
