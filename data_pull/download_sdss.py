import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import bz2
import time


TARGET_FILE_COUNT = 500
DOWNLOAD_DIRECTORY = "sdss_benchmark_dataset"
S3_BUCKET_PREFIX = "s3://endurance/sdss_benchmark_dataset"
BASE_URL = "https://data.sdss.org/sas/dr17/eboss/photoObj/frames/301/"




def discover_file_urls(base_url, target_count):
    """
    Scrapes the SDSS server to systematically find diverse file URLs.
    """
    print(f"Discovering {target_count} diverse file URLs from the SDSS server...")
    found_urls = []
    
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        
        run_links = [a.get('href') for a in soup.find_all('a') 
                     if a.get('href') and a.get('href').rstrip('/').isdigit()]

        for run in run_links:
            camcol_url = base_url + run
            if not camcol_url.endswith('/'):
                camcol_url += '/'

            camcol_response = requests.get(camcol_url)
            camcol_response.raise_for_status()
            camcol_soup = BeautifulSoup(camcol_response.text, 'html.parser')

            
            camcol_links = [a.get('href') for a in camcol_soup.find_all('a') 
                            if a.get('href') and a.get('href').rstrip('/').isdigit()]

            for camcol in camcol_links:
                field_url = camcol_url + camcol
                if not field_url.endswith('/'):
                    field_url += '/'

                field_response = requests.get(field_url)
                field_response.raise_for_status()
                field_soup = BeautifulSoup(field_response.text, 'html.parser')

                
                file_links = [a.get('href') for a in field_soup.find_all('a')]
                for file in file_links:
                    if file.endswith('.fits.bz2'):
                        found_urls.append(field_url + file)
                        if len(found_urls) >= target_count:
                            print("Discovery complete.")
                            return found_urls
    except requests.RequestException as e:
        print(f"Error during URL discovery: {e}")
        return found_urls
    
    return found_urls




def download_file_stream(url, dest_folder):
    local_filename = url.split('/')[-1]
    full_path = os.path.join(dest_folder, local_filename)

    try:
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            total_size_in_bytes = int(r.headers.get('content-length', 0))
            with open(full_path, 'wb') as f:
                progress_bar = tqdm(
                    total=total_size_in_bytes, 
                    unit='iB',
                    unit_scale=True,
                    desc=f"Downloading {local_filename}"
                )
                for chunk in r.iter_content(chunk_size=8192):
                    progress_bar.update(len(chunk))
                    f.write(chunk)
                progress_bar.close()
        return full_path
    except Exception as e:
        print(f"\nError downloading {url}: {e}")
        return None




def decompress_bz2_file(compressed_path):
    print(f"Decompressing {os.path.basename(compressed_path)}...")
    decompressed_path = compressed_path.replace('.bz2', '')
    
    try:
        with bz2.BZ2File(compressed_path, 'rb') as source, open(decompressed_path, 'wb') as dest:
            dest.write(source.read())

        os.remove(compressed_path)
        print(f"Successfully decompressed to {os.path.basename(decompressed_path)}")
        return decompressed_path
    except Exception as e:
        print(f"\nError decompressing {compressed_path}: {e}")
        if os.path.exists(decompressed_path):
            os.remove(decompressed_path)
        return None




def create_manifest(filenames, dest_folder, s3_prefix):
    s3_paths = [s3_prefix + os.path.basename(f) for f in filenames]
    df = pd.DataFrame(s3_paths)
    manifest_path = os.path.join(dest_folder, 'manifest.csv')
    df.to_csv(manifest_path, header=False, index=False)
    print(f"\nManifest file with {len(s3_paths)} entries created at: {manifest_path}")
    return manifest_path




def main():
    if not os.path.exists(DOWNLOAD_DIRECTORY):
        os.makedirs(DOWNLOAD_DIRECTORY)

    
    urls_to_process = discover_file_urls(BASE_URL, TARGET_FILE_COUNT)
    if not urls_to_process:
        print("Could not find any files. Exiting.")
        return

    
    print("\nPreview of discovered files:")
    for u in urls_to_process[:5]:
        print("  ", u)

    
    final_fits_paths = []
    for url in urls_to_process:
        print("-" * 20)
        compressed_path = download_file_stream(url, DOWNLOAD_DIRECTORY)
        if compressed_path:
            decompressed_path = decompress_bz2_file(compressed_path)
            if decompressed_path:
                final_fits_paths.append(decompressed_path)
        time.sleep(0.1)

    
    if final_fits_paths:
        create_manifest(final_fits_paths, DOWNLOAD_DIRECTORY, S3_BUCKET_PREFIX)
        print("\nDataset preparation complete!")
    else:
        print("\nProcess failed.")

if __name__ == "__main__":
    main()
