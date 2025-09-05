import os
import pandas as pd





DATA_FOLDER_PATH = "sdss_benchmark_dataset" 


BUCKET_NAME = "endurance/sdss_benchmark_dataset"


OUTPUT_FILENAME = "manifest.csv"


def generate_manifest(data_folder, bucket_name, output_file):
    """
    Scans a directory for .fits files and creates a manifest
    file with their corresponding S3 paths.
    """
    print(f"Scanning directory: '{data_folder}' for .fits files...")
    
    
    try:
        all_files = os.listdir(data_folder)
        fits_files = sorted([f for f in all_files if f.endswith('.fits')])
    except FileNotFoundError:
        print(f"Error: Directory not found at '{data_folder}'")
        return

    if not fits_files:
        print("No .fits files were found in the directory.")
        return

    print(f"Found {len(fits_files)} FITS files.")

    
    s3_prefix = f"s3://{bucket_name}/"
    s3_paths = [s3_prefix + filename for filename in fits_files]

    
    df = pd.DataFrame(s3_paths)
    
    
    output_path = os.path.join(data_folder, output_file)

    
    df.to_csv(output_path, header=False, index=False)
    
    print(f"✅ Success! Manifest file created at: '{output_path}'")

if __name__ == "__main__":
    
    
    
    generate_manifest(DATA_FOLDER_PATH, BUCKET_NAME, OUTPUT_FILENAME)