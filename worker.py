from pathlib import Path
import boto3
import json
from urllib.parse import quote
from ng_link.exaspim_link import generate_exaspim_link

# =============================================================================
# CONFIGURATION - Modify these variables as needed
# =============================================================================

#ZARR_PATH = "s3://aind-open-data/exaSPIM_3296445_2025-09-08_16-35-50_processed_2025-10-12_08-48-11/flatfield_correction/SPIM.ome.zarr"
ZARR_PATH = "s3://sean-fusion/exaSPIM_720164_2025-07-07_17-55-45_rhapso/fusion/fused.zarr"
VMIN = 0
VMAX = 200
OPACITY = 0.5
BLEND = "default"
OUTPUT_JSON_PATH = "./SF-results"

# =============================================================================
# END CONFIGURATION
# =============================================================================


def upload_to_s3(file_path, bucket_name, s3_file_path):
    """
    Upload a file to an S3 bucket

    :param file_path: File to upload
    :param bucket_name: Bucket to upload to
    :param s3_file_path: S3 object name
    """
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, bucket_name, s3_file_path)
        print(f"File {file_path} uploaded to {bucket_name}/{s3_file_path}")
    except Exception as e:
        print(f"Error uploading file: {e}")

def parse_s3_path(s3_path):
    """
    Parse the S3 path to get the bucket name and the parent directory

    :param s3_path: S3 path (s3://bucket-name/path/to/zarr)
    :return: tuple (bucket_name, parent_directory)
    """
    if s3_path.startswith("s3://"):
        path_parts = s3_path[5:].split("/")
        bucket_name = path_parts[0]
        parent_directory = "/".join(path_parts[1:-1])  # Exclude the zarr file/directory itself
        return bucket_name, parent_directory
    else:
        raise ValueError("Invalid S3 path format")

if __name__ == "__main__":
    print("=" * 80)
    print("Generating ExaSPIM Neuroglancer Link")
    print("=" * 80)
    print(f"Zarr path: {ZARR_PATH}")
    print(f"Intensity range: [{VMIN}, {VMAX}]")
    print(f"Opacity: {OPACITY}")
    print(f"Blend: {BLEND}")
    print(f"Output: {OUTPUT_JSON_PATH}")
    print("=" * 80)
    print()

    # Create output directory if it doesn't exist
    output_dir = Path(OUTPUT_JSON_PATH)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory created/verified: {output_dir.absolute()}")
    print()

    # Determine the S3 bucket and parent directory
    s3_bucket, parent_directory = parse_s3_path(ZARR_PATH)

    # Call the function with the hardcoded values
    # This will print the S3-based URL, which we'll ignore
    generate_exaspim_link(
        None,
        s3_path=ZARR_PATH,
        opacity=OPACITY,
        blend=BLEND,
        output_json_path=OUTPUT_JSON_PATH,
        vmin=VMIN,
        vmax=VMAX,
        dataset_name=parent_directory,
    )

    # Read the generated JSON file
    output_json_file = Path(OUTPUT_JSON_PATH).joinpath("process_output.json")
    
    with open(output_json_file, 'r') as f:
        state_json = json.load(f)
    
    # Create URL-encoded neuroglancer link
    json_string = json.dumps(state_json, separators=(',', ':'))  # Compact JSON
    encoded_state = quote(json_string, safe='')
    encoded_url = f"https://neuroglancer-demo.appspot.com/#!{encoded_state}"
    
    print()
    print("=" * 80)
    print("URL-Encoded Neuroglancer Link:")
    print("=" * 80)
    print(encoded_url)
    print()
    print("=" * 80)
    print(f"✓ JSON also saved locally: {output_json_file.absolute()}")
    print("=" * 80)
    
    # Optional: Uncomment below to upload to S3
    # s3_file_path = f"{parent_directory}/{output_json_file.name}"
    # upload_to_s3(str(output_json_file), s3_bucket, s3_file_path)
