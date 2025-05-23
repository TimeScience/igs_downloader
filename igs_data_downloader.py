import argparse
import datetime
import requests
import gzip
import os
import re
import shutil # For decompressing

# --- Configuration (You might adjust these if IGS URL structures change) ---
CDDIS_BASE_URL = "https://cddis.nasa.gov/archive/gnss/data/daily"
MGEX_NAV_BASE_URL = "https://cddis.nasa.gov/archive/gnss/data/campaign/mgex/daily/rinex3"

# --- Helper Functions ---

def get_obs_date_from_rinex(rinex_rover_file):
    """
    Parses the 'TIME OF FIRST OBS' from a RINEX 3.x header.
    Returns a datetime object.
    """
    try:
        with open(rinex_rover_file, 'r') as f:
            for line in f:
                if "TIME OF FIRST OBS" in line:
                    match = re.search(r'(\d{4})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+([\d\.]+)', line)
                    if match:
                        year, month, day, hour, minute, second_full = match.groups()
                        second = int(float(second_full)) # Truncate fractional seconds for datetime object
                        microsecond = int((float(second_full) - second) * 1_000_000)
                        return datetime.datetime(int(year), int(month), int(day),
                                                 int(hour), int(minute), int(second), microsecond,
                                                 tzinfo=datetime.timezone.utc)
                    else:
                        print(f"Could not parse date from 'TIME OF FIRST OBS' line: {line.strip()}")
                        return None
            print("ERROR: 'TIME OF FIRST OBS' line not found in RINEX header.")
            return None
    except FileNotFoundError:
        print(f"ERROR: Rover RINEX file not found: {rinex_rover_file}")
        return None
    except Exception as e:
        print(f"ERROR: Could not read or parse rover RINEX file: {e}")
        return None

def get_doy(dt_object):
    """Converts a datetime object to Day Of Year (DOY)."""
    return dt_object.timetuple().tm_yday

def download_file(url, output_path):
    """Downloads a file from a URL to the specified output path."""
    print(f"Attempting to download: {url}")
    try:
        response = requests.get(url, stream=True, timeout=60) # Added timeout
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Successfully downloaded to: {output_path}")
        return True
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} (URL: {url})")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err} (URL: {url})")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout occurred: {timeout_err} (URL: {url})")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred during request: {req_err} (URL: {url})")
    return False

def decompress_gz_file(gz_file_path, output_file_path):
    """Decompresses a .gz file."""
    print(f"Decompressing: {gz_file_path}")
    try:
        with gzip.open(gz_file_path, 'rb') as f_in:
            with open(output_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Successfully decompressed to: {output_file_path}")
        os.remove(gz_file_path) # Remove the .gz file after decompression
        print(f"Removed compressed file: {gz_file_path}")
        return True
    except FileNotFoundError:
        print(f"ERROR: Compressed file not found for decompression: {gz_file_path}")
    except Exception as e:
        print(f"ERROR: Could not decompress file {gz_file_path}: {e}")
    return False

# --- Main Script Logic ---
def main():
    parser = argparse.ArgumentParser(description="Download IGS base station data for PPK.")
    parser.add_argument("--rinex_rover_file", required=True, help="Path to the rover's RINEX observation file.")
    parser.add_argument("--station_id", required=True, help="4-character IGS station ID (e.g., dhak).")
    parser.add_argument("--output_dir", required=True, help="Directory to save downloaded and decompressed files.")
    # Optional argument to specify RINEX version for OBS file if needed
    parser.add_argument("--rinex_obs_version", default="3", choices=["2", "3"],
                        help="RINEX version for observation file (2 for RINEX 2.11, 3 for RINEX 3.x, default: 3).")

    args = parser.parse_args()

    # Create output directory if it doesn't exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
        print(f"Created output directory: {args.output_dir}")

    # 1. Get observation date from rover RINEX
    obs_datetime = get_obs_date_from_rinex(args.rinex_rover_file)
    if not obs_datetime:
        return

    year = obs_datetime.year
    yy = str(year)[-2:] # 2-digit year
    doy = get_doy(obs_datetime)
    doy_str = f"{doy:03d}" # 3-digit DOY (e.g., 001, 143)
    station_id_lower = args.station_id.lower()

    print(f"\nRover Observation Start: {obs_datetime.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Year: {year}, Day of Year: {doy_str}, Station: {station_id_lower.upper()}")

    # 2. Construct URLs and download Observation (OBS) file
    obs_filename_gz = ""
    obs_final_filename = ""
    obs_url = ""

    if args.rinex_obs_version == "3":
        # Try RINEX 3.x Compact RINEX first (common)
        # Example: https://cddis.nasa.gov/archive/gnss/data/daily/2023/001/23o/ALGO00CAN_R_20230010000_01D_30S_MO.crx.gz
        # The exact daily RINEX 3 OBS filename can vary. This is one common pattern for high-rate or combined files.
        # A more reliable pattern for daily observation data might be station-specific.
        # Example: https://cddis.nasa.gov/archive/gnss/data/daily/2023/001/23o/algo0010.23o.crx.gz (older pattern)
        # For this script, we'll try a common pattern, but this might need adjustment.
        # Using a pattern that's often available: ssssDDD0.YYo.crx.gz
        obs_filename_stem = f"{station_id_lower}{doy_str}0.{yy}o"
        obs_filename_crx_gz = f"{obs_filename_stem}.crx.gz"
        obs_url = f"{CDDIS_BASE_URL}/{year}/{doy_str}/{yy}o/{obs_filename_crx_gz}"
        obs_filename_gz = obs_filename_crx_gz
        obs_final_filename = f"{obs_filename_stem}.crx"
    elif args.rinex_obs_version == "2":
        # RINEX 2.11
        obs_filename_stem = f"{station_id_lower}{doy_str}0.{yy}d" # e.g., dhak1430.25d
        obs_filename_211_gz = f"{obs_filename_stem}.Z" # Often .Z for RINEX 2
        obs_url = f"{CDDIS_BASE_URL}/{year}/{doy_str}/{yy}d/{obs_filename_211_gz}"
        print(f"Warning: RINEX 2.11 uses .Z compression. This script handles .gz. Manual decompression of {obs_filename_211_gz} might be needed if download succeeds.")
        obs_filename_gz = obs_filename_211_gz # Placeholder, decompression won't work if it's .Z
        obs_final_filename = obs_filename_stem


    obs_dl_path_gz = os.path.join(args.output_dir, obs_filename_gz)
    obs_dl_path_final = os.path.join(args.output_dir, obs_final_filename)

    if download_file(obs_url, obs_dl_path_gz):
        if args.rinex_obs_version == "3" and obs_filename_gz.endswith(".crx.gz"): # Only decompress if it's .gz
            decompress_gz_file(obs_dl_path_gz, obs_dl_path_final)
        elif args.rinex_obs_version == "2" and obs_filename_gz.endswith(".Z"):
             print(f"Downloaded {obs_dl_path_gz}. Please decompress it manually (e.g., using 'uncompress' or 7-Zip).")
        else:
            print(f"Downloaded {obs_dl_path_gz}. Not attempting automated decompression for this extension.")
    else:
        print(f"Failed to download OBS file. Please check URL or try another IGS source if CDDIS is down.")


    # 3. Construct URL and download Navigation (NAV) file (Multi-GNSS Broadcast Ephemeris)
    # Common pattern: BRDM00DLR_S_YYYYDDD0000_01D_MN.rnx.gz
    # Some MGEX files might use BRDM00GDE_S... or other analysis center codes. DLR is common.
    nav_filename_stem = f"BRDM00DLR_S_{year}{doy_str}0000_01D_MN"
    nav_filename_rnx_gz = f"{nav_filename_stem}.rnx.gz" # RINEX 3 NAV
    nav_url = f"{MGEX_NAV_BASE_URL}/{year}/{doy_str}/{nav_filename_rnx_gz}"

    # Fallback if DLR is not found, try GDE (another common AC for MGEX)
    nav_filename_stem_gde = f"BRDM00GDE_S_{year}{doy_str}0000_01D_MN"
    nav_filename_rnx_gz_gde = f"{nav_filename_stem_gde}.rnx.gz"
    nav_url_gde = f"{MGEX_NAV_BASE_URL}/{year}/{doy_str}/{nav_filename_rnx_gz_gde}"


    nav_dl_path_gz = os.path.join(args.output_dir, nav_filename_rnx_gz)
    nav_dl_path_final = os.path.join(args.output_dir, f"{nav_filename_stem}.rnx")

    if not download_file(nav_url, nav_dl_path_gz):
        print(f"Failed to download NAV file with DLR. Trying GDE...")
        nav_dl_path_gz = os.path.join(args.output_dir, nav_filename_rnx_gz_gde)
        nav_dl_path_final = os.path.join(args.output_dir, f"{nav_filename_stem_gde}.rnx")
        if download_file(nav_url_gde, nav_dl_path_gz):
            decompress_gz_file(nav_dl_path_gz, nav_dl_path_final)
        else:
            print(f"Failed to download NAV file with GDE as well. Please check MGEX directory for available BRDM files for {year}/{doy_str} or try a different IGS source.")
    else:
        decompress_gz_file(nav_dl_path_gz, nav_dl_path_final)

    print("\n--- Download process finished. ---")
    print(f"Please check the '{args.output_dir}' directory for downloaded and decompressed files.")
    print("Required files for RTKPOST (typically):")
    print(f"  - Rover OBS: {args.rinex_rover_file}")
    print(f"  - Base OBS:  {obs_dl_path_final if os.path.exists(obs_dl_path_final) else 'Download/Decompress OBS manually if failed'}")
    print(f"  - NAV file:  {nav_dl_path_final if os.path.exists(nav_dl_path_final) else 'Download/Decompress NAV manually if failed'}")

if __name__ == "__main__":
    main()