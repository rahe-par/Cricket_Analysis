import os
import requests
import zipfile
import io
import json
import yaml
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# Base download folder
DOWNLOAD_DIR = "cricsheet_data"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

FORMAT_MAP = {
    "tests.zip": "tests",
    "odis.zip": "odis",
    "t20s.zip": "t20s"
}

def selenium_download():
    """Download all formats (Tests, ODIs, T20s) using Selenium"""
    print("Attempting Selenium download...")
    try:
        chrome_options = Options()
        chrome_options.add_argument("--start-maximized")
        service = Service(r'C:\Users\rahen\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe')  # UPDATE PATH
        driver = webdriver.Chrome(service=service, options=chrome_options)

        driver.get("https://cricsheet.org/downloads/")
        print("Opened Cricsheet downloads page")

        for zip_name, folder_name in FORMAT_MAP.items():
            try:
                link = driver.find_element(By.XPATH, f"//a[contains(@href, '{zip_name}')]")
                zip_url = link.get_attribute("href")
                print(f"Found ZIP URL for {zip_name}: {zip_url}")
                download_zip(zip_url, folder_name)
            except Exception as e:
                print(f"Could not find {zip_name} link: {e}")

    except Exception as e:
        print(f"Selenium failed: {str(e)}")
        print("Falling back to direct download...")
        direct_download()
    finally:
        if 'driver' in locals():
            driver.quit()

def direct_download():
    """Fallback: Direct download"""
    print("Starting direct download...")
    for zip_name, folder_name in FORMAT_MAP.items():
        url = f"https://cricsheet.org/downloads/{zip_name}"
        download_zip(url, folder_name)

def download_zip(url, folder_name):
    """Download and extract a ZIP file to the format's folder"""
    folder_path = os.path.join(DOWNLOAD_DIR, folder_name)
    os.makedirs(folder_path, exist_ok=True)

    try:
        print(f"Downloading {url}...")
        response = requests.get(url)
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            zip_ref.extractall(folder_path)
        print(f"✓ Extracted {url} to {folder_path}")
        convert_yaml_to_json(folder_path)
    except Exception as e:
        print(f"✗ Failed {url}: {str(e)}")

def convert_yaml_to_json(directory):
    """Convert all .yaml files in directory to .json and delete original YAML"""
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".yaml") or file.endswith(".yml"):
                yaml_path = os.path.join(root, file)
                json_path = os.path.splitext(yaml_path)[0] + ".json"
                try:
                    with open(yaml_path, "r", encoding="utf-8") as f:
                        yaml_data = yaml.safe_load(f)
                    with open(json_path, "w", encoding="utf-8") as f:
                        json.dump(yaml_data, f, indent=4)
                    os.remove(yaml_path)  # Delete YAML after conversion
                    print(f"Converted and removed {file}")
                except Exception as e:
                    print(f"Failed to convert {file}: {e}")

if __name__ == "__main__":
    selenium_download()
    print("\nFinal JSON files by format:")
    for fmt in FORMAT_MAP.values():
        fmt_path = os.path.join(DOWNLOAD_DIR, fmt)
        json_files = [f for f in os.listdir(fmt_path) if f.endswith(".json")]
        print(f"{fmt}: {len(json_files)} JSON files")
