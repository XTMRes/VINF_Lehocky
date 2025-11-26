import argparse
from urllib.parse import urlparse
from scraper_extensions.browser import Browser
from scraper_extensions.url_manager import URLManager
import os
import asyncio
import shutil
from tqdm import tqdm
import random
import csv
from datetime import date
import hashlib, struct

DEFAULT_OUTPUT_DIR = "./html"
DEFAULT_JSON_DIR = "data/urls.json"
DEFAULT_CSV_DIR = "data/metadata.csv"
DEFAULT_ROOT_URL = "https://olympics.com/en/athletes"
DEFAULT_MAX_PAGES = 500
SLEEP_TIMEOUT = 0.5

def convert_filename(url):
    p = urlparse(url)
    base = ((p.path.strip("/") or "index")[3:]).replace("/", "_")
    return base + ".html"

def doc_id_hash(url): # Hashovanie url pre doc id
    h = hashlib.sha1(url.encode("utf-8")).digest()[:8]  # 64-bit
    return struct.unpack(">Q", h)[0]

def write_file(path, data):
    with open(path, "w", encoding="utf-8") as f:
        f.write(data)
def write_metadata(csv_path, url, doc_id, status = "", file_path = ""):
    norm = file_path.replace("\\", "/").lstrip("./")
    with open(csv_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([doc_id, status, norm, date.today().isoformat(), url])

def reset(output_dir,json_output_path,csv_path):
    # Mazanie HTML / assets
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir, ignore_errors=True)
    # Mazanie JSON
    try:
        os.remove(json_output_path)
    except FileNotFoundError:
        pass
    try:
        os.remove(csv_path)
    except FileNotFoundError:
        pass


def create_write_files(json_output_path,csv_path):
    os.makedirs(os.path.dirname(json_output_path), exist_ok=True)
    csv_exists = os.path.exists(csv_path)
    if not csv_exists:
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        with open(csv_path, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["doc_id","status", "file", "date", "url"])


# Stránka vracia iba prázdne html so scriptmi, pre rendering bude použitý async playwright browser z vlastnej classy Browser
async def main(root_url, output_dir, json_output_path, csv_path ,max_pages, do_reset):
    if do_reset:
        print("Wiping existing outputs…")
        reset(output_dir, json_output_path, csv_path)

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "athletes"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "lookup"), exist_ok=True)

    manager = URLManager(root_url,json_output_path)
    manager.load()
    create_write_files(json_output_path,csv_path)
    processed = 0
    async with Browser() as b:
        with tqdm(total=max_pages, desc="Scraping URLs", unit="page") as pbar:
            while processed < max_pages:
                url, save = manager.get()

                if not url:
                    print("Queue empty.")
                    break
                doc_id = doc_id_hash(url)
                try:
                    html, hrefs, status = await b.fetch(url)
                    if status == 403:
                        pbar.write(f"[{status}] {url}")
                        await asyncio.to_thread(write_metadata, csv_path, url, "-1", status, "")
                        break # Stop if backed off
                    if status != 200:
                        pbar.write(f"[{status}] {url}")
                        await asyncio.to_thread(write_metadata, csv_path, url, "-1", status, "")
                        await asyncio.sleep(0.5 + random.uniform(0.1, 0.4))
                        continue # Skip pre iné ako 200
                except Exception as e:
                    print(f"Url error: {url}\n{e}")
                    await asyncio.to_thread(write_metadata, csv_path, url, "-1", status="ERR", file_path="")
                    continue

                file_path = ""
                manager.add(hrefs)   
                is_athlete_root = url.rstrip("/").endswith("/en/athletes")

                if save and not is_athlete_root:
                    subdir = "athletes"
                    doc_id = "-1" # Atletovia nepotrebuju doc id
                else:
                    subdir = "lookup"

                if subdir:
                    path = os.path.join(output_dir, subdir, convert_filename(url))
                    await asyncio.to_thread(write_file, path, html)
                    file_path = path

                if file_path == "": # Ak sa neuloží file, netreba doc id
                    doc_id = "-1"

                await asyncio.to_thread(write_metadata, csv_path, url, doc_id, status, file_path)
                
                processed += 1
                pbar.update(1)
                await asyncio.sleep(0.5 + random.uniform(0.1, 0.4))

    manager.serialize(override=True) # Forced serializácia pred ukončením

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        dest="output_dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Cieľový adresár pre HTML (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--url",
        dest="root_url",
        default=DEFAULT_ROOT_URL,
        help=f"ROOT URL (default: {DEFAULT_ROOT_URL})",
    )
    parser.add_argument(
        "--json",
        dest="json_output_path",
        default=DEFAULT_JSON_DIR,
        help=f"Cesta k JSON pre serializáciu (default: {DEFAULT_JSON_DIR})",
    )
    parser.add_argument(
        "--csv",
        dest="csv_path",
        default=DEFAULT_CSV_DIR ,
        help=f"Cesta k csv pre serializáciu (default: {DEFAULT_CSV_DIR})",
    )
    parser.add_argument(
        "--max-pages",
        dest="max_pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=f"Maximálny počet spracovaných stránok (default: {DEFAULT_MAX_PAGES})",
    )
    parser.add_argument(
        "--reset",
        dest="do_reset",
        action="store_true",
        help="Pred spustením zmaže výstupný adresár a JSON výstup",
    )

    args = parser.parse_args()
    asyncio.run(
        main(
            root_url=args.root_url,
            output_dir=args.output_dir,
            json_output_path=args.json_output_path,
            csv_path=args.csv_path,
            max_pages=args.max_pages,
            do_reset=args.do_reset,
        )
    )