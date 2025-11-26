import os, csv, argparse
from tqdm import tqdm
import extractor_extensions.lookup_extractor as lookup_extractor
import extractor_extensions.athlete_extractor as athlete_extractor
import re
from collections import Counter, defaultdict

METADATA_PATH = "data/metadata.csv"
DEFAULT_OUTPUT_DIR = "extracted"
MIN_LEN = 30

def main(metadata_path,out_dir,min_lenght,debug):
    missing = defaultdict(list) # Zoznam / counter pre chýbajúce zložky/cesty
    bad_statuses = Counter() # Zoznam / counter http status 
    failed_lookup_count = 0 # countery
    malformed_athlete_count = 0

    rows = []
    with open(metadata_path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            http_status = (row.get("status") or "").strip()
            url = (row.get("url") or "").strip()
            path = (row.get("file") or "").strip().replace("\\", "/") 
            doc_id = row.get("doc_id") # toto nepotrebuje check, vždy by malo byť inak je chyba inde

            if http_status != "200": # Ignorujem zly crawl
                bad_statuses[http_status] += 1
                continue
            if not path:
                missing["missing_csv_path"].append(url) # chybajuce path v csv (malokedy ale v niektorych edgecase-och to bolo)
                continue
            if not os.path.exists(path):
                missing["missing_html"].append(path) # chybajuce html (zle ulozene, odstranene)
                continue
            rows.append((doc_id, path, url))

    out_text_dir = os.path.join(out_dir, "text")
    athletes_csv = os.path.join(out_dir, "athletes.csv")
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(out_text_dir, exist_ok=True)

    with open(athletes_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        athlete_extractor.write_header(w)
        for doc_id, path, url in tqdm(rows, desc="Extracting", unit="files"):
            if doc_id == "-1": # Atleti su oznaceni -1
                missing_attributes = athlete_extractor.write_csv(w, path, url)
                if missing_attributes == len(athlete_extractor.ATTR_MAP): malformed_athlete_count += 1 # Žiadny field nebol vyplnený - malformed
            else: # lookup
                file_name = f"{doc_id}.txt"
                output_path = os.path.join(out_text_dir, file_name)
                if not lookup_extractor.write_txt(path, output_path, minlen=min_lenght):
                    failed_lookup_count += 1

    if debug:
        print(f"Missing paths for URL: {len(missing.get('missing_csv_path', []))}", *missing.get('missing_csv_path', []), sep='\n ')
        print(f"Missing HTML files: {len(missing.get('missing_html', []))}", *missing.get('missing_html', []), sep='\n ')
        print(f"Bad statuses: {bad_statuses.total()}", *[f" - {k}: {v}" for k, v in sorted(bad_statuses.most_common())], sep='\n')
        print(f"Malformed athlete entries: {malformed_athlete_count}") 
        print(f"Failed lookup texts: {failed_lookup_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--metadata",
        dest="metadata_path",
        default=METADATA_PATH,
        help=f"Cesta k metadátam (default: {METADATA_PATH})",
    )
    parser.add_argument(
        "--outdir",
        dest="out_dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Cieľový adresár pre extrakciu (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--minlen",
        dest="min_lenght",
        type=int,
        default=MIN_LEN,
        help=f"Minimálna akceptovateľná dĺžka textu (default: {MIN_LEN})",
    )
    parser.add_argument(
        "--debug",
        dest="debug",
        action="store_true",
        help="Výpis štatistík a chýb",
    )
    args = parser.parse_args()
    main(
        metadata_path=args.metadata_path,
        out_dir=args.out_dir,
        min_lenght=args.min_lenght,
        debug=args.debug
    )
