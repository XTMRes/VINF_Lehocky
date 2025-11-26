import os, json, re, csv
from math import log
from collections import Counter, defaultdict
from glob import glob
from tqdm import tqdm
import argparse
import unicodedata

DEFAULT_IN_PATH = "extracted/text"
DEFAULT_OUT_PATH = "data/index.json"
DEFAULT_IN_ATH_PATH = "extracted/athletes.csv"
DEFAULT_OUT_ATH_PATH = "data/index_athletes.json"
DEFAULT_METADATA_PATH = "data/metadata.csv"
DEFAULT_IDF = ""
EPSILON = 0.01
TOKEN = re.compile(r"\w+", re.UNICODE) # tokeny = slová
STOP_WORDS = set(  #stop wordy https://gist.github.com/sebleier/554280
    ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves",
     "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs",
     "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be",
     "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or",
     "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during",
     "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further",
     "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other",
     "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]
)

def clean(s):
    s = (s or "").casefold() # lower
    normalized = unicodedata.normalize("NFKD", s) # á -> a + ´
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)) # odstránenie ´ znakov

def is_numeric(token):
    return token.isdigit() or token.isnumeric()

def tokenize(text):
    text = clean(text)
    tokens = TOKEN.findall(text) # všetky slová / tokeny
    filtered_tokens = []
    for t in tokens:
        if t in STOP_WORDS: continue # stop slová
        if is_numeric(t): continue # čísla (možno ponechám neskôr)
        if len(t) <= 1: continue # malé tokeny vynechávam
        filtered_tokens.append(t)
    return filtered_tokens

def load_metadata(csv_path):
    rows = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            doc_id = (row.get("doc_id") or "").strip()
            if doc_id != "-1": # atleti
                rows.append(doc_id)
    return rows

def compute_idf(df, N, method = ""):
    idf = {}
    for t, dft in df.items():
        if dft <= 0 or N <= 0:
            idf[t] = 0.0
            continue
        if method == "bm25":
            idf[t] = log((N - dft + 0.5) / (dft + 0.5)) #best matching 25
            idf[t] = max(idf[t], EPSILON)  # minimum (podľa prednášky ošetrené malou hodnotou)
        else:
            idf[t] = log(N / dft) # klasický výpočet
    return idf

def build_athlete_index(csv_path, out_path):
    index = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        total = sum(1 for _ in f) - 1
        f.seek(0)
        reader = csv.DictReader(f)
        for row in tqdm(reader, total=total, mininterval=0.0, desc="Building athlete index"):
            index.append({
                "name": row["name"].strip(),
                "country": row["country"].strip(),
                "sport": row["sport"].strip(),
                "gold": int(row["gold"]),
                "silver": int(row["silver"]),
                "bronze": int(row["bronze"]),
                "url": row["url"].strip()
            })
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

def build_index(in_path,out_path,metadata_csv, idf_method=""):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    docs = load_metadata(metadata_csv)
    postings = defaultdict(dict) # postinng listy
    doc_ids = []
    doc_len = {} 
    df = Counter() 

    for doc_id in tqdm(docs, desc="Building lookup index"):
        path = os.path.join(in_path, f"{doc_id}.txt")
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            tokens = tokenize(f.read())
        if not tokens:
            continue
        doc_ids.append(doc_id)
        doc_len[doc_id] = len(tokens)
        tf = Counter(tokens)

        for term, term_count in tf.items():
            postings[term][doc_id] = term_count
        
        for term in tf.keys():
            df[term] += 1

    N = len(doc_ids)
    idf = compute_idf(df,N, idf_method)
    index = {
        "N": N,
        "df": df,
        "idf": idf,
        "postings": {t: d for t, d in postings.items()},
        "docs": doc_ids,
        "doc_len": doc_len,
        "avgdl": (sum(doc_len.values()) / N),
        "idf_method": idf_method
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2, sort_keys=True)
    print(f"Počet tokenov:{len(df)}")

def main(in_path, in_ath_path, out_ath_path ,out_path, metadata_path, idf_method):
    build_index(in_path, out_path, metadata_path, idf_method=idf_method)
    build_athlete_index(in_ath_path,out_ath_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inpath",
        dest="in_path",
        default=DEFAULT_IN_PATH,
        help=f"Cesta k vstupným textom (default: {DEFAULT_IN_PATH})",
    )
    parser.add_argument(
        "--outpath",
        dest="out_path",
        default=DEFAULT_OUT_PATH,
        help=f"Cieľová cesta pre index (default: {DEFAULT_OUT_PATH})",
    )
    parser.add_argument(
        "--inpath_ath",
        dest="in_ath_path",
        default=DEFAULT_IN_ATH_PATH,
        help=f"Cesta k CSV atlétov (default: {DEFAULT_IN_ATH_PATH})",
    )
    parser.add_argument(
        "--outpath_ath",
        dest="out_ath_path",
        default=DEFAULT_OUT_ATH_PATH,
        help=f"Cieľová cesta pre index (default: {DEFAULT_OUT_ATH_PATH})",
    )
    parser.add_argument(
        "--metadata",
        dest="metadata_path",
        default=DEFAULT_METADATA_PATH,
        help=f"Cesta k metadátam (default: {DEFAULT_METADATA_PATH})",
    )
    parser.add_argument(
        "--idf",
        dest="idf_method",
        default=DEFAULT_IDF,
        choices=["","bm25"],
        help="Metóda výpočtu IDF (default: <blank>)",
    )
    args = parser.parse_args()

    main(
        in_path=args.in_path,
        out_path=args.out_path,
        in_ath_path=args.in_ath_path,
        out_ath_path=args.out_ath_path,
        metadata_path=args.metadata_path,
        idf_method=args.idf_method
    )