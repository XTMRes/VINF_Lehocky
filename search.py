import json, argparse, csv, re
from collections import Counter
import unicodedata
import re, shlex, operator
import time
TOKEN = re.compile(r"\w+", re.UNICODE)
OPS = {">": operator.gt, ">=": operator.ge, "<": operator.lt, "<=": operator.le, "=": operator.eq, "==": operator.eq}

def clean(s):
    s = (s or "").casefold() # lower
    normalized = unicodedata.normalize("NFKD", s) # á -> a + ´
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)) # odstránenie ´ znakov

def tokenize(s):
    return TOKEN.findall(clean(s))

# Mapovanie z metadát
# URL Nie je ukladaná v indexe aby bolo šetrené miesto
def load_metadata_urls(metadata_path):
    docid_to_url = {} # url <-> docid
    with open(metadata_path, "r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            doc_id = (row.get("doc_id") or "").strip()
            url = (row.get("url") or "").strip()
            file_path = (row.get("file") or "").strip().replace("\\", "/")
            status = (row.get("status") or "").strip()
            if status != "200": # Aj tu ignorujem zly crawl
                continue
            if doc_id and doc_id != "-1": # Non-athlete zaznamy majú doc id != -1
                docid_to_url[doc_id] = url
    return docid_to_url

# Atléti - rekonštrukcia athlete entít
def load_athletes(athletes_path):
    athletes = []  # [{name, url, file, country, ...}]
    with open(athletes_path, "r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            file_path = (row.get("file") or "").strip().replace("\\", "/")
            name = (row.get("name") or "").strip()
            url = (row.get("url") or "").strip()
            athletes.append({
                "file": file_path,
                "name": name,
                "country": (row.get("country") or "").strip(),
                "sport": (row.get("sport") or "").strip(),
                "participations": (row.get("participations") or "").strip(),
                "first_games": (row.get("first_games") or "").strip(),
                "yob": (row.get("yob") or "").strip(),
                "gold": int(row.get("gold")),
                "silver": int(row.get("silver")),
                "bronze": int(row.get("bronze")),
                "url": url,
            })
    return athletes

def score_tfidf(query_terms, index):
    idf = index["idf"]
    postings = index["postings"]
    scores = Counter()
    for t in query_terms:
        if t not in postings or t not in idf: continue # Ignorujem nenájdené slová
        w = idf.get(t)
        for doc_id, tf in postings[t].items():
            scores[doc_id] += tf * w
    return scores

def score_bm25(query_terms, index, k1=1.2, b=0.75): # k1,b podľa prednášky
    idf = index["idf"]
    postings = index["postings"]
    dl = index["doc_len"]
    avgdl = index.get("avgdl")
    scores = Counter()
    for t in query_terms:
        if t not in postings or t not in idf: continue # Ignorujem nenájdené slová
        idf_t = idf.get(t)
        for doc_id, tf in postings[t].items():
            scores[doc_id] += (
                idf_t * (tf * (k1 + 1))
                ) / (
                tf + k1 * (1 - b + b * (dl[doc_id] / avgdl))
            )
    return scores

def parse_ath(q):
    parts = shlex.split(q)
    attributes = {"country": None, "sport": None, "name": None}
    medals = []
    search_term = []
    for p in parts:
        m = re.fullmatch(r'(?i)(gold|silver|bronze)\s*(>=|<=|==?|<|>)\s*(\d+)', p) # Match pre search operátory gold > 5 a pod.
        if m:
            medals.append((m.group(1).lower(), OPS[m.group(2)], int(m.group(3)))) # color, operácia, počet
            continue
        m = re.fullmatch(r'(?i)(country|sport|name)\s*:\s*(.+)', p) # Match pre atribúty
        if m:
            attributes[m.group(1).lower()] = m.group(2) # f["sport"] = Basketball
            continue
        search_term.append(p)
    if search_term and not attributes["name"]:
        attributes["name"] = " ".join(search_term)
    return attributes, medals

def match_all(athlete, search, medals):
    if search["country"] and clean(athlete["country"]) != clean(search["country"]): 
        return False
    if search["sport"]   and clean(athlete["sport"])   != clean(search["sport"]):
        return False
    for col, op, val in medals:
        if not op(int(athlete.get(col, 0) or 0), val): return False
    return True

def score_athlete(athlete, search, medals_len):
    score = 0.0
    if search["name"] and clean(search["name"]) in clean(athlete["name"]): 
        score += 2.0
    if search["country"]: 
        score += 1.0
    if search["sport"]:  
        score += 1.0
    if medals_len:   
        score += 0.25 * medals_len
    return score

def search_athletes(athletes, query, topk=10):
    search, medals = parse_ath(query)
    hits = []
    for athlete in athletes:
        if match_all(athlete, search, medals):
            score = score_athlete(athlete, search, len(medals))
            # ak nie je meno ani žiadny filter, nemá zmysel vracať všetko
            if score == 0 and not (search["country"] or search["sport"] or medals):
                continue
            hits.append((score, athlete))
    hits.sort(key=lambda x: x[0], reverse=True)
    return hits[:topk]

def search_loop(docid_to_url, athletes, index, rank, k):
    query = input("OASIS Search > ").strip()
    start = time.perf_counter()

    if query.lower().startswith("q:"):
        return True

    if query.lower().startswith("ath:"): # iba athlete vyhladavanie
        query = query[4:].strip()
        print(f"\nTop {k} results for: \"{query}\"\n")
        scored_athletes = search_athletes(athletes, query, topk=k)
        elapsed = (time.perf_counter() - start) * 1000
        print(f"Query time: {elapsed:.2f}ms\n")
        for score, a in scored_athletes:
            print(f"{a['name']}\t{score:.3f}\n"
                f"Country: {a['country']} | Sport: {a['sport']} | " f"Medals: G{a.get('gold',0)}/S{a.get('silver',0)}/B{a.get('bronze',0)}\n"
                f"Link: {a.get('url','')}\n")
        return False

    q_terms = tokenize(query)
    print(f"\nTop {k} results for: \"{query}\"")

    scores = score_bm25(q_terms, index) if rank == "bm25" else score_tfidf(q_terms, index)
    top = scores.most_common(k)

    elapsed = (time.perf_counter() - start) * 1000
    # Výsledky podľa skóre
    print(f"Query time: {elapsed:.2f}ms\n")
    for i, (doc_id, score) in enumerate(top, 1): 
        url = docid_to_url.get(str(doc_id), "")
        print(f"{i}. {doc_id}\t{score:.3f}\nLink: {url}\n")
    return False


def main(index_path, rank, metadata_path, athletes_path, k):
    with open(index_path, "r", encoding="utf-8") as f:
        index = json.load(f)

    docid_to_url = load_metadata_urls(metadata_path) # URL Nie je ukladaná v indexe aby bolo šetrené miesto, treba lookup medzi metadátami a indexom
    athletes = load_athletes(athletes_path)
    stop = False
    while not stop:
        stop = search_loop(docid_to_url, athletes, index, rank, k)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--index",
        dest="index_path",
        default="data/index.json",
        help="Cesta k JSON indexu"
    )
    parser.add_argument(
        "--rank",
        dest="rank",
        default="bm25",
        choices=["tfidf","bm25"],
        help="Skórovanie"
    )
    parser.add_argument(
        "--metadata",
        dest="metadata_path",
        default="data/metadata.csv",
        help="Cesta k metadata.csv"
    )
    parser.add_argument(
        "--athletes",
        dest="athletes_path",
        default="extracted/athletes.csv",
        help="Cesta k athletes.csv"
    )
    parser.add_argument(
        "--k",
        dest="k",
        type=int,
        default=10,
        help="Počet výsledkov"
    )
    args = parser.parse_args()
    main(args.index_path, args.rank, args.metadata_path, args.athletes_path, args.k)
