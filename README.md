# Projekt OASIS #
## Olympic Athlete Search and Information Service (OASIS) ##
Projekt OASIS bol vytvorený za účelom vyhľadávania primárnych informácií o jednotlivých atlétoch zúčastnených v olympijských športoch.

Jeho hlavným cieľom je zhromažďovanie a výpis dát na základe dopytu užívateľa ohľadom atlétov, ich účasti v športoch, výhier olympijských medajlí a dodatočných informácií v článkoch.

Projekt zahŕňa kompletný IR pipeline – webový crawler, HTML extraktor, tvorbu indexu, výpočet IDF (klasický a BM25) a jednoduchý konzolový searcher.

Kompletná dokumentácia sa nachádza na https://vi2025.ui.sav.sk/doku.php?id=user:zdenko.lehocky:start

--- 

This script is the result of a project for the subject VINF_I

The script asynchronously crawls and renders pages from [olympics.com/en/athletes](https://olympics.com/en/athletes) (or another root URL you specify), saving **rendered HTML**, **URL queue state**, and **metadata** about each processed page.

It uses a custom asynchronous browser (`Browser`) for JavaScript rendering and a URL manager (`URLManager`) to maintain the crawl queue.

---

## Requirements

- **Python** 3.10+
- **Dependencies:**
    - tqdm library
    - Playwright framwork

##  Quick start
```
python scraper.py
```
#### This will:
- Start from https://olympics.com/en/athletes
- Save rendered pages under ./html
- Store the crawl state in ./data/urls.json
- Write crawl metadata to ./data/metadata.csv
- Process up to 100 pages by default (adjustable)
- Sleep ~0.5–0.9 seconds between requests

## Output structure
```
html
  ├─ athletes
  │    ├─ athletes_1.html   # athlete profile pages
  │    ├─ athletes_2.html
  │    └─ ...
  └─ lookup
       ├─ athletes.html     # index/list pages leading to athlete profiles
       ├─ news.html
       └─ ...
data
  ├─ urls.json              # serialized URL queue (managed by URLManager)
  └─ metadata.csv           # processing log
```
#### Metadata structure
| Collumn | Description |
|--------|--------------------------------------------|
| status | HTTP status code (or "ERR" on failure) |
| file	 | File path to the saved HTML | 
| date	 | Date of processing (ISO format YYYY-MM-DD) | 
| url	 | The processed URL | 

## Command-line options
```
python scraper.py [--output PATH] [--url URL] [--json PATH] [--csv PATH] [--max-pages N] [--reset]
```

|Option     |Description	                             |Default                          |
|-----------|--------------------------------------------|---------------------------------|
|--output   |Output directory for HTML files	         |./html                           |
|--url      |	Root URL to start crawling from	         |https://olympics.com/en/athletes |
|--json     |Path to JSON file for serialized queue state|./data/urls.json                 |
|--csv	    |Path to CSV metadata file	                 |./data/metadata.csv              |
|--max-pages|	Maximum number of pages to process     	 |100                              |
|--reset	|Wipes all outputs and starts clean	         |off                              |

## Examples
Run with defaults:
```
python scraper.py
```

Change page limit:
```
python scraper.py --max-pages 1000
```

Specify custom output paths:
```
python scraper.py --output ./out/html --json ./state/urls.json --csv ./logs/metadata.csv
```

Change url:
```
python scraper.py --url "https://olympics.com/en/sports"
```

Start from scratch:
```
python scraper.py --reset
``` 


## How it works

### 1. Initialization

- Optional --reset cleans previous output and state files.
- Creates output folders (athletes, lookup).
- Loads existing queue from urls.json (or starts fresh).
- Initializes the CSV log file if missing.

### 2. Crawling Loop
- Takes the next URL from URLManager.get().
- Fetches HTML using Browser.fetch(url) (rendered with JS).
- Extracts links and adds them to the queue.
- Saves relevant pages (athletes/ or lookup/).
- Logs result to metadata.csv.
- Waits 0.5–0.9s between requests.

### 3. Finish
- Serializes queue state back to urls.json.
