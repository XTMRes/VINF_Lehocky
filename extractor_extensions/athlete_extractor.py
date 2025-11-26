import re, csv, html

# /athlete/meno stranky su velmi konzistentne s formatom, tento kód je celkom robustný na ne

ATTR_MAP = { # V prvej verzii toto bol Athlete class ale takto je to krajsie
    "name": "display-name",
    "noc": "nocs",
    "sport": "disciplines",
    "participations": "games-participations",
    "first_games": "first-olympic-game",
    "yob": "year-of-birth"
}

TAG = re.compile(r"<[^>]+>") #tagy
WS  = re.compile(r"\s+") #whitespace

MEDALS = re.compile(
    r'<div[^>]*data-cy=["\']medal-module["\'][^>]*title=["\'](Gold|Silver|Bronze)["\'][\s\S]*?'
    r'<span[^>]*data-cy=["\']medal-main["\'][^>]*>[\s\S]*?>(\d+)<',
    re.I
)

def build_regex(key): # Helper funkcia pre dynamicke vytvaranie regexu (nech tu nie je kopa textu pre kazdy regex zvlast)
    # <div data-cy="display-name" class="sc-450d2f6e-3 dWeNcs"> <h1 class="sc-amhWr jgZeYT en original-size-title" columns="5">Aaliyah Nickole BUTLER</h1> </div>
    # <div data-cy="athlete-games-participations" class="sc-450d2f6e-12 kRfToA"> <span>Games Participations</span> <span data-cy="games-participations">1</span> </div>
    return re.compile(
        rf'(?:<div[^>]*\bdata-cy\s*=\s*["\']{re.escape(key)}["\'][^>]*>.*?<(?P<tag>h1|span)[^>]*>(?P<val>.*?)</(?P=tag)>.*?</div>)'
        rf'|(?:<span[^>]*\bdata-cy\s*=\s*["\']{re.escape(key)}["\'][^>]*>(?P<val2>.*?)</span>)',
        re.I | re.S
    )


# normalizacia
def clean(s):
    s = TAG.sub(" ", s or "")
    s = html.unescape(s)
    return WS.sub(" ", s).strip()

def extract_medals(raw):
    out = {"gold": 0, "silver": 0, "bronze": 0}
    for color, count in MEDALS.findall(raw):
        c = color.lower()
        out[c] = out.get(c, 0) + int(count)
    return out

def extract_fields(raw):
    attributes = {} # name, games, yob, ...
    missing = 0
    for out_key, data_cy in ATTR_MAP.items():
        found = build_regex(data_cy).search(raw)
        attribute = ""
        if found:
            attribute = clean(found.group("val") if found.group("val") is not None else found.group("val2")) # najprv skusim div potom span (niekedy je hodnota az v span ak je expandable)
        if not attribute: 
            missing += 1
        attributes[out_key] = attribute
    medals = extract_medals(raw)
    return attributes, medals, missing

def write_header(csv_writer):
    csv_writer.writerow(["file", "name", "country", "sport", "participations", "first_games", "yob", "gold", "silver", "bronze", "url"])

def write_row(csv_writer, html_path, attributes, medals, url):
    csv_writer.writerow([html_path, attributes["name"], attributes["noc"], attributes["sport"], attributes["participations"], attributes["first_games"], attributes["yob"], medals["gold"], medals["silver"], medals["bronze"] ,url])

def write_csv(csv_writer, html_path, url):
    with open(html_path, "r", encoding="utf-8", errors="ignore") as rf:
        raw = rf.read()
    attributes, medals,  missing = extract_fields(raw)
    write_row(csv_writer, html_path, attributes, medals ,url)
    return missing 