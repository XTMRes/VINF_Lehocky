import os, re, html

TAG = re.compile(r"<[^>]+>") #tagy
WS  = re.compile(r"\s+") #whitespace
BLOCK = re.compile(
    r'<(?P<tag>div|section)(?=[^>]*\bdata-cy\s*=\s*["\'](?:text-block-container|second-block)["\'])[^>]*>(?P<body>.*?)</(?P=tag)>', # väčšina relevantného textu je v text-block alebo second-block containeroch
    re.I | re.S
)
JUNK = re.compile(
    r"""(?is)<(script|style|noscript|template|svg|picture|source)\b[^>]*>.*?</\1>"""
)
MAIN = re.compile(r'<main[^>]*>(?P<body>.*?)</main>', re.I | re.S) # fallback na main text extrakciu

# normalizacia
def clean(s):
    s = TAG.sub(" ", s or "")
    s = html.unescape(s)
    return WS.sub(" ", s).strip()

# Text-blocky
def extract_blocks(raw, minlen):
    chunks = []
    for m in BLOCK.finditer(raw):
        t = clean(m.group("body") or "")
        if len(t) >= minlen:
            chunks.append(t)
    return chunks

# Main content (fallback)
def fallback_main(raw, minlen):
    mm = MAIN.search(raw)
    if mm:
        t = clean(mm.group("body") or "")
        if len(t) >= minlen:
            return t
    ps = re.findall(r'<p[^>]*>(.*?)</p>', raw, flags=re.I|re.S)
    t = clean(" \n ".join(ps))
    return t if len(t) >= minlen else ""

def write_txt(html_path, out_txt, minlen):
    os.makedirs(os.path.dirname(out_txt), exist_ok=True)

    with open(html_path, "r", encoding="utf-8", errors="ignore") as f:
        raw = f.read()

    raw = JUNK.sub(" ", raw) # Odstránenie scriptov a pod.
    blocks = extract_blocks(raw, minlen=minlen) # extrakcia text-blockov
    
    if not blocks:
        fb = fallback_main(raw, minlen=minlen) # extrakcia main (fallback)
        if not fb:
            return False
        with open(out_txt, "w", encoding="utf-8") as out: # zapis fallback
            out.write(fb)
        return True
    
    with open(out_txt, "w", encoding="utf-8") as out: # zapis blockov
        out.write("\n\n".join(blocks))
    return True
