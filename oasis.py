import lucene # type: ignore
import re
from org.apache.lucene.analysis.standard import StandardAnalyzer # type: ignore
from org.apache.lucene.index import DirectoryReader, Term # type: ignore
from org.apache.lucene.store import MMapDirectory # type: ignore
from org.apache.lucene.search import IndexSearcher, TermQuery, BoostQuery, BooleanQuery, BooleanClause # type: ignore
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser # type: ignore
from org.apache.lucene.document import IntPoint # type: ignore
from java.nio.file import Paths # type: ignore
from org.apache.lucene.search import BooleanQuery, BooleanClause, BoostQuery # type: ignore
from org.apache.lucene.util import QueryBuilder # type: ignore
from fastapi import FastAPI # type: ignore
from typing import List, Dict, Any
from pydantic import BaseModel # type: ignore
import time

from fastapi import FastAPI, Request # type: ignore
from fastapi.responses import HTMLResponse # type: ignore
from fastapi.staticfiles import StaticFiles # type: ignore

# Operacie nad integermi - pre rovnaky search ako v 1. odovzdani

MEDAL_RE = re.compile(r'(?i)\b(gold|silver|bronze)\s*(>=|<=|>|<|==?)\s*(\d+)\b')
YEAR_RE = re.compile(r'(?i)\b(yob|year|born)\s*(>=|<=|>|<|==?)?\s*(\d{1,4})\b')
PARTICIPATION_RE = re.compile(r'(?i)\b(participations)\s*(>=|<=|>|<|==?)\s*(\d+)\b')

OPS = {
    ">":  lambda f, v: (v+1, 10000),
    ">=": lambda f, v: (v,10000),
    "<":  lambda f, v: (0,v-1),
    "<=": lambda f, v: (0,v),
    "=":  lambda f, v: (v,v),
    "==": lambda f, v: (v,v)
}

FIELDS = [
    "name",
    "sport_text",
    "country_text",
    "wiki_sport",
    "wiki_birth_place",
    "wiki_position",
    "wiki_club",
    "wiki_bio",
    "lookup_texts"
]
EXACT_FIELDS = ["id", "sport", "country", "first_games"]
BOOSTS = {
    "name": 5.0,
    "sport_text": 3.0, 
    "country_text": 3.0,
    "wiki_sport": 2.0,
    "wiki_position": 1.5,
    "wiki_birth_place": 2.0,
    "wiki_club": 1.2,
    "wiki_bio": 1.1,
    "lookup_texts": 1.05
}

app = FastAPI()
class QueryModel(BaseModel):
    query: str
#app.mount("/static", StaticFiles(directory="static"), name="static")

lucene.initVM(vmargs=['-Djava.awt.headless=true'])

index_dir = MMapDirectory(Paths.get("data/lucene_index"))
reader = DirectoryReader.open(index_dir)
searcher = IndexSearcher(reader)
analyzer = StandardAnalyzer()

def to_dict(doc):
    result = {}
    for field in doc.getFields():
        name = field.name()
        value = doc.get(name)
        if value is None:
            continue
        result[name] = value
    return result

def multifield_query(text, analyzer):
    tokens = re.findall(r'"[^"]*"|\S+',text) # Ponechanie "takychto termov" ako jeden

    builder = QueryBuilder(analyzer)
    qb = BooleanQuery.Builder()

    for tok in tokens:
        is_phrase = tok.startswith('"') and tok.endswith('"')
        clean_tok = tok.strip('"')

        # StringField
        if is_phrase:
            exact_block = BooleanQuery.Builder()
            for field in EXACT_FIELDS:
                tq = TermQuery(Term(field, clean_tok))
                exact_block.add(tq, BooleanClause.Occur.SHOULD)
            exact_block.setMinimumNumberShouldMatch(1)
            qb.add(exact_block.build(), BooleanClause.Occur.MUST)
        # Full-text TextField
        for field in FIELDS:
            q = builder.createBooleanQuery(field, clean_tok)
            if q:
                boost = BOOSTS.get(field, 1.0)
                qb.add(BoostQuery(q, boost), BooleanClause.Occur.SHOULD)

    if tokens:
        qb.setMinimumNumberShouldMatch(1)

    return qb.build()


@app.post("/search")
def search(q: QueryModel):
    lucene.getVMEnv().attachCurrentThread() 

    start = time.perf_counter()
    user_input = q.query.strip()

    medal_filters = []
    for m in MEDAL_RE.finditer(user_input):
        color = m.group(1).lower()
        op = m.group(2)
        value = int(m.group(3))
        medal_filters.append((color, op, value))

    clean_query = MEDAL_RE.sub("", user_input).strip()

    year_filters = []
    for m in YEAR_RE.finditer(user_input):
        field = m.group(1).lower()
        op = m.group(2) or "=="
        value = int(m.group(3))
        year_filters.append((field, op, value))

    clean_query = YEAR_RE.sub("", clean_query)

    participation_filters = []
    for m in PARTICIPATION_RE.finditer(user_input):
        op = m.group(2)
        value = int(m.group(3))
        participation_filters.append((op, value))

    clean_query = PARTICIPATION_RE.sub("", clean_query)

    builder = BooleanQuery.Builder()

    if clean_query:
        main_q = multifield_query(clean_query, analyzer)
        builder.add(main_q, BooleanClause.Occur.SHOULD)

    for color, op, val in medal_filters:
        lo, hi = OPS[op](color, val)
        range_query = IntPoint.newRangeQuery(color, lo, hi)
        builder.add(range_query, BooleanClause.Occur.MUST)

    for field, op, value in year_filters:
        lo, hi = OPS[op](field, value)
        range_q = IntPoint.newRangeQuery("yob", lo, hi)
        builder.add(range_q, BooleanClause.Occur.MUST)

    for op, value in participation_filters:
        lo, hi = OPS[op]("participations", value)
        range_q = IntPoint.newRangeQuery("participations", lo, hi)
        builder.add(range_q, BooleanClause.Occur.MUST)

    final_query = builder.build()
    top_docs = searcher.search(final_query, 100)

    results = []
    for score_doc in top_docs.scoreDocs:
        doc = searcher.doc(score_doc.doc)
        results.append({
            "score": score_doc.score,
            "fields": to_dict(doc)
        })

    elapsed = (time.perf_counter() - start) * 1000
    return {
        "query_time_ms": elapsed,
        "results": results
        }

@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!DOCTYPE html>
<html>
<head>
    <title>OASIS Search</title>
    <style>
        body { font-family: Arial; margin: 40px; background:#fafafa; }
        .search-box { width: 500px; font-size: 18px; padding: 10px; }
        .result { background:white; box-shadow:0 2px 6px rgba(0,0,0,0.15); padding:20px;
                  margin-bottom:20px; border-radius:8px; }
        .score { color: #888; font-size:14px; margin-bottom: 5px; }
        .title { font-size:22px; font-weight:bold; margin-bottom:8px; }
        .field { margin:3px 0; }
        .label { font-weight:bold; }
        .bio { margin-top:8px; color:#444; }
        .medals { margin: 10px 0; }
        .links-toggle { color:#0077cc; cursor:pointer; margin-top:6px; display:block; }
        .lookup-list { margin-top:6px; display:none; }
    </style>
</head>
<body>

<h1>OASIS Search</h1>

<input class="search-box" id="query" placeholder="Search athletes...">
<button onclick="doSearch()">Search</button>

<div id="results"></div>

<script>
function safeField(value) {
    return (value === undefined || value === null || value.trim() === "") ? null : value;
}

async function doSearch() {
    let q = document.getElementById("query").value;
    let r = await fetch("/search", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({query: q})
    });

    let data = await r.json();
    let container = document.getElementById("results");
    container.innerHTML = "";
    container.innerHTML = `<div style="margin-bottom: 15px; color: #555;"> Query time: ${data.query_time_ms.toFixed(2)} ms</div>`;

    for (let item of data.results) {
        let f = item.fields;

        let html = `<div class="result">
            <div class="title">${safeField(f.name) || "Unknown"}</div>
            <div class="score">Score: ${item.score.toFixed(2)}</div>
        `;

        const fieldsToShow = [
            ["wiki_birth_place", "Birth place"],
            ["wiki_club", "Club"],
            ["country", "Country"],
            ["sport", "Sport"],
            ["first_games", "First Games"],
            ["participations", "Participations"],
            ["yob", "Year of Birth"],
            ["wiki_height", "Height"],
            ["athlete_url", "Athlete URL"]
        ];

        for (let [key, label] of fieldsToShow) {
            let v = safeField(f[key]);
            if (v) {
                if (key === "athlete_url")
                    html += `<div class="field"><span class="label">${label}:</span> <a href="${v}" target="_blank">${v}</a></div>`;
                else
                    html += `<div class="field"><span class="label">${label}:</span> ${v}</div>`;
            }
        }

        // Medals
        let gold = safeField(f.gold) || "0";
        let silver = safeField(f.silver) || "0";
        let bronze = safeField(f.bronze) || "0";
        html += `
            <div class="medals">
                <span class="label">Medals:</span><br>
                 - gold: ${gold}<br>
                 - silver: ${silver}<br>
                 - bronze: ${bronze}
            </div>
        `;

        // Bio
        if (safeField(f.wiki_bio)) {
            html += `<div class="bio">${f.wiki_bio.slice(0, 300)}...</div>`;
        }

        // Lookup URLs — collapsed
        if (safeField(f.lookup_urls)) {
            let links = f.lookup_urls.split(" ");
            html += `
                <span class="links-toggle" onclick="toggleLinks(this)">Show ${links.length} links...</span>
                <div class="lookup-list">
            `;
            for (let url of links.slice(0, 20)) {
                html += `<div><a href="${url}" target="_blank">${url}</a></div>`;
            }
            html += `</div>`;
        }

        html += `</div>`; // end card
        container.innerHTML += html;
    }
}

function toggleLinks(el) {
    let box = el.nextElementSibling;
    if (box.style.display === "none" || box.style.display === "") {
        box.style.display = "block";
        el.textContent = "Hide links";
    } else {
        box.style.display = "none";
        el.textContent = "Show links...";
    }
}
</script>

</body>
</html>
"""
