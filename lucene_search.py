import lucene # type: ignore
import re
from org.apache.lucene.analysis.standard import StandardAnalyzer # type: ignore
from org.apache.lucene.index import DirectoryReader, Term # type: ignore
from org.apache.lucene.store import MMapDirectory # type: ignore
from org.apache.lucene.search import IndexSearcher, TermQuery, BoostQuery, BooleanQuery, BooleanClause # type: ignore
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser # type: ignore
from org.apache.lucene.document import IntPoint # type: ignore
from java.nio.file import Paths # type: ignore
from org.apache.lucene.search import BooleanQuery, BooleanClause # type: ignore
from org.apache.lucene.util import QueryBuilder # type: ignore
import time

MEDAL_RE = re.compile(r"(?i)\b(gold|silver|bronze)\s*(>=|<=|>|<|==?)\s*(\d+)\b")
YEAR_RE = re.compile(r"(?i)\b(yob|year|born)\s*(>=|<=|>|<|==?)?\s*(\d{1,4})\b")
PARTICIPATION_RE = re.compile(r"(?i)\b(participations)\s*(>=|<=|>|<|==?)\s*(\d+)\b")


OPS = {
    ">": lambda f, v: (v + 1, 10000),
    ">=": lambda f, v: (v, 10000),
    "<": lambda f, v: (0, v - 1),
    "<=": lambda f, v: (0, v),
    "=": lambda f, v: (v, v),
    "==": lambda f, v: (v, v),
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
    "lookup_texts": 1.05,
}
# Formatovanie vysledkov
def print_document(doc, truncate_len=150):
    for field in doc.getFields():
        name = field.name()
        value = doc.get(name)

        if value is None: continue

        value = value.strip()
        if not value:
            continue

        if name == "lookup_texts":
            value = value[:truncate_len] + "..."

        if name == "gold":
            print("medals:")
        if name in ["gold","silver","bronze"]:
            print(f"\t- {name}: {value}")
            continue

        elif name == "lookup_urls":
            lookup_urls = value.split(" ")
            print(f"{name}:")

            for url in lookup_urls[:3]:
                print("\t-",url)

            if len(lookup_urls) > 3:
                print("\t...")
            continue

        print(f"{name}: {value}")

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

# Search loop
def search(searcher, analyzer):
    while True:
        user_input = input("OASIS Search > ").strip()
        if not user_input:
            continue
        
        start = time.perf_counter()
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
            main_query = multifield_query(clean_query, analyzer)
            builder.add(main_query, BooleanClause.Occur.SHOULD)

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
        top_docs = searcher.search(final_query, 3)

        elapsed = (time.perf_counter() - start) * 1000
        print(f"Query time: {elapsed:.2f}ms\n")
        for score_doc in top_docs.scoreDocs:
            doc = searcher.doc(score_doc.doc)
            print("-----")
            print(f"score: {score_doc.score}")
            print_document(doc)


def main():
    lucene.initVM(vmargs=["-Djava.awt.headless=true"])

    index_dir = MMapDirectory(Paths.get("data/lucene_index"))
    reader = DirectoryReader.open(index_dir)
    searcher = IndexSearcher(reader)
    analyzer = StandardAnalyzer()

    search(searcher, analyzer)

    reader.close()

if __name__ == "__main__":
    main()
