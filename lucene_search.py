import lucene
import re
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader, Term
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.search import IndexSearcher, TermQuery
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.document import IntPoint
from java.nio.file import Paths
from org.apache.lucene.search import BooleanQuery, BooleanClause

# Operacie nad integermi - pre rovnaky search ako v 1. odovzdani

MEDAL_RE = re.compile(r'(?i)\b(gold|silver|bronze)\s*(>=|<=|>|<|==?)\s*(\d+)\b')

OPS = {
    ">":  lambda f, v: (v+1, 10000),
    ">=": lambda f, v: (v,10000),
    "<":  lambda f, v: (0,v-1),
    "<=": lambda f, v: (0,v),
    "=":  lambda f, v: (v,v),
    "==": lambda f, v: (v,v),
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

# Search loop
def search(searcher, analyzer):
    print("Type 'help' for instructions. Type 'exit' to quit.")

    allowed_fields_text = [
        "name", "sport_text", "country_text",
        "wiki_sport", "wiki_birth_place", "wiki_position",
        "wiki_club", "wiki_bio", "lookup_texts"
    ]

    allowed_fields_terms = [
        "country", "sport", "id", "first_games", "participations", "athlete_url"
    ]  # Povolene query terms, mozu byt dodane ine ale tieto mi stacili na pekny search

    while True:
        user_input = input("OASIS Search > ").strip()

        if user_input.lower() == "exit": break

        if user_input.lower() == "help":
            print("Usage examples:")
            print("\tname: Bolt")
            print("\tcountry: SVK gold >= 1")
            print("\tsport: Swimming silver = 0")
            continue

        if not user_input: continue

        # Podobne ako non-spark verzia
        medal_filters = []
        for m in MEDAL_RE.finditer(user_input):
            color = m.group(1).lower()
            op = m.group(2)
            value = int(m.group(3))
            medal_filters.append((color, op, value))

        clean_query = MEDAL_RE.sub("", user_input).strip()

        pairs = re.findall(
            r'(\w+)\s*:\s*("(?:[^"\\]|\\.)*"|.*?)(?=\s+\w+\s*:|$)', # Sekanie query dopytov
            clean_query
        )
        builder = BooleanQuery.Builder()

        if clean_query:
            if not pairs:
                parser = QueryParser("name", analyzer)
                text_query = parser.parse(clean_query)
                builder.add(text_query, BooleanClause.Occur.MUST)
            else:
                for field, value in pairs:
                    value = value.strip('"')

                    if field not in allowed_fields_terms and field not in allowed_fields_text:
                        print("Unknown field:", field)
                        continue

                    # Text polia
                    if field in allowed_fields_text:
                        parser = QueryParser(field, analyzer)
                        q = parser.parse(value)

                    # Exact match polia
                    elif field in allowed_fields_terms:
                        q = TermQuery(Term(field, value))

                    builder.add(q, BooleanClause.Occur.MUST)

        for color, op, val in medal_filters:
            lo, hi = OPS[op](color, val)
            range_query = IntPoint.newRangeQuery(color, lo, hi) # Integer porovnavacia logika
            builder.add(range_query, BooleanClause.Occur.MUST) # Boolean logika, vsetko je must

        final_query = builder.build()
        top_docs = searcher.search(final_query, 5) # Vraciam iba top 5 aby tam nebol clutter

        for score_doc in top_docs.scoreDocs:
            doc = searcher.doc(score_doc.doc)
            print("-----")
            print(f"score: {score_doc.score}")
            print_document(doc)

def main():
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])

    index_dir = MMapDirectory(Paths.get("data/lucene_index"))
    reader = DirectoryReader.open(index_dir)
    searcher = IndexSearcher(reader)

    analyzer = StandardAnalyzer()

    search(searcher, analyzer)

    reader.close()


if __name__ == "__main__":
    main()
