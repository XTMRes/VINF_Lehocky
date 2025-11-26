import lucene
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, StringField, TextField, StoredField, IntPoint
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import MMapDirectory
from java.nio.file import Paths
import os

from pyspark.sql import SparkSession, functions as F

def doc_from_row(row):
    doc = Document()

    def get(name, default=""):
        v = row[name] if name in row and row[name] is not None else default
        return str(v)

    # fulltext
    doc.add(TextField("name", get("name"), Field.Store.YES))
    doc.add(TextField("sport_text", get("sport"), Field.Store.NO))
    doc.add(TextField("country_text", get("country"), Field.Store.NO))

    # chybajuce nepridavam do dokument indexu pre lepsi search
    wiki_sport = get("wiki_sport").strip()
    if wiki_sport: doc.add(TextField("wiki_sport", wiki_sport, Field.Store.YES))

    wiki_birth_place = get("wiki_birth_place").strip()
    if wiki_birth_place: doc.add(TextField("wiki_birth_place", wiki_birth_place, Field.Store.YES))

    wiki_position = get("wiki_position").strip()
    if wiki_position: doc.add(TextField("wiki_position", wiki_position, Field.Store.YES))

    wiki_club = get("wiki_club").strip()
    if wiki_club: doc.add(TextField("wiki_club", wiki_club, Field.Store.YES))

    wiki_bio = get("wiki_bio").strip()
    if wiki_bio: doc.add(TextField("wiki_bio", wiki_bio, Field.Store.YES))

    lookup_texts = row.get("lookup_texts") or []
    doc.add(TextField("lookup_texts", " ".join(lookup_texts), Field.Store.NO))

    # exact match atributy
    doc.add(StringField("id", get("ath_name_norm"), Field.Store.YES))
    doc.add(StringField("country", get("country"), Field.Store.YES))
    doc.add(StringField("sport", get("sport"), Field.Store.YES))
    doc.add(StringField("first_games", get("first_games"), Field.Store.YES))
    doc.add(StringField("participations", get("participations"), Field.Store.YES))

    # ciselne atributy
    for integers in ["gold", "silver", "bronze", "yob"]:
        val = int(get(integers, "0") or 0)
        doc.add(IntPoint(integers, val))
        doc.add(StoredField(integers, val))


    # iba ulozene
    doc.add(StoredField("athlete_url", get("athlete_url")))
    doc.add(StoredField("wiki_height", get("wiki_height")))

    lookup_urls = row.get("lookup_urls") or []
    doc.add(StoredField("lookup_urls", " ".join(lookup_urls)))

    return doc


def main():
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])

    spark = (SparkSession.builder.getOrCreate())

    df = spark.read.parquet("extracted/spark")
    os.makedirs("lucene_index", exist_ok=True)

    analyzer = StandardAnalyzer()
    index_dir = MMapDirectory(Paths.get("data/lucene_index"))
    config = IndexWriterConfig(analyzer)
    writer = IndexWriter(index_dir, config)

    for row in df.toLocalIterator():
        doc = doc_from_row(row.asDict())
        writer.addDocument(doc)

    writer.commit()
    writer.close()
    spark.stop()

if __name__ == "__main__":
    main()
