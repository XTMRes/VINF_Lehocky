# python src/spark.py --metadata data/metadata.csv --out extracted --wiki_dump data/enwiki-latest-pages-articles.xml.bz2

import os
import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from functools import reduce

from spark_extensions.html_builder import build_htmls
from spark_extensions.wiki_builder import build_wiki

# Primitivna normalizacia mien, v sparku chyba vhodna kniznica
def normalize_name(col):
    src = "áäàâãåçčďéěèêëíìîïñńňóôöòõřŕśšťúůüùûýÿžÁÄÀÂÃÅÇČĎÉĚÈÊËÍÌÎÏÑŃŇÓÔÖÒÕŘŔŚŠŤÚŮÜÙÛÝŸŽłĺľŁĹĽ"
    dst = "aaaaaaccdeeeeeiiiinnnooooorrsstuuuuuyyzAAAAAACCDEEEEEIIIINNNOOOOORRSSTUUUUUYYZlllLLL"
    col = F.translate(col, src, dst)
    col = F.lower(F.trim(col))
    col = F.regexp_replace(col, "\\s*\\(([^(]*)\\)\\s*", " ")
    col = F.regexp_replace(col, "\\s*\\[([^]]*)\\]\\s*", " ")
    col = F.regexp_replace(col, r"[^a-z0-9\\s]", " ")
    col = F.regexp_replace(col, r"\\s+", " ")
    return F.trim(col)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metadata", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--wiki_dump", required=True)
    ap.add_argument("--minlen", type=int, default=30)
    args = ap.parse_args()

    spark = (
        SparkSession.builder
        .master("local[*]")
        .config("spark.driver.memory", "16g") # Priblizne 10gb je potrebnych
        .config("spark.executor.memory", "16g")
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0") # Pre citanie xml
        .getOrCreate()
    )

    out_dir = args.out
    os.makedirs(out_dir, exist_ok=True)

    print("Loading metadata")
    meta = spark.read.option("header", True).csv(args.metadata)

    ok = meta.filter("status = '200'") # Iba spravne crawlnute

    athletes_rows = ok.filter("doc_id = '-1'").withColumnRenamed("file", "file_norm") # Rozlisenie atletov a lookup podla doc_id
    lookup_rows = ok.filter("doc_id != '-1'").withColumnRenamed("file", "file_norm")

    print("Loading HTML files")
    files_df = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .load("html")
        .select(
            F.regexp_extract("path", r".*?(html/.*)$", 1).alias("file_norm"),
            F.decode("content", "UTF-8").alias("raw_html")
        )
    )

    html_a = athletes_rows.join(files_df, "file_norm", "left") # Join existujucich suborov na metadata informacie
    html_l = lookup_rows.join(files_df, "file_norm", "left")

    print("Extracting HTML attributes")
    ath, lookups = build_htmls(spark, html_a, html_l, args.minlen) # Vytvorenie df a extrakcia scrapnutych html
    ath = ath.withColumn("name_norm", normalize_name("name")) # Normalizacia mena ako unikatny kluc pre joiny

    print("Joining lookup")

    lookup_matched = (  # Joinute su iba lookups ktore maju atletov
        lookups.join(
            F.broadcast(
                ath.select(
                    "name",
                    F.col("name_norm").alias("name_norm_b")
                )
            ),
            F.col("text_norm").contains(F.col("name_norm_b")),
            "left"
        )
    )

    lookup_agg = (
        lookup_matched.groupBy("name_norm_b")
            .agg(
                F.collect_list("text").alias("lookup_texts"), # agregacia lookupov pod jeden athlete zaznam
                F.collect_list("url").alias("lookup_urls") # 1:n vztah (v realite je to many to many ale tento kontext je to 1:n)
            )
            .withColumnRenamed("name_norm_b", "name_norm")
    )

    print("Loading and extracting Wikipedia")
    wiki_df = build_wiki(spark, args.wiki_dump) # Vytvorenie df a extrakcia wiki

    wiki_df_norm = wiki_df.withColumn("name_norm", normalize_name("name")) # Rovnake ako pre atletov, kluc pre join

    print("Joining WIKI with athletes by normalized name")

    matched = ( # wiki + athlete
        ath.join(
            F.broadcast(wiki_df_norm),
            on="name_norm",
            how="left"
        ).select(
            ath["*"],
            wiki_df_norm["wiki_sport"],
            wiki_df_norm["wiki_birth_place"],
            wiki_df_norm["wiki_position"],
            wiki_df_norm["wiki_club"],
            wiki_df_norm["wiki_height"],
            wiki_df_norm["wiki_bio"]
        )
    )

    final = matched.join(lookup_agg, "name_norm", "left") # wiki_athlete + lookup

    print("Deduplicating")

    # Deduplikacia mien je podla kompletnejsich wiki zaznamov (zly wiki join ma menej fieldov)
    # Rovnake mena pre atletov je bohuzial treba akceptovat, lebo 100% konzistentna extrakcia atributov z wiki nebola mozna a teda sa neda na nic spolahnut (ani birth year lebo je velmi vela roznych formatov a s regexami je nespolahlivy)
    wiki_cols = [
        "wiki_sport",
        "wiki_birth_place",
        "wiki_position",
        "wiki_club",
        "wiki_height",
        "wiki_bio"
    ]

    bool_cols = [
        ((F.col(c).isNotNull()) & (F.length(F.col(c)) > 0)).cast("int")
        for c in wiki_cols
    ]

    wiki_score_col = reduce(lambda a, b: a + b, bool_cols)

    final_scored = final.withColumn("wiki_score", wiki_score_col) # Skorovana tabulka podla poctu wiki_*

    w = Window.partitionBy("name_norm").orderBy(F.desc("wiki_score")) # Vybera sa najvyssi score
    final_ranked = final_scored.withColumn("rn", F.row_number().over(w))
    final_dedup = final_ranked.filter("rn = 1").drop("rn", "wiki_score")

    print("Final entry count", final_dedup.count()) # 3064 < 3079 realnych athlete html, takze duplicity vyriesene

    print("Writing to parquet")

    final_dedup.write.mode("overwrite").parquet(os.path.join(out_dir, "spark"))

    spark.stop()

if __name__ == "__main__":
    main()
