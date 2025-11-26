from pyspark.sql import functions as F
import re

def build_htmls(spark, html_a, html_l, minlen):
    # Mirror prveho odovzdania
    ATTR_MAP = {
        "name": "display-name",
        "noc": "nocs",
        "sport": "disciplines",
        "participations": "games-participations",
        "first_games": "first-olympic-game",
        "yob": "year-of-birth",
    }

    # Primitivna normalizacia mien, v sparku chyba vhodna kniznica
    def normalize_name(col):
        src = "áäàâãåçčďéěèêëíìîïñńňóôöòõřŕśšťúůüùûýÿžÁÄÀÂÃÅÇČĎÉĚÈÊËÍÌÎÏÑŃŇÓÔÖÒÕŘŔŚŠŤÚŮÜÙÛÝŸŽłĺľŁĹĽ"
        dst = "aaaaaaccdeeeeeiiiinnnooooorrsstuuuuuyyzAAAAAACCDEEEEEIIIINNNOOOOORRSSTUUUUUYYZlllLLL"
        col = F.translate(col, src, dst)
        col = F.lower(F.trim(col))
        col = F.regexp_replace(col, r"\s*[\(\[].*?[\)\]]\s*", " ")
        col = F.regexp_replace(col, r"[^a-z0-9\s]", " ")
        col = F.regexp_replace(col, r"\s+", " ")
        return F.trim(col)

    def get_attr(html_col, key): # Logika extrakcie atributov z prveho odovzdania prepisana na spark potom a krvou
        re_div  = rf"(?is)<div[^>]*data-cy=['\"]{re.escape(key)}['\"][^>]*>.*?<(?:h1|span)[^>]*>(.*?)</(?:h1|span)>"
        re_span = rf"(?is)<span[^>]*data-cy=['\"]{re.escape(key)}['\"][^>]*>(.*?)</span>"

        v1 = F.regexp_extract(html_col, re_div, 1)
        v2 = F.regexp_extract(html_col, re_span, 1)
        val = F.when(F.length(v1) > 0, v1).otherwise(v2)
        val = F.regexp_replace(val, r"<[^>]+>", " ")
        val = F.trim(F.regexp_replace(val, r"\s+", " "))
        return F.when(F.length(val) > 0, val)

    def get_medals(html_col): # Extrakcia medajli a poctov, toto bol asi najvacsi boj a zobralo mi to 10 rokov zivota lebo v sparku sa neda rovnako zrkadlit extrakciu z 1. zadania
        blocks = F.regexp_extract_all(html_col, F.lit(r'(?is)<div[^>]*data-cy=["\']medal-module["\'][^>]*>.*?</div>'), F.lit(0))

        colors = F.transform(blocks, lambda b: F.regexp_extract(b, r'title=["\'](Gold|Silver|Bronze)["\']', 1))
        counts = F.transform(blocks, lambda b: F.regexp_extract( b, r'data-cy=["\']ocs-text-module["\'][^>]*>(\d+)</', 1 ).cast("int"))

        medals = F.zip_with(colors,counts,lambda c, n: F.struct(c.alias("color"),n.alias("count")))

        gold = F.aggregate(F.filter(medals, lambda m: m["color"]=="Gold"), F.lit(0), lambda a,m: a+m["count"])
        silver = F.aggregate(F.filter(medals, lambda m: m["color"]=="Silver"), F.lit(0), lambda a,m: a+m["count"])
        bronze = F.aggregate(F.filter(medals, lambda m: m["color"]=="Bronze"), F.lit(0), lambda a,m: a+m["count"])

        return gold, silver, bronze
    
    out = html_a
    for col, key in ATTR_MAP.items():
        out = out.withColumn(col, get_attr(F.col("raw_html"), key))

    gold_col, silver_col, bronze_col = get_medals(F.col("raw_html"))
    out = out.withColumn("gold", gold_col).withColumn("silver", silver_col).withColumn("bronze", bronze_col)

    ath = out.select(
        "name",
        F.col("noc").alias("country"),
        "sport",
        "participations",
        "first_games",
        "yob",
        "gold",
        "silver",
        "bronze",
        F.col("url").alias("athlete_url")
    )

    junk = r"(?is)<(script|style|noscript|template|svg|picture|source)[^>]*>.*?</\1>"
    cleaned = F.regexp_replace(F.col("raw_html"), junk, " ")

    block = (
        r'(?is)<(div|section)'
        r'(?=[^>]*data-cy=["\'](?:text-block-container|second-block)["\'])'
        r'[^>]*>(.*?)</\1>'
    )

    blocks = F.regexp_extract_all(cleaned, F.lit(block), F.lit(2))

    cleaned_blocks = F.transform(blocks, lambda b: F.trim(F.regexp_replace(F.regexp_replace(b, r"<[^>]+>", " "), r"\s+", " ")))

    long_blocks = F.filter(cleaned_blocks, lambda x: F.length(x) >= minlen)
    joined_blocks = F.array_join(long_blocks, "\n\n")

    # fallback main
    main_body = F.regexp_extract(cleaned, r"(?is)<main[^>]*>(.*?)</main>", 1)
    clean_main = F.trim(F.regexp_replace( F.regexp_replace(main_body, r"<[^>]+>", " "), r"\s+", " "))

    # fallback <p>
    p_tags = F.regexp_extract_all(cleaned, F.lit(r"(?is)<p[^>]*>(.*?)</p>"), F.lit(1))
    cleaned_ps = F.transform(p_tags,lambda b: F.trim(F.regexp_replace(F.regexp_replace(b, r"<[^>]+>", " "),r"\s+"," ")))
    joined_ps = F.array_join(cleaned_ps, "\n")

    final_text = (
        F.when(F.size(long_blocks) > 0, joined_blocks)
         .otherwise(
             F.when(F.length(clean_main) >= minlen, clean_main)
              .otherwise(
                  F.when(F.length(joined_ps) >= minlen, joined_ps)
                   .otherwise(F.lit(""))
              )
         )
    )

    lookups = (
        html_l
            .withColumn("text", final_text)
            .filter(F.length(F.col("text")) > 0)
            .withColumn("text_norm", normalize_name(F.col("text")))
            .select("doc_id", "url", "text", "text_norm")
    )

    return ath, lookups