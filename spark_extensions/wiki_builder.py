from pyspark.sql import functions as F

def build_wiki(spark, wiki_dump_path):
    TITLE = F.col("title")
    TEXT  = F.col("revision.text._VALUE")

    wiki = (
        spark.read.format("xml")
        .option("rowTag", "page")
        .option("ignoreNamespace", "true")
        .load(wiki_dump_path)
        .filter(TITLE.isNotNull())
        .filter(~TITLE.rlike(r"^(Category|List|Template|Module|Portal|Draft|Help|Wikipedia):")) # Pre-filter
        .filter(~TEXT.rlike(r"(?i)#redirect"))
    )

    cand = (
        wiki.withColumn("head", F.substring(TEXT, 1, 3000))
            .withColumn("head", F.regexp_replace("head", r"<!--.*?-->", " "))
            .withColumn("head", F.regexp_replace("head", r"(?i)<br\s*/?>", " \n "))
            .withColumn("head", F.regexp_replace("head", r"&nbsp;", " "))
    )

    cand = cand.filter(
        F.col("head").rlike(r"(?mi)^\|\s*(birth_place|birth_date)\s*=") |                   # Athlete pre-filter, rovnako ako odovzdani 1
        F.col("head").rlike(r"(?i)(sport|sports|event|discipline|team|club|position)\s*=")
    )

    def extract_line(pattern):
        return F.regexp_extract( F.col("head"), rf"(?im)^\|\s*{pattern}\s*=\s*(?!\s*$)([^\|\n]+)", 1) # Kombinovany regex pre extrakciu atributov | attribute = 

    sport_raw = extract_line("(?:sport|sports|event|discipline)") # iba "sport" bol velmi malo krat zachyteny
    birth_place_raw = extract_line("(?:birth_place|birthplace)")
    position_raw = extract_line("(?:position|positions)")
    club_raw = extract_line(r"[a-z_ ]*clubs?")  # curling_club / Curling club a pod.
    height_raw = extract_line("(?:height|height_cm|height_m)")

    def clean(col):
        col = F.regexp_replace(col, r"(?s)<ref[^>]*>.*?</ref>", " ") # <ref> ... </ref>
        col = F.regexp_replace(col, r"(?i)<ref[^/>]*>", " ") # <ref>
        col = F.regexp_replace(col, r"(?i)<ref[^/>]*/>", " ") # <ref/>
        col = F.regexp_replace(col, r"(?s)\{\{.*?\}\}", " ") # {{...}}
        col = F.regexp_replace(col, r"\{\{.*", " ") # {{...
        col = F.regexp_replace(col, r"\[\[[^|\]]*\|([^\]]+)\]\]", r"$1") # [[page|label]] = label
        col = F.regexp_replace(col, r"\[\[([^\]]+)\]\]", r"$1") # [[page]] = page
        col = F.regexp_replace(col, r"&[a-z]+;", " ") # nbsp
        col = F.regexp_replace(col, r"[•]", " ") # Zvlastne bullet pointy
        col = F.regexp_replace(col, r"\s+", " ") # ws
        return F.trim(col)
    
    wiki_sport = clean(sport_raw)
    wiki_birth_place = clean(birth_place_raw)
    wiki_position = clean(position_raw)
    wiki_club = clean(club_raw)
    wiki_height = clean(height_raw)

    bio = TEXT
    bio = F.regexp_replace(bio, r"(?s)\{\{Infobox[^}]*\}\}", " ") # Pokus o odseknutie celeho infoboxu, dodatocne este ocistene neskor
    bio = F.regexp_replace(bio, r"(?m)^\|.*$", " ") # | attribute =
    bio = F.regexp_replace(bio, r"<ref[^>]*>.*?</ref>", " ")
    bio = F.regexp_replace(bio, r"<ref[^/>]*/>", " ")
    bio = F.regexp_replace(bio, r"(?i)<small[^>]*>.*?</small>", " ")
    bio = F.regexp_replace(bio, r"(?i)</?small[^>]*>", " ")
    bio = F.regexp_replace(bio, r"(?s)\{\{.*?\}\}", " ") # {{...}}
    bio = F.regexp_replace(bio, r"<!--.*?-->", " ")
    bio = F.regexp_replace(bio, r"&[a-z]+;", " ") # nbsp
    bio = F.regexp_replace(bio, r"(?m)^\s*={1,6}\s*[^=]+?\s*={1,6}\s*$", " ") # nadpisy
    bio = F.regexp_replace(bio, r"\[\[([^|\]]+)\|([^\]]+)\]\]", r"$2") # rovnako ako v clean()
    bio = F.regexp_replace(bio, r"\[\[([^\]]+)\]\]", r"$1") # zvysky
    bio = F.regexp_replace(bio, r"[{}'\"|]+", " ")
    bio = F.regexp_replace(bio, r"\s+", " ")

    bio = F.trim(bio)
    bio = F.regexp_extract(
        bio,
        r"(?s)"
        r"([A-ZÁ-Ž][a-zá-ž]+ [A-ZÁ-Ž][a-zá-ž]+)" # meno
        r".*?[.!?]" # Prva veta
        r"\s*.*?[.!?]" # 3 dalsie vety
        r"\s*.*?[.!?]"
        r"\s*.*?[.!?]",
        0
    )

    out = (
        cand
        .withColumn("name", F.trim(F.regexp_replace(TITLE, r"\s*\(.*?\)$", ""))) # bez (zatvoriek) lebo by nesedel s ath_name
        .withColumn("wiki_sport", wiki_sport)
        .withColumn("wiki_birth_place", wiki_birth_place)
        .withColumn("wiki_position", wiki_position)
        .withColumn("wiki_club", wiki_club)
        .withColumn("wiki_height", wiki_height)
        .withColumn("wiki_bio", bio)
        .select(
            "name",
            "wiki_sport",
            "wiki_birth_place",
            "wiki_position",
            "wiki_club",
            "wiki_height",
            "wiki_bio"
        )
    )

    return out
