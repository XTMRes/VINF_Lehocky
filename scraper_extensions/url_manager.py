import json, re
import os
from collections import deque # pre FIFO
from urllib.parse import urlsplit,urlunsplit

# Striktnejší whitelist oseká blacklist
WHITELIST = [
    re.compile(r"^https?://(?:www\.)?olympics\.com/en(?:/|$)"), # kontrola domény
    re.compile(r"^(?:news|athletes|olympic-games)(?:/.*)?$"),   # /en/news/*, /en/athletes/*, /en/olympic-games/*
    re.compile(r"^sports/[^/]+/.+$"),   # /en/sports/*sport*/*
]
BLACKLIST = [
    re.compile(r"/sign-?in(?:/|[?#]|$)"),   # sign-in
    re.compile(r"/videos?(?:/|[?#]|$)"),    # /video or /videos (full section)
    re.compile(r"/films?(?:/|[?#]|$)"), # /film or /films (full section)
    re.compile(r"/podcast(?:/|[?#]|$)"),    # /podcast or /podcast/...
    re.compile(r"/live(?:/|[?#]|$)"),   # /live or /live/
]
ATHLETES = re.compile(r"^https?://(?:www\.)?olympics\.com/en/athletes/[a-z0-9-]+/?$")

class URLManager:
    def __init__(self,root_url,save_path):
        self.seen = set()
        self.links = deque()
        self.sources = deque()
        self.source_cap = 2000
        self.serialize_counter = 0
        self.save_path = save_path
        self.root_url = root_url
        if os.path.exists(self.save_path):
            self.load()
        else:
            self.seed()
            self.serialize()

    def normalize(self,url): # Primárne pre odstránenie www a fragmentov
        s = urlsplit(url)
        host = (s.hostname or "")
        if host.lower().startswith("www."):
            host = host[4:]
        return urlunsplit((s.scheme, host, s.path or "/", "", ""))

    def add(self,urls):
        changed = False
        for url in urls:
            if not url:
                continue
            url = self.normalize(url)
            if url in self.seen or not self.filter(url):
                continue
            if ATHLETES.match(url):
                self.links.append(url)
                self.seen.add(url)
                changed = True
            else:
                if len(self.sources) < self.source_cap:
                    self.sources.append(url)
                    self.seen.add(url)
                    changed = True
        if changed:
            self.serialize()
    
    def get(self):
        if self.links:
            url = self.links.popleft()
            self.serialize()
            return url, True
        if self.sources:
            url = self.sources.popleft()
            self.serialize()
            return url, False
        return None, None
    
    def filter(self, url):
        if any(rx.search(url) for rx in BLACKLIST):
            return False

        if not WHITELIST[0].match(url): # Kontrola domény
            return False
        
        path = url.split("/en/", 1)[-1]

        return any(rx.match(path) for rx in WHITELIST[1:])
    
    def serialize(self, override = False):
        self.serialize_counter += 1
        if self.serialize_counter % 25 != 0 and not override:
                return
        data = {
            "queue": list(self.links),
            "sources": list(self.sources),
            "seen": list(self.seen),
        }
        s = json.dumps(data, ensure_ascii=False, indent=0)
        with open(self.save_path, "w", encoding="utf-8") as f:
            f.write(s)

    def seed(self):
        if not self.seen and not self.links and not self.sources:
            self.seen.add(self.root_url)
            self.sources.append(self.root_url)

    def load(self):
        if not os.path.exists(self.save_path):
            self.seed()
            self.serialize()
            return
        
        p = self.save_path
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.seen = set(data.get("seen", []))
        self.links = deque(data.get("queue", []))
        self.sources = deque(data.get("sources", []))

        self.seed()

        self.serialize()
        