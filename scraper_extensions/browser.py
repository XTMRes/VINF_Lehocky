from playwright.async_api import async_playwright
import re

# User agent pre headery GET requestu.
# Kopírované priamo z prehliadača po requeste
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/140.0.0.0 Safari/537.36"
    "(FIIT-Research)"
)

# Match pre <a> tagy obsahujúce href pre BFS
# Ignorecase pre duplicity, dotall pre newline v <a>
HREF_REGEX = re.compile(
    r"""<a\b[^>]*?\bhref\s*=\s*(['"])(.*?)\1""",re.IGNORECASE | re.DOTALL
)

class Browser:
    def __init__(self):
        self.pw = self.browser = self.context = None
        self.href_regex = HREF_REGEX

    async def __aenter__(self):
        self.pw = await async_playwright().start()
        self.browser = await self.pw.chromium.launch(
            headless=True,
            channel="chrome"
        )
        # GET request headery
        self.context = await self.browser.new_context(
            user_agent=USER_AGENT,
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "X-Research-Purpose": "Non-commercial educational research from FIIT STU",
            },
            locale="en-US",
            ignore_https_errors=True,
        )
        self.page = await self.context.new_page()
        # Filtrácia veľkých objektov
        async def handle_route(route):
            rt = route.request.resource_type
            if rt in {"media", "font"}:
                return await route.abort()
            return await route.continue_()
        await self.context.route("**/*", handle_route)

        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.page:    await self.page.close()
        if self.context: await self.context.close()
        if self.browser: await self.browser.close()
        if self.pw:      await self.pw.stop()

    async def fetch(self, url):
        try:
            resp = await self.page.goto(url, wait_until="domcontentloaded", timeout=8_000)
            status = resp.status if resp else None
            try:
                await self.page.wait_for_load_state("networkidle", timeout=1_500)
            except: 
                pass
            html = await self.page.content()
            pairs = self.href_regex.findall(html)   # [('"', 'URL'), ...] - 2 páry lebo sa hľadajú aj href bez zátvoriek pre istotu
            hrefs = [url for _, url in pairs]  # len URL
            return html, hrefs, status
        
        finally:
            pass
