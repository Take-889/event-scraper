# events_monthly.py
# -*- coding: utf-8 -*-

import os
import re
import logging
from datetime import datetime
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from requests.adapters import HTTPAdapter

try:
    from urllib3.util.retry import Retry
except Exception:
    Retry = None


# ---------------- Logging ----------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)
logger = logging.getLogger("events")


# ---------------- Utilities ----------------
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; EventsAggregator/1.7; +https://example.org)",
        "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
    })
    if Retry is not None:
        retries = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.7,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET", "HEAD"])
        )
        adapter = HTTPAdapter(max_retries=retries)
    else:
        adapter = HTTPAdapter()
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def get_html(url, session, timeout=30, headers=None):
    r = session.get(url, timeout=timeout, headers=headers or {})
    r.raise_for_status()
    ctype = r.headers.get("Content-Type", "")
    m = re.search(r"charset=([^\s;]+)", ctype, flags=re.I)
    enc = (m.group(1).strip() if m else None) or r.apparent_encoding or r.encoding or "utf-8"
    r.encoding = enc
    return r.text


# ---------------- Date parsing (shared) ----------------
def parse_date_range(text):
    if not text:
        return None, None

    t = str(text)
    t = re.sub(r'（.*?）', '', t)
    t = re.sub(r'\(.*?\)', '', t)
    t = re.sub(r'(月|火|水|木|金|土|日)曜?', '', t)
    t = re.sub(r'\s+', '', t)

    for ch in ['〜', '～', '-', '−', '—', '–', '－', '―']:
        t = t.replace(ch, '〜')

    t = t.replace('年', '/').replace('月', '/').replace('日', '')
    parts = t.split('〜')
    now_y = datetime.now().year

    def _norm(p, default_year=None):
        if not p:
            return None
        try:
            default = datetime(default_year or now_y, 1, 1)
            dt = dtparser.parse(p, default=default)
            return dt.strftime('%Y-%m-%d')
        except Exception:
            return None

    if len(parts) == 2:
        left, right = parts
        s = _norm(left)
        e = _norm(right, default_year=int(s.split('-')[0]) if s else now_y)
        return s, e
    else:
        d = _norm(t)
        return d, d


# ---------------- Site A: Kagaku.com ----------------
def fetch_kagaku():
    session = make_session()
    url_page = (
        "https://www.kagaku.com/calendar.php"
        "?selectgenre=society_all&selectpref=all_area&submit=%B8%A1%BA%F7&eid=none"
    )
    rows = []

    try:
        html = get_html(url_page, session)
        soup = BeautifulSoup(html, "lxml")

        candidate_tables = []
        for tb in soup.find_all("table"):
            txt = tb.get_text(" ", strip=True)
            if ("イベント" in txt or "イベントの名称" in txt) and ("会期" in txt or "場所" in txt):
                candidate_tables.append(tb)

        for tb in candidate_tables:
            for tr in tb.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 5:
                    continue

                title = tds[1].get_text(" ", strip=True)
                date_text = tds[3].get_text(" ", strip=True)
                venue = tds[4].get_text(" ", strip=True)

                link = None
                for a in tds[1].find_all("a", href=True):
                    if a["href"].startswith(("http://","https://")):
                        link = a["href"]
                        break

                start, end = parse_date_range(date_text or "")
                if title and (start or end):
                    rows.append({
                        "source": "kagaku",
                        "title": title,
                        "start_date": start,
                        "end_date": end,
                        "venue": venue,
                        "url": link
                    })

    except Exception as e:
        logger.exception("kagaku fetch failed: %s", e)

    logger.info("kagaku: parsed rows=%d", len(rows))
    return pd.DataFrame(rows)


# ---------------- Site B: Tokyo Big Sight (with page=1 retry) ----------------
def fetch_bigsight(url="https://www.bigsight.jp/visitor/event/search.php?page=1"):

    session = make_session()

    def _to_abs(u, ctx):
        if not u:
            return None
        absu = urljoin(ctx, u)
        return absu if absu.startswith(("http://","https://")) else None

    def _norm_label(s):
        if not s:
            return ""
        return re.sub(r"\s+","", s.replace("：",":"))

    LABELS_DATE = {_norm_label(x) for x in ["開催期間","会期","開催日程","期間"]}
    LABEL_URL   = _norm_label("URL")
    LABEL_VENUE = _norm_label("利用施設")

    re_range_y_to_y = re.compile(
        r'(?P<y1>\d{4})年\s*(?P<m1>\d{1,2})月\s*(?P<d1>\d{1,2})日?\s*?[〜～\-－—–―]\s*?'
        r'(?P<y2>\d{4})年\s*(?P<m2>\d{1,2})月\s*(?P<d2>\d{1,2})日?'
    )
    re_range_y_to_md = re.compile(
        r'(?P<y1>\d{4})年\s*(?P<m1>\d{1,2})月\s*(?P<d1>\d{1,2})日?\s*?[〜～\-－—–―]\s*?'
        r'(?P<m2>\d{1,2})月\s*(?P<d2>\d{1,2})日?'
    )
    re_single_y = re.compile(r'(?P<y1>\d{4})年\s*(?P<m1>\d{1,2})月\s*(?P<d1>\d{1,2})日?')

    def _to_iso(y,m,d):
        try:
            return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
        except:
            return None

    def _find_dates(text):
        if not text:
            return None, None
        t = re.sub(r'（.*?）','', text)

        m = re_range_y_to_y.search(t)
        if m:
            return (
                _to_iso(m.group("y1"),m.group("m1"),m.group("d1")),
                _to_iso(m.group("y2"),m.group("m2"),m.group("d2"))
            )

        m = re_range_y_to_md.search(t)
        if m:
            left = _to_iso(m.group("y1"),m.group("m1"),m.group("d1"))
            right = _to_iso(m.group("y1"),m.group("m2"),m.group("d2"))
            return (left, right)

        m = re_single_y.search(t)
        if m:
            d = _to_iso(m.group("y1"),m.group("m1"),m.group("d1"))
            return (d, d)

        return None, None

    def _parse_article(art, page_url, prefer_fulltext=False):
        h3 = art.find("h3", class_="hdg-01")
        a_title = h3.find("a", href=True) if h3 else None
        title = a_title.get_text(" ", strip=True) if a_title else (h3.get_text(" ", strip=True) if h3 else None)

        start,end = None,None

        if not prefer_fulltext:
            dl = art.find("dl", class_="list-01")
            if dl:
                pairs = {}
                for dt in dl.find_all("dt"):
                    lab = _norm_label(dt.get_text(" ", strip=True))
                    dd = dt.find_next_sibling("dd")
                    if lab and dd:
                        pairs[lab] = dd.get_text(" ", strip=True)

                for lab in LABELS_DATE:
                    if lab in pairs:
                        s,e = _find_dates(pairs[lab])
                        if s or e:
                            start,end = s,e
                            break

                if not (start or end):
                    for dd in dl.find_all("dd"):
                        s,e = _find_dates(dd.get_text(" ", strip=True))
                        if s or e:
                            start,end = s,e
                            break

        if not (start or end):
            s,e = _find_dates(art.get_text(" ", strip=True))
            if s or e:
                start,end = s,e

        link = None
        dl = art.find("dl", class_="list-01")
        if dl:
            for dt in dl.find_all("dt"):
                if _norm_label(dt.get_text(strip=True)) == LABEL_URL:
                    dd = dt.find_next_sibling("dd")
                    if dd:
                        for a in dd.find_all("a", href=True):
                            u = _to_abs(a.get("href"), page_url)
                            if u:
                                link = u
                                break
                    break

        if not link:
            for a in art.find_all("a", href=True):
                u = _to_abs(a.get("href"), page_url)
                if u and not u.startswith(("https://www.bigsight.jp","http://www.bigsight.jp")):
                    link = u
                    break

        venue = None
        if dl:
            for dt in dl.find_all("dt"):
                if _norm_label(dt.get_text(strip=True)) == LABEL_VENUE:
                    dd = dt.find_next_sibling("dd")
                    if dd:
                        venue = dd.get_text(" ", strip=True)
                    break
            if not venue:
                for dd in dl.find_all("dd"):
                    t = dd.get_text(" ", strip=True)
                    if any(k in t for k in ["ホール","会議棟","会場"]):
                        venue = t
                        break

        if title and (start or end):
            return {
                "source":"bigsight",
                "title":title,
                "start_date":start,
                "end_date":end,
                "venue":venue,
                "url":link
            }
        return None

    def _scrape_page(page_url, referer=None, prefer_fulltext=False):
        headers = {"Referer": referer} if referer else None
        html = get_html(page_url, session, headers=headers)
        soup = BeautifulSoup(html, "lxml")

        arts = soup.select("main.event article.lyt-event-01, article.lyt-event-01")
        out=[]
        for art in arts:
            r = _parse_article(art, page_url, prefer_fulltext)
            if r:
                out.append(r)
        return out, soup

    rows, seen_pages = [], set()
    page_index = 0
    page1_rows = 0
    page_url = url
    referer = None

    while page_url and page_url not in seen_pages:
        seen_pages.add(page_url)
        page_index += 1

        page_rows, soup = _scrape_page(page_url, referer, prefer_fulltext=False)
        rows.extend(page_rows)
        if page_index == 1:
            page1_rows = len(page_rows)

        arts_cnt = len(soup.select("article.lyt-event-01"))
        logger.info("bigsight: %s -> articles=%d (accum=%d)", page_url, arts_cnt, len(rows))

        next_a = soup.select_one("div.list-pager-01 p.next a[href]")
        referer = page_url
        page_url = _to_abs(next_a["href"], page_url) if next_a else None

    if page1_rows == 0:

