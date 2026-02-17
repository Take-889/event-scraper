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
    # urllib3 v1/v2 両対応
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

DEBUG_HTML = os.getenv("DEBUG_HTML", "0") == "1"


# ---------------- Utilities ----------------
def _save_debug(name: str, text: str) -> None:
    """
    Save HTML for debugging (optional via DEBUG_HTML=1).
    """
    if not DEBUG_HTML:
        return
    fn = f"_debug_{name}.html"
    with open(fn, "w", encoding="utf-8") as f:
        f.write(text)
    logger.debug("Saved debug HTML: %s", fn)


def make_session() -> requests.Session:
    """
    Make a requests Session with retries and default headers.
    """
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; EventsAggregator/1.1; +https://example.org)"
    })
    # リトライは利用可能な場合のみ設定
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


def get_html(url: str, session: requests.Session, timeout: int = 30) -> str:
    """
    Fetch HTML with robust encoding decision:
    1) charset from Content-Type
    2) apparent_encoding
    3) response.encoding
    4) utf-8 (fallback)
    """
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    ctype = r.headers.get("Content-Type", "")
    m = re.search(r"charset=([^\s;]+)", ctype, flags=re.I)
    enc = (m.group(1).strip() if m else None) or r.apparent_encoding or r.encoding or "utf-8"
    r.encoding = enc
    return r.text


# ---------------- Date parsing ----------------
def parse_date_range(text: str):
    """
    '2026年02月18日（水）～2026年02月20日（金）'
    '2/18 水-2/20 金'
    -> (YYYY-MM-DD, YYYY-MM-DD)
    """
    if not text:
        return None, None

    t = str(text)

    # 括弧内削除（全角/半角）
    t = re.sub(r'（.*?）', '', t)
    t = re.sub(r'\(.*?\)', '', t)

    # 曜日・空白の削除
    t = re.sub(r'(月|火|水|木|金|土|日)曜?', '', t)
    t = re.sub(r'\s+', '', t)

    # 区切り統一（各種ダッシュ/チルダを 〜 に寄せる）
    for ch in ['〜', '～', '-', '−', '—', '–', '－', '―']:
        t = t.replace(ch, '〜')

    # 和文年月日をスラッシュ化（限定的に）
    t = t.replace('年', '/').replace('月', '/').replace('日', '')

    parts = t.split('〜')
    now_y = datetime.now().year

    def _norm(p: str, default_year: int | None = None):
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
        year_for_right = int(s.split('-')[0]) if s else now_y
        e = _norm(right, default_year=year_for_right)
        return s, e
    else:
        d = _norm(t)
        return d, d


# ---------------- Site A: Kagaku.com ----------------
def fetch_kagaku(
    url: str = "https://www.kagaku.com/calendar.php?selectgenre=society_all&selectpref=all_area&submit=%B8%A1%BA%F7&eid=none"
) -> pd.DataFrame:
    """
    科学カレンダーから イベント名/会期/会場/URL を抽出
    """
    session = make_session()
    html = get_html(url, session)
    _save_debug("kagaku", html)
    soup = BeautifulSoup(html, "html.parser")

    rows = []
    tables = soup.find_all("table")
    for tb in tables:
        # 表のどこかにキーワードが含まれていれば候補に
        whole_txt = tb.get_text(" ", strip=True)
        if not any(k in whole_txt for k in ["イベント", "イベント名", "イベントの名称", "会期"]):
            continue

        for tr in tb.find_all("tr"):
            tds = tr.find_all(["td", "th"])
            if len(tds) < 2:
                continue

            # 見出し行スキップ
            joined = " ".join(td.get_text(" ", strip=True) for td in tds)
            if any(k in joined for k in ["イベント", "会期"]) and tr.find("th"):
                continue

            texts = [td.get_text(" ", strip=True) for td in tds]
            title = texts[0] if texts else None

            # URL（行内の最初のリンクを採用）
            a = tr.find("a", href=True)
            link = urljoin(url, a["href"]) if a and a.get("href") else None
            if link and not link.startswith(("http://", "https://")):
                link = None

            # 会期候補
            date_text = None
            for tx in texts:
                if re.search(r'\d{1,2}/\d{1,2}', tx) or ('年' in tx and '月' in tx):
                    date_text = tx
                    break

            start, end = parse_date_range(date_text or "")
            venue = texts[-1] if texts else None

            if title and (start or end):
                rows.append({
                    "source": "kagaku",
                    "title": title,
                    "start_date": start,
                    "end_date": end,
                    "venue": venue,
                    "url": link
                })

    return pd.DataFrame(rows)


# ---------------- Site B: Tokyo Big Sight ----------------
def fetch_bigsight(url: str = "https://www.bigsight.jp/visitor/event/") -> pd.DataFrame:
    """
    東京ビッグサイトのイベント一覧（複数ページ）から抽出
    """
    session = make_session()
    events = []
    page = 1

    while True:
        u = url if page == 1 else f"{url}?page={page}"
        try:
            html = get_html(u, session)
        except requests.HTTPError as e:
            logger.warning("bigsight HTTP error on page %s: %s", page, e)
            break

        _save_debug(f"bigsight_p{page}", html)
        soup = BeautifulSoup(html, "html.parser")

        # 構造が変わる場合を考慮し、複数候補のセレクタを定義
        cards = soup.select("div.l-event__item, li.l-event__item")
        if not cards:
            # 末尾もしくは構造変化
            if page == 1:
                logger.warning("bigsight: no cards found on first page")
            break

        for c in cards:
            # タイトル
            ttl_el = c.select_one(".l-event__ttl, h3, .title, .event-title")
            title = ttl_el.get_text(" ", strip=True) if ttl_el else None

            # 日付要素（複数候補）
            date_el = c.select_one(".l-event__date, .date, .event-date")
            date_text = date_el.get_text(" ", strip=True) if date_el else c.get_text(" ", strip=True)

            # 代表的な日付パターンを抜き出してパース
            m = re.search(r'(\d{4}年?\d{1,2}月?\d{1,2}日?.*?〜?.*?\d{1,2}月?\d{1,2}日?)', date_text)
            if not m:
                # 年が省略されるケース用フォールバック（例：2/18〜2/20）
                m = re.search(r'(\d{1,2}/\d{1,2}.*?〜.*?\d{1,2}/\d{1,2})', date_text)
            start, end = parse_date_range(m.group(1)) if m else (None, None)

            # URL：内部の詳細ページ優先、なければ最初の a[href]
            detail = c.find("a", href=True)
            link = urljoin(url, detail["href"]) if detail else None

            if title and (start or end):
                events.append({
                    "source": "bigsight",
                    "title": title,
                    "start_date": start,
                    "end_date": end,
                    "venue": None,
                    "url": link
                })

        page += 1

    return pd.DataFrame(events)


# ---------------- Site C: Makuhari Messe (print) ----------------
def fetch_makuhari(url: str = "https://www.m-messe.co.jp/event/print") -> pd.DataFrame:
    """
    幕張メッセ印刷用ページ（表）から抽出
    """
    session = make_session()
    html = get_html(url, session)
    _save_debug("makuhari", html)
    soup = BeautifulSoup(html, "html.parser")

    rows = []
    table = soup.find("table")
    if not table:
        return pd.DataFrame(rows)

    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        vals = [td.get_text(" ", strip=True) for td in tds]
        # 想定：0=会期、1=イベント名、2=主催/会場など…
        date_text = vals[0]
        title = vals[1] if len(vals) > 1 else None
        venue = vals[2] if len(vals) > 2 else None

        # aタグ or テキスト中URL を拾う
        a = tr.find("a", href=True)
        link = None
        if a and a.get("href"):
            link = urljoin(url, a["href"])
        if not link:
            tail = " ".join(vals[3:]) if len(vals) > 3 else ""
            murl = re.search(r'(https?://[^\s]+)', tail)
            link = murl.group(1) if murl else None

        start, end = parse_date_range(date_text)
        if title and (start or end):
            rows.append({
                "source": "makuhari",
                "title": title,
                "start_date": start,
                "end_date": end,
                "venue": venue,
                "url": link
            })

    return pd.DataFrame(rows)


# ---------------- Aggregation & Output ----------------
def collect_all() -> pd.DataFrame:
    dfs = []
    for fetcher in (fetch_kagaku, fetch_bigsight, fetch_makuhari):
        try:
            df = fetcher()
            if df is not None and not df.empty:
                dfs.append(df)
                logger.info("%s: %d rows", fetcher.__name__, len(df))
            else:
                logger.warning("%s: empty", fetcher.__name__)
        except Exception as e:
            logger.exception("%s failed: %s", fetcher.__name__, e)

    if not dfs:
        return pd.DataFrame(columns=["source", "title", "start_date", "end_date", "venue", "url"])

    out = pd.concat(dfs, ignore_index=True)

    # 列そろえ
    keep_cols = ["source", "title", "start_date", "end_date", "venue", "url"]
    for col in keep_cols:
        if col not in out.columns:
            out[col] = None
    out = out[keep_cols].copy()

    # 併合日
    out["last_seen_at"] = datetime.now().strftime("%Y-%m-%d")

    # 重複除去：source + title + start_date + url をキーに
    out = out.drop_duplicates(subset=["source", "title", "start_date", "url"])
    return out


def monthly_run(output_csv: str = "events_agg.csv") -> None:
    df = collect_all()
    df.to_csv(output_csv, index=False, encoding="utf-8")
    logger.info("Saved: %s (%d rows)", output_csv, len(df))


if __name__ == "__main__":
    monthly_run()
