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
    """Save HTML for debugging (optional via DEBUG_HTML=1)."""
    if not DEBUG_HTML:
        return
    fn = f"_debug_{name}.html"
    with open(fn, "w", encoding="utf-8") as f:
        f.write(text)
    logger.debug("Saved debug HTML: %s", fn)


def make_session() -> requests.Session:
    """Make a requests Session with retries and default headers."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; EventsAggregator/1.3; +https://example.org)",
        "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
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


def get_html(url: str, session: requests.Session, timeout: int = 30, headers: dict | None = None) -> str:
    """
    Fetch HTML with robust encoding decision:
    1) charset from Content-Type
    2) apparent_encoding
    3) response.encoding
    4) utf-8 (fallback)
    """
    r = session.get(url, timeout=timeout, headers=headers or {})
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
def fetch_kagaku() -> pd.DataFrame:
    """
    科学カレンダー:
    - まず calendar.php を叩いて検索条件をセッションに反映
    - 同じ Session で calendartable.php を取得してテーブルをパース
    - 0件なら最終フォールバックとして calendar.php 自体からも抽出を試みる
    """
    session = make_session()

    url_page = (
        "https://www.kagaku.com/calendar.php"
        "?selectgenre=society_all&selectpref=all_area&submit=%B8%A1%BA%F7&eid=none"
    )
    url_table = (
        "https://www.kagaku.com/calendartable.php"
        "?selectgenre=society_all&selectpref=all_area&eid=none"
    )

    def _parse_tables(soup: BeautifulSoup, url_ctx: str) -> list[dict]:
        out = []
        for tb in soup.find_all("table"):
            txt = tb.get_text(" ", strip=True)
            if not any(k in txt for k in ["イベント", "イベント名", "イベントの名称", "会期"]):
                continue

            # th（ヘッダ）を除いた実体行のみ
            for tr in tb.find_all("tr"):
                if tr.find("th"):
                    continue
                tds = tr.find_all("td")
                if len(tds) < 2:
                    continue

                cells = [td.get_text(" ", strip=True) for td in tds]
                # タイトルとURL（行内の最初の a[href]）
                a = tr.find("a", href=True)
                title = a.get_text(" ", strip=True) if a else (cells[0] if cells else None)
                link = urljoin(url_ctx, a["href"]) if (a and a.get("href")) else None
                if link and not link.startswith(("http://", "https://")):
                    link = None

                # 会期候補（セル内から日付らしさで）
                date_text = None
                for c in cells:
                    if re.search(r'\d{4}年?\d{1,2}月?\d{1,2}日?', c) or re.search(r'\d{1,2}/\d{1,2}', c):
                        date_text = c
                        break
                start, end = parse_date_range(date_text or "")

                # 場所（終端列を優先）
                venue = cells[-1] if cells else None

                if title and (start or end):
                    out.append({
                        "source": "kagaku",
                        "title": title,
                        "start_date": start,
                        "end_date": end,
                        "venue": venue,
                        "url": link
                    })
        return out

    rows: list[dict] = []

    try:
        # 1) calendar.php を先にアクセス（同一セッションで条件を保持）
        _ = get_html(url_page, session)
        # 2) calendartable.php を同セッションで取得（Referer も付与）
        html = get_html(url_table, session, headers={"Referer": url_page})
        _save_debug("kagaku_table_after_page", html)
        soup = BeautifulSoup(html, "html.parser")
        rows.extend(_parse_tables(soup, url_table))
    except Exception as e:
        logger.exception("kagaku fetch failed: %s", e)

    if not rows:
        # 最終フォールバック：calendar.php 自体のページからも探す
        try:
            html = get_html(url_page, session)
            _save_debug("kagaku_page_parse", html)
            soup = BeautifulSoup(html, "html.parser")
            rows.extend(_parse_tables(soup, url_page))
        except Exception as e2:
            logger.warning("kagaku page fallback failed: %s", e2)

    return pd.DataFrame(rows)


# ---------------- Site B: Tokyo Big Sight ----------------
def fetch_bigsight(url: str = "https://www.bigsight.jp/visitor/event/search.php?page=1") -> pd.DataFrame:
    """
    東京ビッグサイト:
    - 起点は search.php?page=1
    - 各 <article class="lyt-event-01"> の中から
      タイトル, 開催期間(ddのテキストから日付パターンで), 公式URL(「URL」ラベル or 最初の a) を抽出
    - ページャの「次へ」を追って最後まで
    """
    session = make_session()
    rows: list[dict] = []
    seen_pages: set[str] = set()
    page_url = url

    def _to_abs(u: str | None, ctx: str) -> str | None:
        if not u:
            return None
        absu = urljoin(ctx, u)
        return absu if absu.startswith(("http://", "https://")) else None

    def _pick_date_from_dds(art: BeautifulSoup) -> str | None:
        # dl.list-01 内の dd を総当たりでチェック
        for dd in art.select("dl.list-01 dd"):
            txt = dd.get_text(" ", strip=True)
            # 期間（年付き→年なしフォールバック→単発）
            if re.search(r'\d{4}年?\d{1,2}月?\d{1,2}日?.*?(〜|～|-|－|—|–|―).*?\d{1,2}月?\d{1,2}日?', txt):
                return txt
            if re.search(r'\d{1,2}/\d{1,2}.*?(〜|～|-|－|—|–|―).*?\d{1,2}/\d{1,2}', txt):
                return txt
            if re.search(r'\d{4}年?\d{1,2}月?\d{1,2}日?', txt):  # 単発日
                return txt
        return None

    def _pick_official_url(art: BeautifulSoup, ctx: str) -> str | None:
        # まず「URL」ラベルの dd にある a[href]
        for div in art.select("dl.list-01 div"):

