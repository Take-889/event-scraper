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
    # urllib3 v1/v2 両対応（無ければフォールバック）
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
def _save_debug(name, text):
    """Save HTML for debugging (optional via DEBUG_HTML=1)."""
    if not DEBUG_HTML:
        return
    fn = f"_debug_{name}.html"
    with open(fn, "w", encoding="utf-8") as f:
        f.write(text)
    logger.debug("Saved debug HTML: %s", fn)


def make_session():
    """Make a requests Session with retries and default headers."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; EventsAggregator/1.4; +https://example.org)",
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
    """
    Robust HTML fetch:
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
def parse_date_range(text):
    """
    '2026年02月18日（水）～2026年02月20日（金）'
    '2/18 水-2/20 金' など -> (YYYY-MM-DD, YYYY-MM-DD)
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
        year_for_right = int(s.split('-')[0]) if s else now_y
        e = _norm(right, default_year=year_for_right)
        return s, e
    else:
        d = _norm(t)
        return d, d


# ---------------- Site A: Kagaku.com ----------------
def fetch_kagaku():
    """
    科学カレンダー:
    - まず calendar.php を叩いて検索条件をセッションに反映
    - 同じ Session で calendartable.php を取得してテーブルをパース
    - 0件なら最終フォールバックとして calendar.php 自体から抽出
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

    def _parse_tables(soup, url_ctx):
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

    rows = []

    try:
        # 1) calendar.php を先にアクセス（同一セッションで条件を保持）
        _ = get_html(url_page, session)
        # 2) calendartable.php を同セッションで取得（Referer 付与）
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
def fetch_bigsight(url="https://www.bigsight.jp/visitor/event/search.php?page=1"):
    """
    東京ビッグサイト:
    - 起点は search.php?page=1
    - 各 <article class="lyt-event-01"> の中から
      タイトル, 開催期間(ddテキストから日付パターンで), 公式URL(「URL」ラベル or 最初の a) を抽出
    - ページャの「次へ」を追って最後まで
    """
    session = make_session()
    rows = []
    seen_pages = set()
    page_url = url

    def _to_abs(u, ctx):
        if not u:
            return None
        absu = urljoin(ctx, u)
        return absu if absu.startswith(("http://", "https://")) else None

    def _pick_date_from_dds(art):
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

    def _pick_official_url(art, ctx):
        # まず「URL」ラベルの dd にある a[href]
        for div in art.select("dl.list-01 div"):
            dt_el, dd_el = div.find("dt"), div.find("dd")
            if not dt_el or not dd_el:
                continue
            if "URL" in dt_el.get_text(strip=True):
                a = dd_el.find("a", href=True)
                if a and a.get("href"):
                    u = _to_abs(a["href"], ctx)
                    if u:
                        return u
        # 無ければ記事内の最初の a[href]
        a = art.find("a", href=True)
        return _to_abs(a["href"], ctx) if a else None

    def _pick_venue(art):
        # 「利用施設」を venue として取得（存在しない場合は None）
        for div in art.select("dl.list-01 div"):
            dt_el, dd_el = div.find("dt"), div.find("dd")
            if not dt_el or not dd_el:
                continue
            if "利用施設" in dt_el.get_text(strip=True):
                return dd_el.get_text(" ", strip=True)
        return None

    while page_url and page_url not in seen_pages:
        seen_pages.add(page_url)
        try:
            html = get_html(page_url, session)
        except Exception as e:
            logger.warning("bigsight fetch error on %s: %s", page_url, e)
            break

        _save_debug(f"bigsight_{len(seen_pages):02d}", html)
        soup = BeautifulSoup(html, "html.parser")

        articles = soup.select("article.lyt-event-01")
        for art in articles:
            # タイトル
            h3 = art.find("h3", class_="hdg-01")
            a_title = h3.find("a", href=True) if h3 else None
            title = a_title.get_text(" ", strip=True) if a_title else (h3.get_text(" ", strip=True) if h3 else None)

            # 日付（dd 群から日付パターンで抽出）
            date_text = _pick_date_from_dds(art)
            start, end = parse_date_range(date_text or "")

            # 公式 URL（「URL」欄 → 無ければ記事内先頭 a）
            link = _pick_official_url(art, page_url)

            # 会場（任意）
            venue = _pick_venue(art)

            if title and (start or end):
                rows.append({
                    "source": "bigsight",
                    "title": title,
                    "start_date": start,
                    "end_date": end,
                    "venue": venue,
                    "url": link
                })

        # 次へ（存在すれば続行）
        next_a = soup.select_one("div.list-pager-01 p.next a[href]")
        page_url = _to_abs(next_a["href"], page_url) if next_a else None

    return pd.DataFrame(rows)


# ---------------- Site C: Makuhari Messe (print) ----------------
def fetch_makuhari(url="https://www.m-messe.co.jp/event/print"):
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
def collect_all():
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


def monthly_run(output_csv="events_agg.csv"):
    df = collect_all()
    df.to_csv(output_csv, index=False, encoding="utf-8")
    logger.info("Saved: %s (%d rows)", output_csv, len(df))


if __name__ == "__main__":
    monthly_run()
