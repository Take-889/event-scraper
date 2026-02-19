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
        "User-Agent": "Mozilla/5.0 (compatible; EventsAggregator/1.5; +https://example.org)",
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
    '2/18 水-2/19 木' など -> (YYYY-MM-DD, YYYY-MM-DD)
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
    - calendar.php の検索結果をそのままテーブルから抽出
    - 列マッピング: 1列目=選択, 2列目=イベント名, 3=年, 4=会期, 5=場所
      （DEBUG 実DOMで確認済み）  [1](https://nikkeikin-my.sharepoint.com/personal/satoshi-takeda_nikkeikin_co_jp/Documents/Microsoft%20Copilot%20Chat%20%E3%83%95%E3%82%A1%E3%82%A4%E3%83%AB/_debug_kagaku_page_parse.html)
    """
    session = make_session()

    url_page = (
        "https://www.kagaku.com/calendar.php"
        "?selectgenre=society_all&selectpref=all_area&submit=%B8%A1%BA%F7&eid=none"
    )

    rows = []
    try:
        html = get_html(url_page, session)
        _save_debug("kagaku_page_parse", html)
        soup = BeautifulSoup(html, "lxml")  # lxml パーサー

        # 「イベント／会期」などのラベルを含む表だけを対象
        candidate_tables = []
        for tb in soup.find_all("table"):
            txt = tb.get_text(" ", strip=True)
            if ("イベント" in txt or "イベントの名称" in txt) and ("会期" in txt or "場所" in txt):
                candidate_tables.append(tb)

        for tb in candidate_tables:
            for tr in tb.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 5:
                    continue  # 欠損行やヘッダ様行は除外

                # 列から素直に取る（a/imgに依存しない）
                title = tds[1].get_text(" ", strip=True)
                date_text = tds[3].get_text(" ", strip=True)
                venue = tds[4].get_text(" ", strip=True)

                # 公式リンク（2列目の a[href] のうち外部URLらしいもの）
                link = None
                for a in tds[1].find_all("a", href=True):
                    href = a["href"]
                    if href.startswith(("http://", "https://")):
                        link = href
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


# ---------------- Site B: Tokyo Big Sight ----------------
def fetch_bigsight(url="https://www.bigsight.jp/visitor/event/search.php?page=1"):
    """
    東京ビッグサイト:
    - <article class="lyt-event-01"> を列挙
    - 開催期間は dt のラベル正規化でマッチ（開催期間/会期/開催日程/期間）
      → 直後の dd を優先。失敗時は dd 群を正規表現でフォールバック抽出
    - URL は dt=URL の a[href] を優先、無ければ記事内の外部リンク先頭
    - ページャ「次へ」を Referer 付きで最後まで
    """
    session = make_session()

    def _to_abs(u, ctx):
        if not u:
            return None
        absu = urljoin(ctx, u)
        return absu if absu.startswith(("http://", "https://")) else None

    def _norm_label(s: str) -> str:
        if not s:
            return ""
        s = s.replace("：", ":")
        s = re.sub(r"\s+", "", s)
        return s

    LABELS_DATE = {_norm_label(x) for x in ["開催期間", "会期", "開催日程", "期間"]}
    LABEL_URL   = _norm_label("URL")
    LABEL_VENUE = _norm_label("利用施設")

    def _dt_to_dd_texts(dl_node):
        """
        dl ノードから {正規化ラベル: ddテキスト} を作る。
        div の有無に依存せず、dt → 直近の next_sibling dd を辿る。
        """
        out = {}
        # まず全 dt を順に回し、それぞれに最初の dd 兄弟を紐づけ
        for dt in dl_node.find_all("dt"):
            lab = _norm_label(dt.get_text(" ", strip=True))
            dd = dt.find_next_sibling("dd")
            if lab and dd:
                out[lab] = dd.get_text(" ", strip=True)
        return out

    def _pick_date(art):
        dl = art.find("dl", class_="list-01")
        if not dl:
            return None
        pairs = _dt_to_dd_texts(dl)
        # 1) ラベルから優先取得
        for lab in LABELS_DATE:
            if lab in pairs and pairs[lab]:
                return pairs[lab]
        # 2) フォールバック：dd 群から日付パターン抽出
        for dd in dl.find_all("dd"):
            txt = dd.get_text(" ", strip=True)
            if re.search(r'\d{4}年?\d{1,2}月?\d{1,2}日?.*?(〜|～|-|－|—|–|―).*?\d{1,2}月?\d{1,2}日?', txt):
                return txt
            if re.search(r'\d{4}年?\d{1,2}月?\d{1,2}日?', txt):  # 単発日も許容
                return txt
        return None

    def _pick_official_url(art, ctx):
        dl = art.find("dl", class_="list-01")
        if dl:
            pairs = _dt_to_dd_texts(dl)
            if LABEL_URL in pairs:
                dd_text = pairs[LABEL_URL]
                a = dl.find("a", href=True)
                # dd 内の最初の a[href]
                for a in dl.find_all("a", href=True):
                    u = _to_abs(a.get("href"), ctx)
                    if u:
                        return u
        # 記事内の外部リンク先頭
        for a in art.find_all("a", href=True):
            u = _to_abs(a.get("href"), ctx)
            if u and not u.startswith(("https://www.bigsight.jp", "http://www.bigsight.jp")):
                return u
        return None

    def _pick_venue(art):
        dl = art.find("dl", class_="list-01")
        if not dl:
            return None
        pairs = _dt_to_dd_texts(dl)
        return pairs.get(LABEL_VENUE)

    rows, seen_pages = [], set()
    page_url, referer = url, None

    while page_url and page_url not in seen_pages:
        seen_pages.add(page_url)
        headers = {"Referer": referer} if referer else None
        try:
            html = get_html(page_url, session, headers=headers)
        except Exception as e:
            logger.warning("bigsight fetch error on %s: %s", page_url, e)
            break

        _save_debug(f"bigsight_{len(seen_pages):02d}", html)
        soup = BeautifulSoup(html, "lxml")

        arts = soup.select("article.lyt-event-01")
        logger.info("bigsight: %s -> articles=%d (accum=%d)", page_url, len(arts), len(rows))

        for art in arts:
            # タイトル
            h3 = art.find("h3", class_="hdg-01")
            a_title = h3.find("a", href=True) if h3 else None
            title = a_title.get_text(" ", strip=True) if a_title else (h3.get_text(" ", strip=True) if h3 else None)

            # 開催期間 → 日付解析
            date_text = _pick_date(art)
            start, end = parse_date_range(date_text or "")

            # 公式URL・会場
            link = _pick_official_url(art, page_url)
            venue = _pick_venue(art)

            # 取りこぼし検知のためのデバッグ
            if not date_text:
                logger.debug("bigsight: no date text for title=%r on page=%s", title, page_url)

            if title and (start or end):
                rows.append({
                    "source": "bigsight",
                    "title": title,
                    "start_date": start,
                    "end_date": end,
                    "venue": venue,
                    "url": link
                })

        # 次へ（Referer を直前ページに）
        next_a = soup.select_one("div.list-pager-01 p.next a[href]")
        referer = page_url
        page_url = _to_abs(next_a["href"], page_url) if next_a else None

    return pd.DataFrame(rows)


# ---------------- Site C: Makuhari Messe (print) ----------------
def fetch_makuhari(url="https://www.m-messe.co.jp/event/print"):
    """
    幕張メッセ印刷用ページ（表）から抽出（実DOMは印刷用テーブルで安定）  [2](https://nikkeikin-my.sharepoint.com/personal/satoshi-takeda_nikkeikin_co_jp/Documents/Microsoft%20Copilot%20Chat%20%E3%83%95%E3%82%A1%E3%82%A4%E3%83%AB/_debug_kagaku_table_after_page.html)
    """
    session = make_session()
    html = get_html(url, session)
    _save_debug("makuhari", html)
    soup = BeautifulSoup(html, "lxml")  # lxml パーサー

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


