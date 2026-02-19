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
    from urllib3.util.retry import Retry  # urllib3 v1/v2 両対応
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
    if not DEBUG_HTML:
        return
    fn = f"_debug_{name}.html"
    with open(fn, "w", encoding="utf-8") as f:
        f.write(text)
    logger.debug("Saved debug HTML: %s", fn)


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; EventsAggregator/1.2; +https://example.org)",
        "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
    })
    adapter = HTTPAdapter(max_retries=Retry(
        total=3, connect=3, read=3, backoff_factor=0.7,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"])
    )) if Retry else HTTPAdapter()
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def get_html(url: str, session: requests.Session, timeout: int = 30) -> str:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    ctype = r.headers.get("Content-Type", "")
    m = re.search(r"charset=([^\s;]+)", ctype, flags=re.I)
    enc = (m.group(1).strip() if m else None) or r.apparent_encoding or r.encoding or "utf-8"
    r.encoding = enc
    return r.text


# ---------------- Date parsing ----------------
def parse_date_range(text: str):
    if not text:
        return None, None

    t = str(text)
    # 括弧内削除（全角/半角）
    t = re.sub(r'（.*?）', '', t)
    t = re.sub(r'\(.*?\)', '', t)
    # 曜日・空白の削除
    t = re.sub(r'(月|火|水|木|金|土|日)曜?', '', t)
    t = re.sub(r'\s+', '', t)
    # 区切り統一
    for ch in ['〜', '～', '-', '−', '—', '–', '－', '―']:
        t = t.replace(ch, '〜')
    # 和文年月日をスラッシュ化
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
    1) calendar.php の検索結果ページを優先的にパース
    2) だめなら calendartable.php をフォールバック
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
            # 表内に「イベント」や「会期」が含まれるものに限定
            txt = tb.get_text(" ", strip=True)
            if not any(k in txt for k in ["イベント", "イベント名", "イベントの名称", "会期"]):
                continue

            # 行を走査（見出し行=th をスキップ）
            for tr in tb.find_all("tr"):
                if tr.find("th"):
                    continue
                tds = tr.find_all("td")
                if len(tds) < 2:
                    continue

                cells = [td.get_text(" ", strip=True) for td in tds]
                # タイトルとURL
                a = tr.find("a", href=True)
                title = a.get_text(" ", strip=True) if a else (cells[0] if cells else None)
                link = urljoin(url_ctx, a["href"]) if (a and a.get("href")) else None
                if link and not link.startswith(("http://", "https://")):
                    link = None

                # 会期候補（セル内いずれかに「年/月」相当が入っている）
                date_text = None
                for c in cells:
                    if re.search(r'\d{1,2}/\d{1,2}', c) or ('年' in c and '月' in c):
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

    # 1) 検索ページ（優先）
    try:
        html = get_html(url_page, session)
        _save_debug("kagaku_page", html)  # デバッグ
        soup = BeautifulSoup(html, "html.parser")
        rows.extend(_parse_tables(soup, url_page))
    except Exception as e:
        logger.warning("kagaku page fetch failed: %s", e)

    # 2) 0件ならフォールバック
    if not rows:
        try:
            html = get_html(url_table, session)
            _save_debug("kagaku_table_fallback", html)
            soup = BeautifulSoup(html, "html.parser")
            rows.extend(_parse_tables(soup, url_table))
        except Exception as e:
            logger.exception("kagaku fallback fetch failed: %s", e)

    return pd.DataFrame(rows)


def _parse_kagaku_tables(tables, url_ctx):
    rows = []
    for tb in tables:
        header_txt = tb.get_text(" ", strip=True)
        # 見出しが「イベント／会期」を含むテーブルを優先
        if not any(k in header_txt for k in ["イベント", "イベント名", "イベントの名称", "会期"]):
            continue

        for tr in tb.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            # 見出し行スキップ
            if tr.find("th"):
                continue

            texts = [c.get_text(" ", strip=True) for c in cells]
            if not texts:
                continue

            title = texts[0]
            # URL は行内の最初の a[href]
            a = tr.find("a", href=True)
            link = urljoin(url_ctx, a["href"]) if a and a.get("href") else None
            if link and not link.startswith(("http://", "https://")):
                link = None

            # 会期候補（「会期」列が2列目付近にあることが多い）
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
    return rows


# ---------------- Site B: Tokyo Big Sight ----------------
def fetch_bigsight(url: str = "https://www.bigsight.jp/visitor/event/") -> pd.DataFrame:
    """
    東京ビッグサイト:
    - 検索結果は <article class="lyt-event-01"> 単位
    - タイトル: h3.hdg-01 > a
    - 詳細: dl.list-01 の dt/ddd ペア（開催期間, URL など）
    - ページャ: div.list-pager-01 p.next a[href]
    """
    session = make_session()
    rows: list[dict] = []
    seen_pages: set[str] = set()
    page_url = url

    def _to_abs(u: str, ctx: str) -> str | None:
        if not u:
            return None
        absu = urljoin(ctx, u)
        return absu if absu.startswith(("http://", "https://")) else None

    def _get_dd_by_label(dl: BeautifulSoup, label: str) -> str | None:
        # dt のテキストを見て一致した次の dd を返す
        for div in dl.find_all("div", recursive=False):
            dt_el = div.find("dt")
            dd_el = div.find("dd")
            if not dt_el or not dd_el:
                continue
            if label in dt_el.get_text(strip=True):
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

        # 1) イベントカード（記事）を列挙
        articles = soup.select("article.lyt-event-01")
        for art in articles:
            # タイトル・リンク（h3内）
            h3 = art.find("h3", class_="hdg-01")
            a_title = h3.find("a", href=True) if h3 else None
            title = a_title.get_text(" ", strip=True) if a_title else (h3.get_text(" ", strip=True) if h3 else None)
            title_link = _to_abs(a_title["href"], page_url) if a_title else None

            # 詳細 dl → dt/ddd
            dl = art.find("dl", class_="list-01")
            date_text = None
            official_url = None
            if dl:
                # 「開催期間」と「URL」を拾う
                date_text = _get_dd_by_label(dl, "開催期間")
                dd_url = None
                # 「URL」欄の dd から最初の a[href] を拾う
                for div in dl.find_all("div", recursive=False):
                    dt_el = div.find("dt")
                    dd_el = div.find("dd")
                    if not dt_el or not dd_el:
                        continue
                    if "URL" in dt_el.get_text(strip=True):
                        a = dd_el.find("a", href=True)
                        if a and a.get("href"):
                            dd_url = _to_abs(a["href"], page_url)
                            break
                official_url = dd_url or title_link

            start, end = parse_date_range(date_text or "")

            if title and (start or end):
                rows.append({
                    "source": "bigsight",
                    "title": title,
                    "start_date": start,
                    "end_date": end,
                    "venue": None,  # 必要なら「利用施設」も同様に dd から取得可
                    "url": official_url
                })

        # 2) 次ページ（ページャの「次へ」）
        next_a = soup.select_one("div.list-pager-01 p.next a[href]")
        page_url = _to_abs(next_a["href"], page_url) if next_a else None

    return pd.DataFrame(rows)


# ---------------- Site C: Makuhari Messe (print) ----------------
def fetch_makuhari(url: str = "https://www.m-messe.co.jp/event/print") -> pd.DataFrame:
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
        date_text = vals[0]
        title = vals[1] if len(vals) > 1 else None
        venue = vals[2] if len(vals) > 2 else None

        # aタグ or テキスト中URL を拾う
        a = tr.find("a", href=True)
        link = urljoin(url, a["href"]) if (a and a.get("href")) else None
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

    keep_cols = ["source", "title", "start_date", "end_date", "venue", "url"]
    for col in keep_cols:
        if col not in out.columns:
            out[col] = None
    out = out[keep_cols].copy()

    out["last_seen_at"] = datetime.now().strftime("%Y-%m-%d")

    # 重複除去キーを強化（source + title + start_date + url）
    out = out.drop_duplicates(subset=["source", "title", "start_date", "url"])
    return out


def monthly_run(output_csv: str = "events_agg.csv") -> None:
    df = collect_all()
    df.to_csv(output_csv, index=False, encoding="utf-8")
    logger.info("Saved: %s (%d rows)", output_csv, len(df))


if __name__ == "__main__":
    monthly_run()


