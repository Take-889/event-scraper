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
    科学カレンダー：フォーム画面 calendar.php ではなく、表データの calendartable.php を優先して取得。
    """
    session = make_session()

    # 1) 直接テーブルを返すエンドポイント（推奨）
    base = "https://www.kagaku.com/calendartable.php"
    params = "?selectgenre=society_all&selectpref=all_area&eid=none"
    url_table = base + params

    # 2) 互換: 従来の calendar.php もフォールバックで試す
    url_page = "https://www.kagaku.com/calendar.php?selectgenre=society_all&selectpref=all_area&submit=%B8%A1%BA%F7&eid=none"

    rows = []

    # --- Try calendartable.php ---
    try:
        html = get_html(url_table, session)
        _save_debug("kagaku_table", html)
        soup = BeautifulSoup(html, "html.parser")
        candidates = soup.find_all("table")
        if not candidates:
            logger.warning("kagaku: no table on calendartable.php, fallback to calendar.php")
            raise ValueError("no_table")

        rows.extend(_parse_kagaku_tables(candidates, url_table))
    except Exception as e:
        logger.warning("kagaku table fetch failed (%s). fallback page parse.", e)
        try:
            html = get_html(url_page, session)
            _save_debug("kagaku_page", html)
            soup = BeautifulSoup(html, "html.parser")
            candidates = soup.find_all("table")
            if candidates:
                rows.extend(_parse_kagaku_tables(candidates, url_page))
        except Exception as e2:
            logger.exception("kagaku page fetch failed: %s", e2)

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
    東京ビッグサイトのイベント一覧。
    1) 既知のカード/リストの CSS セレクタで抽出
    2) 0件なら「開催期間」「URL」ラベルに基づくフォールバック抽出
    3) それでも 0件なら英語ページでも試行
    """
    session = make_session()
    rows = []

    def _extract_from_html(html: str, ctx_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        result = []

        # --- 既知セレクタ（カード/リスト）。構造変更があり得るため複数候補を束ねる ---
        card_selectors = [
            "div.l-event__item", "li.l-event__item",    # 旧想定
            "li.c-cardList__item", "div.c-cardList__item",
            "li.p-event-list__item", "article.c-eventCard",
        ]
        cards = []
        for sel in card_selectors:
            found = soup.select(sel)
            if found:
                cards.extend(found)

        # 重複除去
        cards = list(dict.fromkeys(cards))

        for c in cards:
            # タイトル候補
            ttl = (c.select_one(".l-event__ttl, h3, .title, .event-title, .c-eventCard__title") or
                   c.find(["h2", "h3", "h4"]))
            title = ttl.get_text(" ", strip=True) if ttl else None

            # 日付候補
            date_el = c.select_one(".l-event__date, .date, .event-date, .c-eventCard__date")
            date_text = date_el.get_text(" ", strip=True) if date_el else c.get_text(" ", strip=True)

            # 抜き出し
            m = re.search(r'(\d{4}年?\d{1,2}月?\d{1,2}日?.*?〜?.*?\d{1,2}月?\d{1,2}日?)', date_text)
            if not m:
                m = re.search(r'(\d{1,2}/\d{1,2}.*?〜.*?\d{1,2}/\d{1,2})', date_text)
            start, end = parse_date_range(m.group(1)) if m else (None, None)

            # URL：カード内最初のリンクを採用
            a = c.find("a", href=True)
            link = urljoin(ctx_url, a["href"]) if a else None

            if title and (start or end):
                result.append({
                    "source": "bigsight",
                    "title": title,
                    "start_date": start,
                    "end_date": end,
                    "venue": None,
                    "url": link
                })

        # --- フォールバック：ラベルに基づく抽出（構造破壊時） ---
        if not result:
            # main 以下のブロックを幅広く対象にする
            blocks = soup.select("main article, main li, main div, main section")
            for b in blocks:
                txt = b.get_text(" ", strip=True)
                if ("開催期間" not in txt) or (("http://" not in txt) and ("https://" not in txt)):
                    continue

                # タイトル：見出し or 最初のリンクテキスト
                ttl = b.find(["h2", "h3", "h4"]) or b.find("a", href=True)
                title = ttl.get_text(" ", strip=True) if ttl else None

                # 日付
                dm = re.search(r'開催期間\s*([0-9０-９]{4}年?.*?[日|曜])', txt)
                if not dm:
                    dm = re.search(r'(\d{4}年?\d{1,2}月?\d{1,2}日?.*?〜?.*?\d{1,2}月?\d{1,2}日?)', txt)
                if not dm:
                    dm = re.search(r'(\d{1,2}/\d{1,2}.*?〜.*?\d{1,2}/\d{1,2})', txt)
                start, end = parse_date_range(dm.group(1)) if dm else (None, None)

                # URL（最初の絶対 URL）
                link = None
                for a in b.find_all("a", href=True):
                    u = urljoin(url, a["href"])
                    if u.startswith(("http://", "https://")):
                        link = u
                        break

                if title and (start or end):
                    result.append({
                        "source": "bigsight",
                        "title": title,
                        "start_date": start,
                        "end_date": end,
                        "venue": None,
                        "url": link
                    })

        return result

    # --- 日本語ページ ---
    try:
        html = get_html(url, session)
        _save_debug("bigsight_ja", html)
        rows.extend(_extract_from_html(html, url))
    except Exception as e:
        logger.exception("bigsight ja fetch failed: %s", e)

    # --- 英語ページ（日本語が 0 件の時のみ試す） ---
    if not rows:
        en = "https://www.bigsight.jp/english/visitor/event/"
        try:
            html = get_html(en, session)
            _save_debug("bigsight_en", html)
            rows.extend(_extract_from_html(html, en))
        except Exception as e:
            logger.exception("bigsight en fetch failed: %s", e)

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

