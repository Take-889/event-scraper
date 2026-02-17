# events_monthly.py
# -*- coding: utf-8 -*-
"""
月1回の自動収集用スクリプト
- 科学カレンダー（kagaku.com）
- 東京ビッグサイト（bigsight.jp）
- 幕張メッセ（印刷用）
出力: events_agg.csv（UTF-8 with BOM：Excelで文字化けしない）
"""
import re
from datetime import datetime
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser as dtparser


# --- デバッグ: 取得したHTMLを保存（Artifactsで参照するため） ---
def _save_debug(name: str, text: str):
    """
    取得したHTMLを _debug_*.html として保存します。
    GitHub Actions 側で upload-artifact してダウンロード・目視確認できます。
    """
    fn = f"_debug_{name}.html"
    try:
        with open(fn, "w", encoding="utf-8") as f:
            f.write(text)
    except Exception:
        # ワークスペースに書けない環境でも落ちないようにする
        pass


# -------- 共通: 日付パース（柔軟な和暦/日本語表記対応） --------
def parse_date_range(text):
    """
    '2026年02月18日（水）～2026年02月20日（金）' /
    '2/18 水－2/20 金' / '2/16月−2/18水' 等から (YYYY-MM-DD, YYYY-MM-DD) を返す。
    ・括弧内（曜日など）を削除
    ・年/月/日 をスラッシュに正規化
    ・各種ダッシュ/波線を統一
    """
    if not text:
        return None, None
    t = str(text)

    # 1) 括弧（全角/半角）内を除去（曜日・注記などまとめて削除）
    t = re.sub(r'[（(].*?[）)]', '', t)
    # 2) 空白類の削除
    t = re.sub(r'\s+', '', t)

    # 3) 区切り記号の統一（すべて '〜' に）
    for ch in ['〜', '～', '-', '−', '—', '–', '－', '―']:
        t = t.replace(ch, '〜')

    # 4) 年月日 → スラッシュに正規化（年→/、月→/、日→削除）
    t = t.replace('年', '/').replace('月', '/').replace('日', '')

    parts = t.split('〜')

    def _norm(p):
        now_y = datetime.now().year
        try:
            dt = dtparser.parse(p, default=datetime(now_y, 1, 1))
            return dt.strftime('%Y-%m-%d')
        except Exception:
            return None

    if len(parts) == 2:
        return _norm(parts[0]), _norm(parts[1])
    else:
        d = _norm(t)
        return d, d


# -------- A) 科学カレンダー（kagaku.com） --------
def fetch_kagaku(
    url="https://www.kagaku.com/calendar.php?selectgenre=society_all&selectpref=all_area&submit=%B8%A1%BA%F7&eid=none"
):
    """
    科学カレンダーのイベント一覧（複数テーブル）を抽出。
    - 文字コードを自動判別（EUC-JP 等に対応）
    - 見出し同義語マッピングで列インデックスを推定
    - 採用条件：
        * タイトル（a[href]）必須
        * 会期（start or end）を解釈できる
        * URLはhttp(s)必須（相対→絶対に補完）
    """
    HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; KagakuScraper/1.0; +https://github.com/your/repo)"}
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    # 文字コードを堅牢化
    enc = r.apparent_encoding or r.encoding or "EUC-JP"
    try:
        r.encoding = enc
        html = r.text
    except Exception:
        r.encoding = "utf-8"
        html = r.text

    _save_debug("kagaku", html)
    soup = BeautifulSoup(html, "html.parser")

    tables = soup.find_all("table")
    print(f"[kagaku] tables found = {len(tables)}")

    # “イベント一覧”らしいテーブル（見出し語で候補化）
    candidates = []
    for tb in tables:
        header_txt = tb.get_text(" ", strip=True)
        if any(k in header_txt for k in ["イベント", "イベント名", "イベントの名称", "会期"]):
            candidates.append(tb)
    print(f"[kagaku] candidate tables = {len(candidates)}")

    # 見出し同義語
    synonyms = {
        "title": ["イベント名", "イベント", "イベントの名称", "名称", "題目"],
        "year":  ["年"],
        "date":  ["会期", "開催日", "日程", "期間"],
        "venue": ["場所", "会場", "開催地", "場所/会場"],
        "url":   ["URL", "リンク", "Link"],
        # organizer は a@title="主催：..." に入ることが多い（列には無い）
    }

    def map_headers(tr):
        ths = [th.get_text(" ", strip=True) for th in tr.find_all("th")]
        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        cells = ths if ths else tds
        idx = {"title": None, "year": None, "date": None, "venue": None, "url": None}
        for i, c in enumerate(cells):
            for key, words in synonyms.items():
                if any(w in c for w in words) and idx[key] is None:
                    idx[key] = i
        return idx, bool(ths)

    rows = []
    for ti, tb in enumerate(candidates, 1):
        trs = tb.find_all("tr")
        print(f"[kagaku] t{ti}: tr_count = {len(trs)}")
        header_idx = None
        header_seen = False

        for ri, tr in enumerate(trs, 1):
            # 見出し推定（th 行 or 最上段）
            if header_idx is None:
                header_idx, header_seen = map_headers(tr)
                if header_seen:
                    continue  # ヘッダ行を読み飛ばす

            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            texts = [td.get_text(" ", strip=True) for td in tds]

            # ---- タイトル & URL（a[href] を必須に）----
            a_title = None
            if header_idx.get("title") is not None and header_idx["title"] < len(tds):
                a_title = tds[header_idx["title"]].find("a", href=True)
            a_any = a_title or tr.find("a", href=True)
            if not a_any:
                continue  # a が無い行はイベントとみなさない

            title = a_any.get_text(" ", strip=True)
            link = a_any.get("href")

            # URL 絶対化 & http(s) 必須 & 末尾ノイズ除去
            if link and link.startswith("/"):
                link = urljoin(url, link)
            if not (link and link.startswith("http")):
                continue
            link = link.rstrip("&")

            # ---- 年（任意：補完に使用）----
            year_val = None
            if header_idx.get("year") is not None and header_idx["year"] < len(texts):
                m = re.search(r"\b(20\d{2}|19\d{2})\b", texts[header_idx["year"]])
                if m:
                    year_val = m.group(1)

            # ---- 会期（列インデックス or 厳しめフォールバック）----
            date_text = None
            if header_idx.get("date") is not None and header_idx["date"] < len(texts):
                date_text = texts[header_idx["date"]]
            else:
                for tx in texts:
                    if re.search(r"\d{1,2}/\d{1,2}", tx) or (("年" in tx) and ("月" in tx) and ("日" in tx)):
                        date_text = tx
                        break
            if not date_text:
                continue

            # 年が分かっていて date_text に年が無ければ補完
            date_for_parse = date_text
            if year_val and ("年" not in date_text):
                date_for_parse = f"{year_val}年{date_text}"

            start, end = parse_date_range(date_for_parse)
            if not (start or end):
                continue  # 採用条件：会期が解釈できる

            # ---- 会場 ----
            venue = None
            if header_idx.get("venue") is not None and header_idx["venue"] < len(texts):
                venue = texts[header_idx["venue"]]
            else:
                venue = texts[-1] if texts else None

            # タイトル最小長で軽くノイズ除外（任意）
            if not title or len(title) < 5:
                continue

            rows.append({
                "source": "kagaku",
                "title": title,
                "start_date": start,
                "end_date": end,
                "venue": venue,
                "url": link
            })

    print(f"[kagaku] parsed_rows = {len(rows)}")
    return pd.DataFrame(rows)


# -------- B) 東京ビッグサイト（bigsight.jp） --------
def fetch_bigsight(url="https://www.bigsight.jp/visitor/event/", max_pages=5):
    """
    来場者向けイベント一覧のカードを抽出。
    - カード: <article class="lyt-event-01">
    - タイトル: h3.hdg-01 > a
    - 詳細: div.content dl.list-01 内の dt/dd
    - 採用条件：タイトル + 会期（start or end）
    """
    HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; EventBot/1.0; +https://github.com/your/repo)"}
    events, seen_pages, queue = [], set(), [url]
    pages_crawled = 0

    def parse_cards(soup, page_idx):
        cards = soup.select("article.lyt-event-01")
        print(f"[bigsight] page{page_idx}: cards found = {len(cards)}")
        for card in cards:
            a_t = card.select_one("h3.hdg-01 a[href]")
            title = a_t.get_text(strip=True) if a_t else None
            dl = card.select_one("div.content dl.list-01")
            if not title or not dl:
                continue

            date_text, venue, link = None, None, None

            # 1) 通常パス：dt=開催期間 / dt=利用施設 / dt=URL
            for div in dl.select("div"):
                dt_ = div.find("dt")
                dd_ = div.find("dd")
                if not dt_ or not dd_:
                    continue
                key = dt_.get_text(strip=True)
                val = dd_.get_text(" ", strip=True)
                if ("開催" in key) and ("期間" in key):
                    date_text = val
                elif ("利用" in key) and ("施設" in key):
                    venue = val
                elif key and key.strip().upper() == "URL":
                    a2 = dd_.find("a", href=True)
                    if a2:
                        link = a2["href"]

            # 2) フォールバック：dl内のddから日付パターンで拾う
            if not date_text:
                for dd_ in dl.find_all("dd"):
                    txt = dd_.get_text(" ", strip=True)
                    if re.search(r"\d{4}年?\d{1,2}月?\d{1,2}日?", txt) or re.search(r"\d{1,2}/\d{1,2}", txt):
                        date_text = txt
                        break

            start, end = parse_date_range(date_text) if date_text else (None, None)
            if not (start or end):
                continue

            if not link and a_t and a_t.get("href"):
                link = a_t["href"]

            events.append({
                "source": "bigsight",
                "title": title,
                "start_date": start,
                "end_date": end,
                "venue": venue,
                "url": link
            })

    while queue and pages_crawled < max_pages:
        u = queue.pop(0)
        if u in seen_pages:
            continue
        seen_pages.add(u)

        r = requests.get(u, headers=HEADERS, timeout=30)
        r.raise_for_status()
        pages_crawled += 1
        _save_debug(f"bigsight_p{pages_crawled}", r.text)

        soup = BeautifulSoup(r.text, "html.parser")
        parse_cards(soup, pages_crawled)

        if max_pages > 1:
            for a in soup.select(".list-pager-01 a[href]"):
                queue.append(urljoin(u, a["href"]))

    print(f"[bigsight] total events parsed = {len(events)}")
    return pd.DataFrame(events)


# -------- C) 幕張メッセ（印刷用） --------
def fetch_makuhari(url="https://www.m-messe.co.jp/event/print"):
    """
    印刷用ページの表から抽出。
    - 1列目: 会期
    - 2列目: イベント名
    - 3列目: 会場
    - 後続セル: 連絡先やURL（正規表現でURLを抽出）
    - 採用条件：タイトル + 会期（start or end）
    """
    HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MesseBot/1.0; +https://github.com/your/repo)"}
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    _save_debug("makuhari", r.text)

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    print(f"[makuhari] table_found = {bool(table)}")
    rows = []
    if not table:
        return pd.DataFrame(rows)

    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        vals = [td.get_text(" ", strip=True) for td in tds]
        dr = vals[0]
        if "会期" in dr:   # ヘッダ様行は除外
            continue

        title = vals[1] if len(vals) > 1 else None
        venue = vals[2] if len(vals) > 2 else None
        start, end = parse_date_range(dr) if dr else (None, None)
        if not (title and (start or end)):
            continue

        tail = " ".join(vals[3:]) if len(vals) > 3 else ""
        murl = re.search(r'(https?://[^\s]+)', tail)
        link = murl.group(1) if murl else None

        rows.append({
            "source": "makuhari",
            "title": title,
            "start_date": start,
            "end_date": end,
            "venue": venue,
            "url": link
        })
    print(f"[makuhari] parsed_rows = {len(rows)}")
    return pd.DataFrame(rows)


# -------- 統合・出力 --------
def monthly_run(output_csv="events_agg.csv"):
    df_k = fetch_kagaku();                  print("kagaku:", len(df_k))
    df_b = fetch_bigsight(max_pages=5);     print("bigsight:", len(df_b))
    df_m = fetch_makuhari();                print("makuhari:", len(df_m))

    all_df = pd.concat([df_k, df_b, df_m], ignore_index=True)

    # 文字列の軽い正規化（空白の連続を1つに）
    def _norm(s):
        return re.sub(r'\s+', ' ', s).strip() if isinstance(s, str) else s
    for c in ["title", "venue", "url"]:
        if c in all_df.columns:
            all_df[c] = all_df[c].map(_norm)

    keep_cols = ["source", "title", "start_date", "end_date", "venue", "url"]
    for col in keep_cols:
        if col not in all_df.columns:
            all_df[col] = None
    all_df = all_df[keep_cols].copy()
    all_df["last_seen_at"] = datetime.now().strftime("%Y-%m-%d")

    # 重複除去：タイトル + 開始日 + 会場
    all_df = all_df.drop_duplicates(subset=["title", "start_date", "venue"])

    # 並び（開始日の昇順→タイトル）
    all_df = all_df.sort_values(by=["start_date", "title"], kind="stable")

    # Excelで文字化けしないUTF-8（BOM付き）
    all_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"Saved: {output_csv} ({len(all_df)} rows)")


if __name__ == "__main__":
    monthly_run()
