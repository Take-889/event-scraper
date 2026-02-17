# events_monthly.py
# -*- coding: utf-8 -*-
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
    with open(fn, "w", encoding="utf-8") as f:
        f.write(text)


# -------- 共通: 日付パース（柔軟な和暦/日本語表記対応の簡易版） --------
def parse_date_range(text):
    """
    '2026年02月18日（水）～2026年02月20日（金）' /
    '2/18 水－2/20 金' / '2/16月−2/18水' 等から (YYYY-MM-DD, YYYY-MM-DD) を返す。
    """
    if not text:
        return None, None
    t = str(text)

    # 不要文字（曜日・全角空白等）を除去
    t = re.sub(r'[（）\(\)曜月火水木金土日・\s]', '', t)

    # 区切り記号の統一（すべて '〜' に）
    # 半角/全角ハイフン、ダッシュ類、波線類を網羅
    # '−'(U+2212), '—'(U+2014), '–'(U+2013), '－'(U+FF0D), '―'(U+2015), '～'(U+FF5E)
    for ch in ['-', '−', '—', '–', '－', '―', '～']:
        t = t.replace(ch, '〜')

    parts = t.split('〜')

    def _norm(p):
        now_y = datetime.now().year
        p1 = p.replace('年', '/').replace('月', '/').replace('日', '')
        try:
            dt = dtparser.parse(p1, default=datetime(now_y, 1, 1))
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
    - 採用条件：タイトル必須 + 会期（start or end）を解釈できる行のみ
    - URLは相対→絶対へ補正し、末尾の余計な'&'を除去
    """
    import re
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (compatible; KagakuScraper/1.0; +https://github.com/your/repo)"
    }
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    # 文字コードを堅牢化（EUC-JP/Shift_JIS/UTF-8）
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
        "date":  ["会期", "開催日", "日程", "期間"],
        "venue": ["場所", "会場", "開催地", "場所/会場"],
        "url":   ["URL", "リンク", "Link"],
        # organizer は a@title="主催：..." に入ることが多い（列には無い）
    }

    def map_headers(tr):
        ths = [th.get_text(" ", strip=True) for th in tr.find_all("th")]
        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        cells = ths if ths else tds
        idx = {"title": None, "date": None, "venue": None, "url": None}
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
                # ←★ 修正ポイント：ヘッダ行（thあり）のときだけスキップ
                if header_seen:
                    continue

            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            texts = [td.get_text(" ", strip=True) for td in tds]

            # タイトル & URL（タイトル列優先、無ければ最初の a）
            title = None
            link = None
            a0 = None
            if header_idx.get("title") is not None and header_idx["title"] < len(tds):
                a0 = tds[header_idx["title"]].find("a", href=True)
                title = (a0.get_text(" ", strip=True) if a0
                         else texts[header_idx["title"]] if header_idx["title"] < len(texts) else None)
                link = a0["href"] if a0 else None
            if title is None:
                a0 = tr.find("a", href=True)
                title = a0.get_text(" ", strip=True) if a0 else (texts[0] if texts else None)
                link = a0["href"] if a0 else link

            # 会期（列インデックス or フォールバック）
            date_text = None
            if header_idx.get("date") is not None and header_idx["date"] < len(texts):
                date_text = texts[header_idx["date"]]
            else:
                for tx in texts:
                    if re.search(r"\d{1,2}/\d{1,2}", tx) or ("年" in tx and "月" in tx):
                        date_text = tx
                        break

            # 会場（列終端フォールバックあり）
            venue = None
            if header_idx.get("venue") is not None and header_idx["venue"] < len(texts):
                venue = texts[header_idx["venue"]]
            else:
                venue = texts[-1] if texts else None

            # URL 絶対化 & 末尾ノイズ除去
            if link and link.startswith("/"):
                link = urljoin(url, link)
            if link:
                link = link.rstrip("&")

            # 主催（必要なら列追加して出力に含める）
            # organizer = None
            # if a0 and a0.has_attr("title"):
            #     organizer = a0["title"].replace("主催：", "").strip()

            # 採用条件：タイトル必須 + 会期（start or end）を解釈できる
            start, end = parse_date_range(date_text) if date_text else (None, None)
            if not title or not (start or end):
                continue

            rows.append({
                "source": "kagaku",
                "title": title,
                "start_date": start,
                "end_date": end,
                "venue": venue,
                "url": link
                # "organizer": organizer,
            })

    print(f"[kagaku] parsed_rows = {len(rows)}")
    return pd.DataFrame(rows)

# -------- B) 東京ビッグサイト（bigsight.jp） --------
def fetch_bigsight(url="https://www.bigsight.jp/visitor/event/", max_pages=5):
    import re

    HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; EventBot/1.0; +https://github.com/your/repo)"}
    events = []
    seen_pages = set()
    queue = [url]
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

            info = {}
            for div in dl.select("div"):
                dt = div.find("dt")
                dd = div.find("dd")
                if not dt or not dd:
                    continue
                info[dt.get_text(strip=True)] = dd.get_text(" ", strip=True)

            # 会期（必須）
            date_text = info.get("開催期間", "")
            start, end = parse_date_range(date_text) if date_text else (None, None)
            venue = info.get("利用施設")

            # 主催サイトURL
            link = None
            url_dt = dl.find("dt", string="URL")
            if url_dt:
                dd = url_dt.find_next_sibling("dd")
                if dd:
                    a2 = dd.find("a", href=True)
                    if a2:
                        link = a2["href"]
            if not link and a_t and a_t.get("href"):
                link = a_t["href"]

            # 採用条件：タイトル + 会期（start or end）
            if title and (start or end):
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
    r = requests.get(url, timeout=30)
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
        title = vals[1] if len(vals) > 1 else None
        venue = vals[2] if len(vals) > 2 else None
        start, end = parse_date_range(dr)

        tail = " ".join(vals[3:]) if len(vals) > 3 else ""
        murl = re.search(r'(https?://[^\s]+)', tail)
        link = murl.group(1) if murl else None

        # 採用条件：タイトル + 会期（start or end）
        if title and (start or end):
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

