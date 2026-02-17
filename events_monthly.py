# events_monthly.py
# -*- coding: utf-8 -*-
import re
from datetime import datetime
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
    import os
    # ワークスペース直下に保存
    fn = f"_debug_{name}.html"
    with open(fn, "w", encoding="utf-8") as f:
        f.write(text)


# -------- 共通: 日付パース（柔軟な和暦/日本語表記対応の簡易版） --------
def parse_date_range(text):
    """
    '2026年02月18日（水）～2026年02月20日（金）' や
    '2/18 水－2/20 金' 等から (YYYY-MM-DD, YYYY-MM-DD) を返す。
    """
    if not text:
        return None, None
    t = str(text)
    # ノイズ除去：曜日・全角スペース・余分な全角/半角チルダ・ダッシュ
    t = re.sub(r'[（）\(\)曜月火水木金土日・\s]', '', t)
    t = t.replace('－', '〜').replace('～', '〜')
    parts = t.split('〜')

    def _norm(p):
        # '2026年02月18日' or '2/18' など
        now_y = datetime.now().year
        p1 = p.replace('年','/').replace('月','/').replace('日','')
        # 年省略に対応（年が無いとき default の年を埋める）
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
# 例: 全学協会（society_all）×全地域
def fetch_kagaku(url="https://www.kagaku.com/calendar.php?selectgenre=society_all&selectpref=all_area&submit=%B8%A1%BA%F7&eid=none"):
    import requests, re
    from bs4 import BeautifulSoup

    r = requests.get(url, timeout=30); r.raise_for_status()
    _save_debug("kagaku", r.text)

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    print(f"[kagaku] table_found = {bool(table)}")  # ★

    rows = []
    if not table:
        return pd.DataFrame(rows)

    trs = table.find_all("tr")
    print(f"[kagaku] tr_count = {len(trs)}")  # ★
    for tr in trs:
        tds = tr.find_all(['td','th'])
        if len(tds) < 3:
            continue
        texts = [td.get_text(strip=True) for td in tds]
        # ヘッダ行スキップ（必要に応じて調整）
        if tr.find('th'):
            continue

        title = texts[0]
        a = tr.find("a", href=True)
        link = a["href"] if a else None

        dr = None
        for tx in texts:
            if re.search(r'\d{1,2}/\d{1,2}', tx) or ('年' in tx and '月' in tx):
                dr = tx; break

        start, end = parse_date_range(dr or "")
        place = texts[-1] if texts else None

        # ★一時緩和
        if title:
            rows.append({
                "source": "kagaku",
                "title": title,
                "start_date": start,
                "end_date": end,
                "venue": place,
                "url": link
            })
    print(f"[kagaku] parsed_rows = {len(rows)}")  # ★
    return pd.DataFrame(rows)

# -------- B) 東京ビッグサイト（bigsight.jp） --------
# 先頭の import 群の近くに入っていなければ追加
from urllib.parse import urljoin

def fetch_bigsight(url="https://www.bigsight.jp/visitor/event/", max_pages=1):
    import requests, re
    from bs4 import BeautifulSoup

    HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; EventBot/1.0; +https://github.com/your/repo)"}
    events = []
    seen_pages = set()
    queue = [url]
    pages_crawled = 0

    def parse_cards(soup, page_idx):
        cards = soup.select("article.lyt-event-01")
        print(f"[bigsight] page{page_idx}: cards found = {len(cards)}")  # ★カード枚数ログ
        for i, card in enumerate(cards, 1):
            a_t = card.select_one("h3.hdg-01 a[href]")
            title = a_t.get_text(strip=True) if a_t else None
            dl = card.select_one("div.content dl.list-01")
            if not title or not dl:
                continue

            info = {}
            for div in dl.select("div"):
                dt = div.find("dt"); dd = div.find("dd")
                if not dt or not dd:
                    continue
                info[dt.get_text(strip=True)] = dd.get_text(" ", strip=True)

            # 会期（無くても一旦通す＝原因切り分け用）
            date_text = info.get("開催期間") or ""
            start, end = parse_date_range(date_text) if date_text else (None, None)
            venue = info.get("利用施設")

            # URL
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

            # ★一時緩和：タイトルさえ取れたら入れる（後で元に戻します）
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

        # max_pages>1にしたときに次ページも追う（今回は1ページで検証）
        if max_pages > 1:
            for a in soup.select(".list-pager-01 a[href]"):
                queue.append(urljoin(u, a["href"]))

    print(f"[bigsight] total events parsed = {len(events)}")  # ★総件数ログ
    return pd.DataFrame(events)

# -------- C) 幕張メッセ（印刷用） --------
def fetch_makuhari(url="https://www.m-messe.co.jp/event/print"):
    import requests
    from bs4 import BeautifulSoup, SoupStrainer

    r = requests.get(url, timeout=30); r.raise_for_status()
    _save_debug("makuhari", r.text)

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    print(f"[makuhari] table_found = {bool(table)}")  # ★有無ログ
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

        import re
        tail = " ".join(vals[3:]) if len(vals) > 3 else ""
        murl = re.search(r'(https?://[^\s]+)', tail)
        link = murl.group(1) if murl else None

        # ★一時緩和：タイトルさえ取れたら入れる（後で元に戻します）
        if title:
            rows.append({
                "source": "makuhari",
                "title": title,
                "start_date": start,
                "end_date": end,
                "venue": venue,
                "url": link
            })
    print(f"[makuhari] parsed_rows = {len(rows)}")  # ★件数ログ
    return pd.DataFrame(rows)

# -------- 統合・出力 --------
def monthly_run(output_csv="events_agg.csv"):
    df_k = fetch_kagaku();     print("kagaku:", len(df_k))
    df_b = fetch_bigsight(max_pages=1);  print("bigsight:", len(df_b))  # ← 呼び先を確認！
    df_m = fetch_makuhari();   print("makuhari:", len(df_m))

    all_df = pd.concat([df_k, df_b, df_m], ignore_index=True)
    keep_cols = ["source","title","start_date","end_date","venue","url"]
    for col in keep_cols:
        if col not in all_df.columns:
            all_df[col] = None
    all_df = all_df[keep_cols].copy()
    all_df["last_seen_at"] = datetime.now().strftime("%Y-%m-%d")

    # ★一時的に重複除去を弱める（start_dateがNoneで全落ちするのを防ぐ）
    all_df = all_df.drop_duplicates(subset=["title"])

    all_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"Saved: {output_csv} ({len(all_df)} rows)")

if __name__ == "__main__":
    monthly_run()

if __name__ == "__main__":
    monthly_run()





