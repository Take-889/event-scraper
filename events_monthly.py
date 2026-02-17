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
    import requests
    from bs4 import BeautifulSoup
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    # ★デバッグ保存
    _save_debug("kagaku", r.text)

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    rows = []
    if not table:
        return pd.DataFrame(rows)

    import re
    for tr in table.find_all("tr"):
        tds = tr.find_all(['td','th'])
        if len(tds) < 3:
            continue
        texts = [td.get_text(strip=True) for td in tds]
        header_like = any(k in "".join(texts) for k in ["イベント", "会期"])
        if header_like and tr.find('th'):
            continue

        title = texts[0]
        a = tr.find("a", href=True)
        link = a["href"] if a else None

        dr = None
        for tx in texts:
            if re.search(r'\d{1,2}/\d{1,2}', tx) or ('年' in tx and '月' in tx):
                dr = tx
                break
        start, end = parse_date_range(dr or "")
        place = texts[-1] if texts else None

        if title and (start or end):
            rows.append({
                "source": "kagaku",
                "title": title,
                "start_date": start,
                "end_date": end,
                "venue": place,
                "url": link
            })
    return pd.DataFrame(rows)

# -------- B) 東京ビッグサイト（bigsight.jp） --------
from urllib.parse import urljoin

def fetch_bigsight(url="https://www.bigsight.jp/visitor/event/", max_pages=5):
    """
    東京ビッグサイト 来場者向けイベント一覧（ページネーション対応版）
    DOM構造（article.lyt-event-01 / h3.hdg-01 / dl.list-01）に準拠。
    ページャは .list-pager-01 a[href] を辿る（相対→絶対URL）。
    参照: _debug_bigsight_latest.html
    """
    import requests, re
    from bs4 import BeautifulSoup

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (compatible; EventBot/1.0; +https://github.com/your/repo)"
    }

    events = []
    seen_pages = set()
    queue = [url]

    def parse_cards(soup):
        nonlocal events
        cards = soup.select("article.lyt-event-01")
        for card in cards:
            a_t = card.select_one("h3.hdg-01 a[href]")
            title = a_t.get_text(strip=True) if a_t else None
            dl = card.select_one("div.content dl.list-01")
            if not title or not dl:
                continue

            # dt/dd を辞書化
            info = {}
            for div in dl.select("div"):
                dt = div.find("dt"); dd = div.find("dd")
                if not dt or not dd: 
                    continue
                info[dt.get_text(strip=True)] = dd.get_text(" ", strip=True)

            # 会期
            date_text = info.get("開催期間")
            start, end = parse_date_range(date_text) if date_text else (None, None)

            # 会場
            venue = info.get("利用施設")

            # 主催サイトURL（dt=URL の dd 内の a）
            link = None
            url_dt = dl.find("dt", string="URL")
            if url_dt:
                dd = url_dt.find_next_sibling("dd")
                if dd:
                    a2 = dd.find("a", href=True)
                    if a2:
                        link = a2["href"]
            # タイトルの a も外部URLであることがある
            if not link and a_t and a_t.get("href"):
                link = a_t["href"]

            if title and (start or end):
                events.append({
                    "source": "bigsight",
                    "title": title,
                    "start_date": start,
                    "end_date": end,
                    "venue": venue,
                    "url": link
                })

    pages_crawled = 0
    while queue and pages_crawled < max_pages:
        u = queue.pop(0)
        if u in seen_pages:
            continue
        seen_pages.add(u)

        r = requests.get(u, headers=HEADERS, timeout=30)
        r.raise_for_status()

        # デバッグ保存（どのページを取ったかが分かるよう連番を付与）
        pages_crawled += 1
        _save_debug(f"bigsight_p{pages_crawled}", r.text)  # [1](https://nikkeikin-my.sharepoint.com/personal/satoshi-takeda_nikkeikin_co_jp/Documents/Microsoft%20Copilot%20Chat%20%E3%83%95%E3%82%A1%E3%82%A4%E3%83%AB/_debug_bigsight_latest.html)

        soup = BeautifulSoup(r.text, "html.parser")

        # 1) カード抽出
        parse_cards(soup)  # 実DOMのカード構造に基づく抽出（article.lyt-event-01 等）[1](https://nikkeikin-my.sharepoint.com/personal/satoshi-takeda_nikkeikin_co_jp/Documents/Microsoft%20Copilot%20Chat%20%E3%83%95%E3%82%A1%E3%82%A4%E3%83%AB/_debug_bigsight_latest.html)

        # 2) ページャから次URLを収集
        for a in soup.select(".list-pager-01 a[href]"):
            next_u = urljoin(u, a["href"])
            # /visitor/event/（初期）と /visitor/event/search.php?page=N が混在するため絶対化が安全[1](https://nikkeikin-my.sharepoint.com/personal/satoshi-takeda_nikkeikin_co_jp/Documents/Microsoft%20Copilot%20Chat%20%E3%83%95%E3%82%A1%E3%82%A4%E3%83%AB/_debug_bigsight_latest.html)
            if next_u not in seen_pages:
                queue.append(next_u)

    # DataFrame で返却
    return pd.DataFrame(events)

# -------- C) 幕張メッセ（印刷用） --------
def fetch_makuhari(url="https://www.m-messe.co.jp/event/print"):
    import requests
    from bs4 import BeautifulSoup
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    # ★デバッグ保存
    _save_debug("makuhari", r.text)

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
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

        if title and (start or end):
            rows.append({
                'source': 'makuhari',
                'title': title,
                'start_date': start,
                'end_date': end,
                'venue': venue,
                'url': link
            })
    return pd.DataFrame(rows)

# -------- 統合・出力 --------
def monthly_run(output_csv="events_agg.csv"):
    # ▼ まず各サイトを取得し、件数をログ出力
    df_k = fetch_kagaku();  print("kagaku:", len(df_k))
    df_b = fetch_bigsight(); print("bigsight:", len(df_b))
    df_m = fetch_makuhari(); print("makuhari:", len(df_m))

    # ▼ 集約・重複排除・出力
    all_df = pd.concat([df_k, df_b, df_m], ignore_index=True)
    keep_cols = ["source","title","start_date","end_date","venue","url"]
    for col in keep_cols:
        if col not in all_df.columns:
            all_df[col] = None
    all_df = all_df[keep_cols].copy()
    all_df["last_seen_at"] = datetime.now().strftime("%Y-%m-%d")

    # タイトル＋開始日で重複除去
    all_df = all_df.drop_duplicates(subset=["title", "start_date"])

    # Excelで文字化けしないUTF-8 BOM付き
    all_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"Saved: {output_csv} ({len(all_df)} rows)")

if __name__ == "__main__":
    monthly_run()

if __name__ == "__main__":
    monthly_run()




