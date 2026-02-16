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
def fetch_bigsight(url="https://www.bigsight.jp/visitor/event/"):
    import requests
    from bs4 import BeautifulSoup
    import re

    events = []

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (compatible; EventBot/1.0; +https://github.com/your/repo)"
    }

    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    _save_debug("bigsight_latest", r.text)

    soup = BeautifulSoup(r.text, "html.parser")

    # 1) 各イベントブロックを抽出（タイトル行を探す方式）
    # BigSight 現行の DOM は h3 や div でイベント名を持っている
    title_nodes = soup.find_all(["h3", "div"], string=True)

    for node in title_nodes:
        title = node.get_text(strip=True)

        # HCJ2026… や Eight EXPO… のような “タイトルっぽい文字列” のみ拾う
        if len(title) < 5:
            continue

        # 次の兄弟ノードに詳細情報が続く構造
        detail_block = node.find_next_sibling()

        if detail_block:
            text_block = detail_block.get_text(" ", strip=True)

            # 開催期間の抽出
            m = re.search(r'(\d{4}年\d{1,2}月\d{1,2}日.*?\d{1,2}月\d{1,2}日)', text_block)
            if m:
                start, end = parse_date_range(m.group(1))
            else:
                start, end = (None, None)

            # URL 抽出
            a = detail_block.find("a", href=True)
            link = a["href"] if a else None

            # 会場（利用施設）抽出
            venue = None
            m2 = re.search(r'利用施設\s*([^\s]+)', text_block)
            if m2:
                venue = m2.group(1)

            events.append({
                "source": "bigsight",
                "title": title,
                "start_date": start,
                "end_date": end,
                "venue": venue,
                "url": link
            })

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
    df_list = []
    for func in (fetch_kagaku, fetch_bigsight, fetch_makuhari):
        try:
            df = func()
            df_list.append(df)
        except Exception as e:
            print(f"[WARN] {func.__name__} failed: {e}")

    if not df_list:
        print("No data fetched.")
        return

    all_df = pd.concat(df_list, ignore_index=True)
    # 正規化：欠損の埋め / 列並び / 重複排除（title+start_date）
    keep_cols = ["source","title","start_date","end_date","venue","url"]
    for col in keep_cols:
        if col not in all_df.columns:
            all_df[col] = None
    all_df = all_df[keep_cols].copy()
    all_df["last_seen_at"] = datetime.now().strftime("%Y-%m-%d")

    # 重複（title+start_date）を基準にユニークに
    all_df = all_df.drop_duplicates(subset=["title","start_date"])

    # CSV出力
    all_df.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"Saved: {output_csv} ({len(all_df)} rows)")

if __name__ == "__main__":
    monthly_run()

if __name__ == "__main__":
    monthly_run()


