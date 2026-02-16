# events_monthly.py
# -*- coding: utf-8 -*-
import re
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

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
KAGAKU_URL = "https://www.kagaku.com/calendar.php?selectgenre=society_all&selectpref=all_area&submit=%B8%A1%BA%F7&eid=none"

def fetch_kagaku():
    resp = requests.get(KAGAKU_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    rows = []
    if not table:
        return pd.DataFrame(rows)

    for tr in table.find_all("tr"):
        # セルテキスト
        tds = tr.find_all(['td','th'])
        if len(tds) < 3:
            continue
        texts = [td.get_text(strip=True) for td in tds]
        # 見出し行などのスキップ条件（"イベントの名称"等が含まれる）
        header_like = any(k in "".join(texts) for k in ["イベント", "会期"])
        if header_like and tr.find('th'):
            continue

        # 想定： [イベント名, 年(省略可), 会期, 場所] の並びが多い
        title = texts[0]

        # 主催者サイトURL（リンク先）
        a = tr.find("a", href=True)
        link = a["href"] if a else None

        # 会期っぽい列を抽出（数字/スラッシュ/年・月・日が含まれる）
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
BIGSIGHT_URL = "https://www.bigsight.jp/visitor/event/"

def fetch_bigsight():
    events = []
    page = 1
    while True:
        u = BIGSIGHT_URL if page == 1 else f"{BIGSIGHT_URL}?page={page}"
        r = requests.get(u, timeout=30)
        if r.status_code != 200:
            break
        soup = BeautifulSoup(r.text, "html.parser")

        # カードアイテム（構造が変わることがあるため、複数候補で探す）
        cards = soup.select("div.l-event__item, li.l-event__item")
        if not cards:
            break

        for c in cards:
            # タイトル
            ttl_el = c.select_one(".l-event__ttl") or c.select_one("h3")
            title = ttl_el.get_text(strip=True) if ttl_el else None

            # まとまったテキストから会期らしき部分を拾う
            block_text = c.get_text(" ", strip=True)
            m = re.search(r'(\d{4}年?\d{1,2}月?\d{1,2}日?.*?〜?.*?\d{1,2}月?\d{1,2}日?)', block_text)
            start, end = parse_date_range(m.group(1)) if m else (None, None)

            # 主催サイトURL（カード内の外部リンク）
            link = None
            for a in c.find_all("a", href=True):
                # 公式の詳細ページ（bigsight.jp内）より、まず外部URLを優先
                if a['href'].startswith("http") and "bigsight.jp" not in a['href']:
                    link = a['href']; break
            # 見つからなければ最初のリンク
            if not link:
                a = c.find("a", href=True)
                link = a['href'] if a else None

            if title and (start or end):
                events.append({
                    "source": "bigsight",
                    "title": title,
                    "start_date": start,
                    "end_date": end,
                    "venue": None,
                    "url": link
                })
        page += 1
    return pd.DataFrame(events)

# -------- C) 幕張メッセ（印刷用） --------
MAKUHARI_PRINT_URL = "https://www.m-messe.co.jp/event/print"

def fetch_makuhari():
    r = requests.get(MAKUHARI_PRINT_URL, timeout=30)
    r.raise_for_status()
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

        # 多くの行は [会期, イベント名, 会場, 対象・入場料, 連絡先...] 構造
        dr = vals[0]
        title = vals[1] if len(vals) > 1 else None
        venue = vals[2] if len(vals) > 2 else None
        start, end = parse_date_range(dr)

        # 連絡先・URL抽出（後続セルを結合してURLを拾う）
        tail = " ".join(vals[3:]) if len(vals) > 3 else ""
        murl = re.search(r'(https?://[^\s]+)', tail)
        link = murl.group(1) if murl else None

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
