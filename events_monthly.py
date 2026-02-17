# -------- 共通: 日付パース --------
def parse_date_range(text):
    if not text:
        return None, None
    t = str(text)
    t = re.sub(r'[（(].*?[）)]', '', t)           # 括弧内削除
    t = re.sub(r'\s+', '', t)                    # 空白削除
    for ch in ['〜', '～', '-', '−', '—', '–', '－', '―']:
        t = t.replace(ch, '〜')                  # 区切り統一
    t = t.replace('年', '/').replace('月', '/').replace('日', '')  # 年/月/日 → スラッシュ
    t = re.sub(r'[月火水木金土日曜]', '', t)     # 曜日文字を最後に除去

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
    import re
    HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; KagakuScraper/1.0; +https://github.com/your/repo)"}
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

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

    candidates = []
    for tb in tables:
        header_txt = tb.get_text(" ", strip=True)
        if any(k in header_txt for k in ["イベント", "イベント名", "イベントの名称", "会期"]):
            candidates.append(tb)
    print(f"[kagaku] candidate tables = {len(candidates)}")

    synonyms = {
        "title": ["イベント名", "イベント", "イベントの名称", "名称", "題目"],
        "year":  ["年"],
        "date":  ["会期", "開催日", "日程", "期間"],
        "venue": ["場所", "会場", "開催地", "場所/会場"],
        "url":   ["URL", "リンク", "Link"],
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
                    continue

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
                continue

            title = a_any.get_text(" ", strip=True)
            link  = a_any.get("href")

            # URL 絶対化（相対/ルート相対どちらでも）
            if link:
                link = urljoin(url, link)
            # http(s) 必須 & 末尾ノイズ除去
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
