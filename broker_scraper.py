#!/usr/bin/env python3
"""
broker_scraper.py
券商分點爬蟲模組 — 不需要 FinMind Token，直接爬富邦 DJ 網站
[Fix] 修正分點名稱疊字問題
[Update] 修復多日查詢邏輯：改用單日精準回推，避免 d 參數累計加總導致的數據偏移
"""

import re
import requests
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
import urllib3
from datetime import datetime, timedelta

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BROKER_JS_URL  = "https://fubon-ebrokerdj.fbs.com.tw/z/js/zbrokerjs.djjs"
BRANCH_DATA_URL = "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm"
ENTRY_URL = "https://fubon-ebrokerdj.fbs.com.tw/Z/ZG/ZGB/ZGB.djhtm"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://fubon-ebrokerdj.fbs.com.tw/",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Connection": "keep-alive"
}

_SESSION = requests.Session()
_SESSION.headers.update(_HEADERS)

def _ensure_session_cookie(force=False):
    if force or not _SESSION.cookies:
        try:
            _SESSION.get(ENTRY_URL, verify=False, timeout=10)
        except Exception:
            pass

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_broker_list() -> dict:
    _ensure_session_cookie(force=True)
    try:
        resp = _SESSION.get(BROKER_JS_URL, verify=False, timeout=15)
        resp.raise_for_status()
        resp.encoding = "big5"
        js_text = resp.text
    except Exception as e:
        raise RuntimeError(f"取得券商清單失敗：{e}")

    match = re.search(r"g_BrokerList\s*=\s*'(.+?)'", js_text, re.DOTALL)
    if not match: raise ValueError("無法解析券商清單 JS")

    raw = match.group(1)
    broker_map = {}

    for group in raw.split(";"):
        group = group.strip()
        if not group: continue
        entries = group.split("!")
        if not entries: continue

        first_parts = entries[0].split(",", 1)
        if len(first_parts) < 2: continue
        main_code, main_name = first_parts[0].strip(), first_parts[1].strip()

        if len(entries) == 1:
            broker_map[main_code] = {
                "name": main_name, 
                "a": main_code, "b": main_code, "main": main_name, "branch": main_name
            }
            continue

        for entry in entries[1:]:
            parts = entry.split(",", 1)
            if len(parts) < 2: continue
            branch_code, branch_name = parts[0].strip(), parts[1].strip()
            
            if branch_name.startswith(main_name):
                display_name = branch_name
            else:
                display_name = f"{main_name}-{branch_name}"

            broker_map[branch_code] = {
                "name": display_name,
                "a": main_code, 
                "b": branch_code, 
                "main": main_name,
                "branch": branch_name
            }

    return broker_map

def lookup_branch_code(branch_input: str, broker_map: dict) -> list:
    keyword = branch_input.strip().replace("-", "").replace(" ", "")
    candidates = []
    
    for b_code, info in broker_map.items():
        branch_name = info['branch'].replace("-", "").replace(" ", "")
        main_name = info['main'].replace("-", "").replace(" ", "")
        full_name = main_name + branch_name
        
        if keyword in branch_name or keyword in full_name or keyword == b_code:
            candidates.append(info)
            
    return candidates

def _parse_int(x: str):
    x = x.replace(",", "").strip()
    if not x or x == "-": return 0
    try: return int(x)
    except ValueError: return None

def fetch_branch_buy_list(branch_name: str, a_code: str, b_code: str) -> pd.DataFrame:
    EMPTY_COLS = ["股票代號", "股票名稱", "買張", "賣張", "買賣超", "資料日期"]
    _ensure_session_cookie(force=True)

    params = {"a": a_code, "b": b_code, "c": "E", "d": "1"}

    try:
        resp = _SESSION.get(BRANCH_DATA_URL, params=params, verify=False, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"連線失敗：{e}")

    resp.encoding = "big5"
    html = resp.text

    date_match = re.search(r"資料日期[：:]\s*(\d{8})", html)
    data_date = f"{date_match.group(1)[:4]}/{date_match.group(1)[4:6]}/{date_match.group(1)[6:]}" if date_match else "N/A"

    soup = BeautifulSoup(html, "html.parser")
    records = []

    for tr in soup.find_all("tr"):
        tds = tr.find_all("td", recursive=False)
        if len(tds) == 4:
            td_html = str(tds[0])
            stock_id, stock_name = None, None

            m_script = re.search(r"GenLink2stk\s*\(\s*['\"](?:AS)?([A-Za-z0-9]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", td_html)
            if m_script:
                stock_id = m_script.group(1).strip()
                stock_name = m_script.group(2).strip()
            else:
                text = tds[0].get_text(strip=True).replace('\u200b', '')
                m_text = re.match(r"^([A-Za-z0-9]{4,6})\s*(.+)$", text)
                if not m_text: 
                    m_text = re.match(r"^([A-Za-z0-9]{4,6})(.+)$", text)
                if m_text:
                    stock_id = m_text.group(1).strip()
                    stock_name = m_text.group(2).strip()

            if stock_id and any(c.isdigit() for c in stock_id):
                b = _parse_int(tds[1].get_text(strip=True))
                s = _parse_int(tds[2].get_text(strip=True))
                n = _parse_int(tds[3].get_text(strip=True))

                if b is not None and s is not None and n is not None:
                    records.append({
                        "股票代號": stock_id,
                        "股票名稱": stock_name,
                        "買張": b,
                        "賣張": s,
                        "買賣超": n,
                        "資料日期": data_date
                    })

    if not records:
        return pd.DataFrame(columns=EMPTY_COLS)

    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["股票代號"], keep="first")
    return df

@st.cache_data(ttl=1800, show_spinner=False)
def get_branch_data_cached(branch_name: str, a_code: str, b_code: str) -> pd.DataFrame:
    return fetch_branch_buy_list(branch_name, a_code, b_code)

def fetch_branch_multi_day(a_code: str, b_code: str, days: int = 5) -> pd.DataFrame:
    """
    精準回推法：先撈最新一天，再逐日往前推算，避開假日，直到收滿 5 個交易日為止。
    這能確保每一天都是獨立的買賣超資料，不會被富邦 DJ 的累積加總 (d=2,3...) 影響。
    """
    _ensure_session_cookie(force=True)
    all_records = []
    collected_dates = []

    # 1. 取得最新的一個交易日
    try:
        params = {"a": a_code, "b": b_code, "c": "E", "d": "1"}
        resp = _SESSION.get(BRANCH_DATA_URL, params=params, verify=False, timeout=15)
        resp.encoding = "big5"
        date_match = re.search(r"資料日期[：:]\s*(\d{8})", resp.text)
        
        if not date_match:
            return pd.DataFrame(columns=["股票代號", "股票名稱", "資料日期", "買賣超"])
            
        latest_date_str = date_match.group(1)
        current_check_date = datetime.strptime(latest_date_str, "%Y%m%d")
    except Exception:
        return pd.DataFrame(columns=["股票代號", "股票名稱", "資料日期", "買賣超"])

    # 2. 從最新日開始逐日往前回推
    failsafe = 0
    while len(collected_dates) < days and failsafe < 20:
        failsafe += 1
        target_str = f"{current_check_date.year}-{current_check_date.month}-{current_check_date.day}"
        
        try:
            params = {"a": a_code, "b": b_code, "c": "E", "e": target_str, "f": target_str}
            resp = _SESSION.get(BRANCH_DATA_URL, params=params, verify=False, timeout=15)
            resp.encoding = "big5"
            html = resp.text
            
            match = re.search(r"資料日期[：:]\s*(\d{8})", html)
            if match:
                ret_date_str = match.group(1)
                ret_date = datetime.strptime(ret_date_str, "%Y%m%d")
                
                # 只有回傳日期與請求日期一致，才代表是有效交易日
                if ret_date == current_check_date:
                    fmt_date = ret_date.strftime("%Y/%m/%d")
                    if fmt_date not in collected_dates:
                        collected_dates.append(fmt_date)
                        
                        soup = BeautifulSoup(html, "html.parser")
                        for tr in soup.find_all("tr"):
                            tds = tr.find_all("td", recursive=False)
                            if len(tds) != 4: continue

                            td_html = str(tds[0])
                            stock_id, stock_name = None, None
                            m_s = re.search(r"GenLink2stk\s*\(\s*['\"](?:AS)?([A-Za-z0-9]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", td_html)
                            if m_s:
                                stock_id, stock_name = m_s.group(1).strip(), m_s.group(2).strip()
                            else:
                                text = tds[0].get_text(strip=True).replace("\u200b", "")
                                m_t = re.match(r"^([A-Za-z0-9]{4,6})\s*(.+)$", text)
                                if m_t: stock_id, stock_name = m_t.group(1).strip(), m_t.group(2).strip()

                            if stock_id and any(c.isdigit() for c in stock_id):
                                net = _parse_int(tds[3].get_text(strip=True))
                                if net is not None:
                                    all_records.append({
                                        "股票代號": stock_id,
                                        "股票名稱": stock_name or "",
                                        "資料日期": fmt_date,
                                        "買賣超": net,
                                    })
        except Exception:
            pass
        
        current_check_date -= timedelta(days=1)

    if not all_records:
        return pd.DataFrame(columns=["股票代號", "股票名稱", "資料日期", "買賣超"])
        
    df = pd.DataFrame(all_records)
    df = df.drop_duplicates(subset=["股票代號", "資料日期"], keep="first")
    return df

@st.cache_data(ttl=1800, show_spinner=False)
def get_branch_multi_day_cached(branch_name: str, a_code: str, b_code: str, days: int = 5) -> pd.DataFrame:
    return fetch_branch_multi_day(a_code, b_code, days)
