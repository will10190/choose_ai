"""
data_loader.py
台股選股資料載入模組（新版 - 五大條件）
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"

# ─────────────────────────────────────────────
# 共用 API 呼叫函式 (終極 Debug 版)
# ─────────────────────────────────────────────
def _call_api(dataset: str, stock_id: str, start_date: str, end_date: str, token: str = "") -> pd.DataFrame | None:
    params = {
        "dataset": dataset,
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
    }
    if token:
        params["token"] = token

    try:
        resp = requests.get(FINMIND_API_URL, params=params, timeout=15)
        resp.raise_for_status() # 觸發 HTTPError 如果狀態碼是 4xx 或 5xx
        body = resp.json()
        if body.get("status") == 200 and body.get("data"):
            df = pd.DataFrame(body["data"])
            df["date"] = pd.to_datetime(df["date"])
            return df.sort_values("date").reset_index(drop=True)
        return None
    except requests.exceptions.HTTPError as e:
        # 🚨 終極 Debug：印出 FinMind 伺服器真正的報錯內容！
        print(f"[{stock_id}] 被拒絕！FinMind 回傳真實原因: {e.response.text}")
        return None
    except Exception as e:
        print(f"[{stock_id}] 發生一般錯誤: {e}")
        return None

# ─────────────────────────────────────────────
# 取得股票清單 (修復小寫市場代碼)
# ─────────────────────────────────────────────
def get_all_stocks(token: str = "", market_filter: list = None) -> pd.DataFrame:
    if market_filter is None:
        market_filter = ["上市"]

    params = {"dataset": "TaiwanStockInfo"}
    if token:
        params["token"] = token

    try:
        resp = requests.get(FINMIND_API_URL, params=params, timeout=15)
        body = resp.json()
        if body.get("status") == 200 and body.get("data"):
            df = pd.DataFrame(body["data"])

            col_map = {
                "stock_id": "stock_id",
                "stock_name": "stock_name",
                "StockID": "stock_id",
                "CompanyName": "stock_name",
            }
            df.rename(columns={k: v for k, v in col_map.items() if k in df.columns}, inplace=True)
            df = df[df["stock_id"].str.match(r"^\d{4}$", na=False)]

            if "type" in df.columns:
                # 補上 twse 和 tpex 小寫
                type_map = {"上市": ["sii", "上市", "TWSE", "twse"], "上櫃": ["otc", "上櫃", "OTC", "tpex"]}
                allowed = []
                for m in market_filter:
                    allowed.extend(type_map.get(m, []))
                df = df[df["type"].isin(allowed)]

            return df[["stock_id", "stock_name"]].drop_duplicates().reset_index(drop=True)
    except Exception:
        pass

    return pd.DataFrame([
        {"stock_id": "2330", "stock_name": "台積電"},
        {"stock_id": "2317", "stock_name": "鴻海"},
    ])

# ─────────────────────────────────────────────
# 條件 1：當日突破還原週均線
# ─────────────────────────────────────────────
def check_breakout_weekly_ma(stock_id: str, start_date: str, end_date: str, token: str = "") -> dict:
    result = {"passed": False, "close": 0, "ma5_adj": 0, "breakout": False, "above_ma5": False}
    df = _call_api("TaiwanStockPrice", stock_id, start_date, end_date, token)
    if df is None or len(df) < 6: return result

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["MA5_adj"] = df["close"].rolling(5, min_periods=1).mean()
    df = df.dropna(subset=["close", "MA5_adj"])

    if len(df) < 2: return result

    today, yesterday = df.iloc[-1], df.iloc[-2]
    close_today, ma5_today = today["close"], today["MA5_adj"]
    close_yesterday, ma5_yesterday = yesterday["close"], yesterday["MA5_adj"]

    above_ma5 = close_today > ma5_today
    breakout = above_ma5 and (close_yesterday <= ma5_yesterday)

    result.update({"close": round(close_today, 2), "ma5_adj": round(ma5_today, 2), "above_ma5": above_ma5, "breakout": breakout, "passed": breakout})
    return result

# ─────────────────────────────────────────────
# 條件 2：日K 100日線糾結或黃金交叉
# ─────────────────────────────────────────────
def check_ma_tangle_or_golden_cross(stock_id: str, start_date: str, end_date: str, tangle_threshold_pct: float = 3.0, check_golden_cross: bool = True, check_tangle: bool = True, token: str = "") -> dict:
    result = {"passed": False, "ma5": 0, "ma20": 0, "ma60": 0, "ma100": 0, "is_tangle": False, "is_golden_cross": False, "tangle_spread_pct": 0}
    df = _call_api("TaiwanStockPrice", stock_id, start_date, end_date, token)
    if df is None or len(df) < 100: return result

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    for w in [5, 20, 60, 100]: df[f"MA{w}"] = df["close"].rolling(w, min_periods=w).mean()
    df = df.dropna(subset=["MA5", "MA20", "MA60", "MA100"])

    if len(df) < 2: return result

    today, yesterday = df.iloc[-1], df.iloc[-2]
    ma5, ma20, ma60, ma100 = today["MA5"], today["MA20"], today["MA60"], today["MA100"]

    ma_values = [ma5, ma20, ma60, ma100]
    ma_min = min(ma_values)
    spread_pct = (max(ma_values) - ma_min) / ma_min * 100 if ma_min > 0 else 999
    is_tangle = spread_pct < tangle_threshold_pct
    is_golden_cross = (today["MA20"] > today["MA100"]) and (yesterday["MA20"] <= yesterday["MA100"])

    result.update({"ma5": round(ma5, 2), "ma20": round(ma20, 2), "ma60": round(ma60, 2), "ma100": round(ma100, 2), "is_tangle": is_tangle, "is_golden_cross": is_golden_cross, "tangle_spread_pct": round(spread_pct, 2)})

    if check_tangle and check_golden_cross: result["passed"] = is_tangle or is_golden_cross
    elif check_tangle: result["passed"] = is_tangle
    elif check_golden_cross: result["passed"] = is_golden_cross

    return result

# ─────────────────────────────────────────────
# 條件 3：當日股價下跌，主力（三大法人）持續買入
# ─────────────────────────────────────────────
def check_price_down_inst_buy(stock_id: str, start_date: str, end_date: str, consecutive_buy_days: int = 3, token: str = "") -> dict:
    result = {"passed": False, "price_down": False, "inst_net_today": 0, "inst_consecutive_days": 0, "change_pct": 0}
    
    price_df = _call_api("TaiwanStockPrice", stock_id, start_date, end_date, token)
    if price_df is None or len(price_df) < 2: return result
    price_df["close"] = pd.to_numeric(price_df["close"], errors="coerce")

    today_close, yesterday_close = price_df["close"].iloc[-1], price_df["close"].iloc[-2]
    price_down = today_close < yesterday_close
    change_pct = (today_close - yesterday_close) / yesterday_close * 100 if yesterday_close > 0 else 0

    inst_df = _call_api("TaiwanStockInstitutionalInvestorsBuySell", stock_id, start_date, end_date, token)
    inst_consecutive, inst_net_today = 0, 0

    if inst_df is not None and len(inst_df) > 0:
        inst_df["buy"] = pd.to_numeric(inst_df["buy"], errors="coerce").fillna(0)
        inst_df["sell"] = pd.to_numeric(inst_df["sell"], errors="coerce").fillna(0)
        inst_df["net"] = inst_df["buy"] - inst_df["sell"]
        daily_net = inst_df.groupby("date")["net"].sum().reset_index().sort_values("date")

        if len(daily_net) > 0:
            inst_net_today = daily_net["net"].iloc[-1]
            inst_consecutive = _count_consecutive_positive(daily_net["net"].values)

    result.update({"price_down": price_down, "inst_net_today": int(inst_net_today), "inst_consecutive_days": inst_consecutive, "change_pct": round(change_pct, 2), "passed": price_down and (inst_consecutive >= consecutive_buy_days)})
    return result

# ─────────────────────────────────────────────
# 條件 4 & 5：股權分散表
# ─────────────────────────────────────────────
def check_shareholding_distribution(stock_id: str, token: str = "", whale_increase_weeks: int = 2, shareholder_decrease_weeks: int = 2) -> dict:
    result = {"whale_passed": False, "shareholder_passed": False, "latest_whale_people": 0, "latest_total_people": 0, "whale_trend": 0, "total_trend": 0, "weeks_available": 0}
    end_date = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(weeks=12)).strftime("%Y-%m-%d")

    df = _call_api("TaiwanStockHoldingSharesPer", stock_id, start_date, end_date, token)
    if df is None or len(df) == 0: return result

    try:
        df["people"] = pd.to_numeric(df["people"], errors="coerce").fillna(0)
        weekly_dates = sorted(df["date"].unique())
        result["weeks_available"] = len(weekly_dates)

        if len(weekly_dates) < 2: return result

        weekly_stats = []
        for d in weekly_dates:
            week_df = df[df["date"] == d]
            whale_mask = week_df["HoldingSharesLevel"].str.contains(r"40萬|100萬|200萬|400萬|600萬|800萬|1000萬|超過", na=False)
            weekly_stats.append({"date": d, "whale_people": week_df[whale_mask]["people"].sum(), "total_people": week_df["people"].sum()})

        stats_df = pd.DataFrame(weekly_stats).sort_values("date")
        if len(stats_df) < 2: return result

        latest = stats_df.iloc[-1]
        result.update({"latest_whale_people": int(latest["whale_people"]), "latest_total_people": int(latest["total_people"])})

        n = min(whale_increase_weeks, len(stats_df) - 1)
        whale_values, total_values = stats_df["whale_people"].values, stats_df["total_people"].values

        whale_increasing = all(whale_values[-(i)] > whale_values[-(i+1)] for i in range(1, n + 1) if len(whale_values) > i)
        total_decreasing = all(total_values[-(i)] < total_values[-(i+1)] for i in range(1, n + 1) if len(total_values) > i)

        result.update({"whale_trend": int(latest["whale_people"] - stats_df.iloc[-2]["whale_people"]), "total_trend": int(latest["total_people"] - stats_df.iloc[-2]["total_people"]), "whale_passed": whale_increasing, "shareholder_passed": total_decreasing})
    except Exception:
        pass
    return result

# ─────────────────────────────────────────────
# 輔助函式
# ─────────────────────────────────────────────
def _count_consecutive_positive(values: np.ndarray) -> int:
    count = 0
    for v in reversed(values):
        if pd.notna(v) and v > 0: count += 1
        else: break
    return count
