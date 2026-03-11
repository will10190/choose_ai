"""
data_loader.py
台股選股資料載入模組

外資抓取策略：
- 大補帖階段：不抓外資（省 API）
- 篩選結束後，只針對贏家補充外資：
  ・上市贏家 → 按日期 bulk 下載（一次拿全市場，快取）
  ・上櫃贏家 → 按 stock_id 逐一查詢（上櫃 bulk 不支援，但贏家數量少）
- 條件⑤（外資連買 N 天）在 app.py 組裝結果時做最後過濾
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import streamlit as st
import time

FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"


# ─────────────────────────────────────────────
# 1. 取得全市場股票名單（含 type 欄位）
# ─────────────────────────────────────────────
@st.cache_data(ttl=86400)
def get_all_stocks(token: str = "") -> pd.DataFrame:
    params = {"dataset": "TaiwanStockInfo", "token": token}
    try:
        resp = requests.get(FINMIND_API_URL, params=params, timeout=15)
        if resp.status_code == 200:
            df = pd.DataFrame(resp.json().get("data", []))
            df.rename(columns={"StockID": "stock_id", "CompanyName": "stock_name"}, inplace=True)
            df = df[df["stock_id"].str.match(r"^\d{4}$", na=False)]
            if "type" in df.columns:
                df = df[df["type"].isin(["sii", "上市", "TWSE", "twse", "otc", "上櫃", "OTC", "tpex"])]
            keep_cols = ["stock_id", "stock_name"] + (["type"] if "type" in df.columns else [])
            return df[keep_cols].drop_duplicates(subset=["stock_id"]).reset_index(drop=True)
    except Exception as e:
        print(f"🚨 [股票清單] 取得失敗: {e}")
    return pd.DataFrame([{"stock_id": "2330", "stock_name": "台積電", "type": "sii"}])


# ─────────────────────────────────────────────
# 2. 取得近期實際交易日
# ─────────────────────────────────────────────
@st.cache_data(ttl=3600)
def get_recent_trading_dates(token: str, lookback_days: int = 250) -> list:
    start = (datetime.today() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end   = datetime.today().strftime("%Y-%m-%d")
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": "2330",
        "start_date": start,
        "end_date": end,
        "token": token,
    }
    try:
        resp = requests.get(FINMIND_API_URL, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                return pd.DataFrame(data)["date"].tolist()
    except Exception as e:
        print(f"🚨 [交易日曆] 連線發生錯誤: {e}")
    return []


# ─────────────────────────────────────────────
# 3. 大補帖：按日期批量下載全市場資料
# ─────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _bulk_download(dataset: str, dates: list, token: str) -> pd.DataFrame:
    all_data = []
    for i, d in enumerate(dates):
        params = {"dataset": dataset, "start_date": d, "end_date": d, "token": token}
        try:
            resp = requests.get(FINMIND_API_URL, params=params, timeout=15)
            if resp.status_code == 200:
                data = resp.json().get("data", [])
                if data:
                    all_data.extend(data)
                print(f"✅ [{dataset}] {d} 下載成功 ({i+1}/{len(dates)})")
            else:
                print(f"🚨 [{dataset}] {d} 遭拒絕: {resp.text}")
        except Exception as e:
            print(f"🚨 [{dataset}] {d} 連線錯誤: {e}")
        time.sleep(0.1)

    if all_data:
        df = pd.DataFrame(all_data)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        if "StockID" in df.columns:
            df.rename(columns={"StockID": "stock_id"}, inplace=True)
        return df
    return pd.DataFrame()


# ─────────────────────────────────────────────
# 4. 主控台：大補帖（股價 + 集保，不含外資）
# ─────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_all_market_data(token: str, use_c45: bool = True):
    trading_dates = get_recent_trading_dates(token)
    if not trading_dates:
        print("🚨 無法取得交易日曆，大補帖中止！")
        return {}, {}

    print("\n📦 === 大補帖啟動：股價 + 集保（外資移至事後補充）===")

    recent_120_dates = trading_dates[-120:]
    price_df = _bulk_download("TaiwanStockPriceAdj", recent_120_dates, token)

    holdings_df = pd.DataFrame()
    if use_c45:
        fridays = [d for d in trading_dates if pd.to_datetime(d).weekday() == 4]
        holdings_df = _bulk_download("TaiwanStockHoldingSharesPer", fridays[-6:], token)

    print("\n📦 === 大補帖下載完畢，建立高速字典 ===")
    prices_dict   = dict(tuple(price_df.groupby("stock_id")))   if not price_df.empty   else {}
    holdings_dict = dict(tuple(holdings_df.groupby("stock_id"))) if not holdings_df.empty else {}

    return prices_dict, holdings_dict


# ─────────────────────────────────────────────
# 5. 事後補充：只為篩選後的贏家抓外資資料
#
#  TaiwanStockInstitutionalInvestorsBuySell 已同時包含上市與上櫃。
#  按日期 bulk 下載即可拿到全市場，不需要分流。
#  之前上櫃顯示 0 的原因是 name 欄位篩選用中文沒匹配，
#  現在已改為 Foreign_Investor，上市上櫃都能正確取到。
# ─────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_foreign_data_for_winners(
    winner_sids: tuple,    # tuple 才能被 st.cache_data 快取
    type_map: tuple,       # 保留簽名相容，不再用來分流
    token: str,
    lookback_days: int = 15,
    c5_days: int = 0,      # 僅記錄用，實際過濾在 app.py
) -> dict:
    """
    針對篩選後的贏家補充外資資料。
    上市 + 上櫃 都在 TaiwanStockInstitutionalInvestorsBuySell 裡，
    bulk 下載後過濾贏家，回傳 {stock_id: inst_df}。
    """
    if not winner_sids:
        return {}

    trading_dates = get_recent_trading_dates(token)
    if not trading_dates:
        return {}

    recent_dates = trading_dates[-lookback_days:]
    print(f"\n📡 [外資補充] bulk 下載近 {len(recent_dates)} 天（上市+上櫃）...")

    inst_df = _bulk_download(
        "TaiwanStockInstitutionalInvestorsBuySell", recent_dates, token
    )

    if inst_df.empty:
        print("⚠️ [外資補充] 無任何資料")
        return {}

    inst_df = inst_df[inst_df["stock_id"].isin(winner_sids)]
    found   = inst_df["stock_id"].unique().tolist()
    missing = [s for s in winner_sids if s not in found]

    print(f"✅ [外資補充] 取得 {len(found)} 檔：{found}")
    if missing:
        print(f"⚠️ [外資補充] 查無資料（ETF 或特殊商品）：{missing}")

    return dict(tuple(inst_df.groupby("stock_id")))


# ─────────────────────────────────────────────
# 條件 1：當日收盤價 > 5/10/20 週均線
# ─────────────────────────────────────────────
def check_above_weekly_mas(df: pd.DataFrame) -> dict:
    result = {"passed": False, "close": 0, "wma5": 0, "wma10": 0, "wma20": 0}
    if df is None or df.empty:
        return result

    try:
        df = df.copy()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)

        weekly_df = df.resample("W-FRI").agg({"close": "last"}).dropna()
        if len(weekly_df) < 20:
            return result

        weekly_df["WMA5"]  = weekly_df["close"].rolling(5).mean()
        weekly_df["WMA10"] = weekly_df["close"].rolling(10).mean()
        weekly_df["WMA20"] = weekly_df["close"].rolling(20).mean()
        weekly_df = weekly_df.dropna()

        if len(weekly_df) < 1:
            return result

        latest = weekly_df.iloc[-1]
        close  = latest["close"]
        w5, w10, w20 = latest["WMA5"], latest["WMA10"], latest["WMA20"]

        result.update({
            "close":  round(close, 2),
            "wma5":   round(w5, 2),
            "wma10":  round(w10, 2),
            "wma20":  round(w20, 2),
            "passed": (close > w5) and (close > w10) and (close > w20),
        })
    except Exception:
        pass
    return result


# ─────────────────────────────────────────────
# 條件 2：100 日線糾結 或 黃金交叉
# ─────────────────────────────────────────────
def check_ma_tangle_or_golden_cross(
    df: pd.DataFrame,
    tangle_threshold_pct: float = 3.0,
    check_golden_cross: bool = True,
    check_tangle: bool = True,
) -> dict:
    result = {
        "passed": False, "ma5": 0, "ma20": 0, "ma60": 0, "ma100": 0,
        "is_tangle": False, "is_golden_cross": False, "tangle_spread_pct": 0,
    }
    if df is None or df.empty or len(df) < 100:
        return result

    try:
        df = df.copy()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.sort_values("date").reset_index(drop=True)

        for w in [5, 20, 60, 100]:
            df[f"MA{w}"] = df["close"].rolling(w, min_periods=w).mean()

        df = df.dropna(subset=["MA5", "MA20", "MA60", "MA100"])
        if len(df) < 2:
            return result

        today, yesterday = df.iloc[-1], df.iloc[-2]
        ma5, ma20, ma60, ma100 = today["MA5"], today["MA20"], today["MA60"], today["MA100"]

        ma_min     = min(ma5, ma20, ma60, ma100)
        spread_pct = (max(ma5, ma20, ma60, ma100) - ma_min) / ma_min * 100 if ma_min > 0 else 999

        is_tangle       = spread_pct < tangle_threshold_pct
        is_golden_cross = (today["MA20"] > today["MA100"]) and (yesterday["MA20"] <= yesterday["MA100"])

        result.update({
            "ma5": round(ma5, 2), "ma20": round(ma20, 2),
            "ma60": round(ma60, 2), "ma100": round(ma100, 2),
            "is_tangle": is_tangle, "is_golden_cross": is_golden_cross,
            "tangle_spread_pct": round(spread_pct, 2),
        })

        if check_tangle and check_golden_cross:
            result["passed"] = is_tangle and is_golden_cross
        elif check_tangle:
            result["passed"] = is_tangle
        elif check_golden_cross:
            result["passed"] = is_golden_cross
    except Exception:
        pass
    return result


# ─────────────────────────────────────────────
# 外資連買計算（展示用 + 條件⑤判斷用）
# ─────────────────────────────────────────────
def get_foreign_buy_info(inst_df: pd.DataFrame) -> dict:
    """
    計算外資連續買超天數 + 今日外資淨買（股）
    只計算 Foreign_Investor，排除 Foreign_Dealer_Self（外資自營）
    """
    result = {"foreign_net_today": 0, "foreign_consecutive_days": 0}
    if inst_df is None or inst_df.empty:
        return result

    try:
        inst_df = inst_df.copy()

        # 只取 Foreign_Investor（不含外資自營）
        mask       = inst_df["name"].str.contains("Foreign_Investor|外資及陸資", case=False, na=False)
        foreign_df = inst_df[mask].copy()

        if foreign_df.empty:
            return result

        foreign_df["buy"]  = pd.to_numeric(foreign_df["buy"],  errors="coerce").fillna(0)
        foreign_df["sell"] = pd.to_numeric(foreign_df["sell"], errors="coerce").fillna(0)
        foreign_df["net"]  = foreign_df["buy"] - foreign_df["sell"]

        daily_net = (
            foreign_df.groupby("date")["net"]
            .sum().reset_index().sort_values("date")
        )

        if not daily_net.empty:
            result["foreign_net_today"]         = int(daily_net["net"].iloc[-1])
            result["foreign_consecutive_days"]  = _count_consecutive_positive(daily_net["net"].values)
    except Exception as e:
        print(f"  [外資] 計算例外: {e}")
    return result


# ─────────────────────────────────────────────
# 條件 3 & 4：集保股權分散表
# ─────────────────────────────────────────────
def check_shareholding_distribution(
    df: pd.DataFrame,
    whale_increase_weeks: int = 2,
    shareholder_decrease_weeks: int = 2,
) -> dict:
    result = {
        "whale_passed": False, "shareholder_passed": False,
        "latest_whale_people": 0, "latest_total_people": 0,
        "whale_trend": 0, "total_trend": 0,
    }
    if df is None or df.empty:
        return result

    try:
        df = df.copy()
        df["people"] = pd.to_numeric(df["people"], errors="coerce").fillna(0)
        weekly_dates = sorted(df["date"].unique())
        if len(weekly_dates) < 2:
            return result

        weekly_stats = []
        for d in weekly_dates:
            week_df  = df[df["date"] == d]
            valid_df = week_df[
                ~week_df["HoldingSharesLevel"].astype(str).str.contains("合計|total", case=False, na=False)
            ]
            whale_mask = valid_df["HoldingSharesLevel"].astype(str).str.contains(
                r"400,001|600,001|800,001|1,000,001|2,000,001|3,000,001|4,000,001|5,000,001", na=False
            )
            weekly_stats.append({
                "date":         d,
                "whale_people": valid_df[whale_mask]["people"].sum(),
                "total_people": valid_df["people"].sum(),
            })

        stats_df = pd.DataFrame(weekly_stats).sort_values("date")
        if len(stats_df) < 2:
            return result

        latest = stats_df.iloc[-1]
        result.update({
            "latest_whale_people": int(latest["whale_people"]),
            "latest_total_people": int(latest["total_people"]),
        })

        n_whale       = min(whale_increase_weeks,       len(stats_df) - 1)
        n_shareholder = min(shareholder_decrease_weeks, len(stats_df) - 1)

        whale_values = stats_df["whale_people"].values
        total_values = stats_df["total_people"].values

        whale_increasing = (
            all(whale_values[-(i)] > whale_values[-(i+1)] for i in range(1, n_whale + 1))
            if len(whale_values) > n_whale else False
        )
        total_decreasing = (
            all(total_values[-(i)] < total_values[-(i+1)] for i in range(1, n_shareholder + 1))
            if len(total_values) > n_shareholder else False
        )

        result.update({
            "whale_trend":        int(latest["whale_people"] - stats_df.iloc[-2]["whale_people"]),
            "total_trend":        int(latest["total_people"] - stats_df.iloc[-2]["total_people"]),
            "whale_passed":       whale_increasing,
            "shareholder_passed": total_decreasing,
        })
    except Exception as e:
        print(f"集保運算錯誤: {e}")
    return result


# ─────────────────────────────────────────────
# 輔助函式
# ─────────────────────────────────────────────
def _count_consecutive_positive(values: np.ndarray) -> int:
    count = 0
    for v in reversed(values):
        if pd.notna(v) and v > 0:
            count += 1
        else:
            break
    return count
