"""
app.py
台股條件選股 Web App

條件執行順序：
① 當日收盤 > 5/10/20 週均線       ─┐
② 100日線糾結 或 黃金交叉          ─┤ 大補帖，極速過濾
③ 400張大戶人數持續增加            ─┤
④ 總股東人數持續下降               ─┘
⑤ 外資連買 N 天  ← 永遠最後執行，且必須有其他條件才能啟動
   上市：按日期 bulk 下載（快取，不重複打）
   上櫃：只有贏家裡有上櫃才額外逐股抓
"""

import streamlit as st
import pandas as pd
import time

from data_loader import (
    get_all_stocks,
    load_all_market_data,
    load_foreign_data_for_winners,
    check_above_weekly_mas,
    check_ma_tangle_or_golden_cross,
    get_foreign_buy_info,
    check_shareholding_distribution,
)

# ─────────────────────────────────────────────
# 頁面設定
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="台股條件選股工具",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📈 台股條件選股工具")
st.caption("資料來源：FinMind API (Backer)")

# ─────────────────────────────────────────────
# 側邊欄：條件設定
# ─────────────────────────────────────────────
st.sidebar.header("🔧 選股條件設定")
st.sidebar.markdown("---")

st.sidebar.subheader("① 當日收盤價 > 5, 10, 20 週均線")
use_c1 = st.sidebar.checkbox("啟用條件 1", value=True)
st.sidebar.markdown("---")

st.sidebar.subheader("② 100日線糾結【且/或】黃金交叉")
use_c2 = st.sidebar.checkbox("啟用條件 2", value=True)
c2_tangle = st.sidebar.checkbox("包含：均線糾結", value=True, disabled=not use_c2)
c2_golden = st.sidebar.checkbox("包含：黃金交叉（MA20穿MA100）", value=True, disabled=not use_c2)
c2_tangle_pct = st.sidebar.slider(
    "糾結判斷門檻（四線最大差距 < N%）", 1.0, 10.0, 3.0, 0.5, disabled=not use_c2
)
st.sidebar.markdown("---")

st.sidebar.subheader("③ 400張大戶人數持續增加")
use_c3 = st.sidebar.checkbox("啟用條件 3", value=True)
c3_weeks = st.sidebar.slider("連續增加週數 ≥", 1, 6, 2, disabled=not use_c3)
st.sidebar.markdown("---")

st.sidebar.subheader("④ 總股東人數持續下降")
use_c4 = st.sidebar.checkbox("啟用條件 4", value=True)
c4_weeks = st.sidebar.slider("連續下降週數 ≥", 1, 6, 2, disabled=not use_c4)
st.sidebar.markdown("---")

# ── 條件⑤：外資連買（永遠最後執行）──
st.sidebar.subheader("⑤ 外資連買 N 天（最後篩選）")
use_c5 = st.sidebar.checkbox("啟用條件 5", value=False)
c5_days = st.sidebar.slider("外資連續買超天數 ≥", 1, 20, 3, disabled=not use_c5)

# 有其他條件才顯示正常，否則給警告
other_conditions_on = use_c1 or use_c2 or use_c3 or use_c4
if use_c5 and not other_conditions_on:
    st.sidebar.warning("⚠️ 外資連買條件需搭配至少一個其他條件才會生效，單獨勾選不會啟動掃描。")

# ─────────────────────────────────────────────
# 開始選股
# ─────────────────────────────────────────────
start_btn = st.button("🔍 開始全市場極速掃描", type="primary", use_container_width=True)

if start_btn:

    # 如果只有條件⑤，直接擋掉
    if use_c5 and not other_conditions_on:
        st.error("❌ 外資連買條件需搭配至少一個其他條件（①②③④）才能啟動，請勾選其他條件。")
        st.stop()

    # 如果什麼都沒勾
    if not any([use_c1, use_c2, use_c3, use_c4, use_c5]):
        st.error("❌ 請至少啟用一個條件。")
        st.stop()

    # 取得 Token
    try:
        raw_token = st.secrets.get("FINMIND_TOKEN", "")
        token = raw_token.replace("\n", "").replace("\r", "").replace(" ", "").strip()
    except Exception:
        token = ""

    if not token:
        st.error("❌ 找不到 Token！請確認 .streamlit/secrets.toml 設定正確。")
        st.stop()

    status = st.empty()
    progress = st.progress(0)

    # ── Step 1：取得股票清單 ──
    status.text("📋 取得全市場股票清單中...")
    stocks_df = get_all_stocks(token)

    if stocks_df.empty:
        st.error("❌ 無法取得股票清單")
        st.stop()

    stock_ids = stocks_df["stock_id"].tolist()
    name_map  = dict(zip(stocks_df["stock_id"], stocks_df["stock_name"]))
    type_col  = stocks_df["type"].values if "type" in stocks_df.columns else ["sii"] * len(stocks_df)
    type_map  = dict(zip(stocks_df["stock_id"], type_col))
    total     = len(stock_ids)

    # ── Step 2：大補帖下載（股價 + 集保）──
    need_holdings = use_c3 or use_c4
    with st.spinner(f"📦 大補帖引擎：下載全市場股價{' + 集保' if need_holdings else ''}資料..."):
        start_bulk = time.time()
        prices_dict, holdings_dict = load_all_market_data(token, use_c45=need_holdings)

    if not prices_dict:
        st.error("❌ 大補帖資料下載失敗，請檢查網路或 API 額度！")
        st.stop()

    st.success(
        f"⚡ 市場資料載入完成！耗時 {round(time.time() - start_bulk, 1)} 秒。開始條件過濾..."
    )

    # ── Step 3：漏斗篩選（條件①②③④）──
    winner_sids = []
    winner_data = {}

    for i, sid in enumerate(stock_ids):
        # 進度條留 80%，剩下 20% 給外資補充
        progress.progress(int((i + 1) / total * 80))
        status.text(f"🔍 掃描 {sid} {name_map.get(sid, '')} ({i+1}/{total})")

        stock_price_df    = prices_dict.get(sid, pd.DataFrame())
        stock_holdings_df = holdings_dict.get(sid, pd.DataFrame())

        any_failed = False
        c1_res, c2_res, c34_res = {}, {}, {}

        if use_c1 and not any_failed:
            c1_res = check_above_weekly_mas(stock_price_df)
            if not c1_res.get("passed", False):
                any_failed = True

        if use_c2 and not any_failed:
            c2_res = check_ma_tangle_or_golden_cross(
                stock_price_df, c2_tangle_pct, c2_golden, c2_tangle
            )
            if not c2_res.get("passed", False):
                any_failed = True

        if (use_c3 or use_c4) and not any_failed:
            c34_res = check_shareholding_distribution(
                stock_holdings_df, c3_weeks, c4_weeks
            )
            if use_c3 and not c34_res.get("whale_passed", False):
                any_failed = True
            if use_c4 and not c34_res.get("shareholder_passed", False):
                any_failed = True

        if not any_failed:
            winner_sids.append(sid)
            winner_data[sid] = {
                "c1":       c1_res,
                "c2":       c2_res,
                "c34":      c34_res,
                "price_df": stock_price_df,
            }

    progress.progress(80)
    status.text(f"✅ 初步篩選完畢，找到 {len(winner_sids)} 檔候選。")

    # ── Step 4：補充外資資料（贏家才抓，上櫃才額外打）──
    # 不論有沒有勾選條件⑤，外資資料都要抓（因為顯示欄位需要）
    inst_dict = {}
    if winner_sids:
        status.text(f"📡 正在補充 {len(winner_sids)} 檔贏家外資資料...")
        inst_dict = load_foreign_data_for_winners(
            winner_sids=tuple(winner_sids),
            type_map=tuple(type_map.items()),
            token=token,
            c5_days=c5_days if use_c5 else 0,  # 傳入天數，0 表示不篩選
        )

    progress.progress(95)

    # ── Step 5：組裝結果，條件⑤在這裡做最後過濾 ──
    results = []
    filtered_by_c5 = 0  # 記錄被條件⑤濾掉幾支

    for sid in winner_sids:
        data   = winner_data[sid]
        c1_res = data["c1"]  if data["c1"]  else check_above_weekly_mas(data["price_df"])
        c2_res = data["c2"]  if data["c2"]  else check_ma_tangle_or_golden_cross(
                     data["price_df"], c2_tangle_pct, False, False)
        c34_res = data["c34"] if data["c34"] else check_shareholding_distribution(
                     pd.DataFrame(), 0, 0)

        # 外資資料
        inst_df_sid  = inst_dict.get(sid, pd.DataFrame())
        foreign_info = get_foreign_buy_info(inst_df_sid)

        # ── 條件⑤：外資連買最後把關 ──
        if use_c5:
            if foreign_info["foreign_consecutive_days"] < c5_days:
                filtered_by_c5 += 1
                continue  # 不符合，跳過

        # 今日漲跌
        try:
            price_df        = data["price_df"]
            latest_close    = float(price_df["close"].iloc[-1])
            yesterday_close = float(price_df["close"].iloc[-2])
            daily_pct       = (latest_close - yesterday_close) / yesterday_close * 100
        except Exception:
            latest_close, daily_pct = 0.0, 0.0

        wma20    = c1_res.get("wma20", 0)
        bias_20w = ((latest_close - wma20) / wma20 * 100) if wma20 > 0 else 0

        results.append({
            "代號":              sid,
            "名稱":              name_map.get(sid, ""),
            "收盤價":            latest_close,
            "今日漲跌 (%)":      daily_pct,
            "糾結度 (%)":        c2_res.get("tangle_spread_pct", 0.0),
            "乖離 20WMA (%)":    round(bias_20w, 2),
            "外資連買 (天)":     foreign_info["foreign_consecutive_days"],
            "今日外資買超 (張)": int(foreign_info["foreign_net_today"] // 1000),
            "大戶週變化 (人)":   c34_res.get("whale_trend", 0),
            "散戶週變化 (人)":   c34_res.get("total_trend", 0),
        })

    progress.progress(100)

    # 顯示條件⑤過濾說明
    if use_c5 and filtered_by_c5 > 0:
        status.text(f"✅ 分析完成！條件⑤（外資連買≥{c5_days}天）再過濾掉 {filtered_by_c5} 檔。")
    else:
        status.text("✅ 分析完成！")

    st.markdown("---")

    # ── Step 6：顯示結果 ──
    if not results:
        msg = "😕 全市場掃描完畢，今日無完全符合條件之標的。"
        if use_c5 and filtered_by_c5 > 0:
            msg += f"\n（條件⑤外資連買≥{c5_days}天，另過濾了 {filtered_by_c5} 檔）"
        st.warning(msg)
    else:
        result_df = pd.DataFrame(results)

        stock_list_str = "、".join(
            f"{row['代號']} {row['名稱']}" for _, row in result_df.iterrows()
        )
        st.success(f"🎉 找到 {len(results)} 檔超級潛力股：{stock_list_str}")

        display_df = result_df.copy()
        display_df["收盤價"]            = display_df["收盤價"].apply(lambda x: f"{x:.2f}")
        display_df["今日漲跌 (%)"]      = display_df["今日漲跌 (%)"].apply(lambda x: f"{x:.2f} %")
        display_df["糾結度 (%)"]        = display_df["糾結度 (%)"].apply(lambda x: f"{x:.2f} %")
        display_df["乖離 20WMA (%)"]    = display_df["乖離 20WMA (%)"].apply(lambda x: f"{x:.2f} %")
        display_df["外資連買 (天)"]     = display_df["外資連買 (天)"].apply(lambda x: f"{int(x)} 天")
        display_df["今日外資買超 (張)"] = display_df["今日外資買超 (張)"].apply(lambda x: f"{int(x)} 張")
        display_df["大戶週變化 (人)"]   = display_df["大戶週變化 (人)"].apply(lambda x: f"{int(x)} 人")
        display_df["散戶週變化 (人)"]   = display_df["散戶週變化 (人)"].apply(lambda x: f"{int(x)} 人")
        display_df["代號"]              = display_df["代號"].astype(str)

        st.dataframe(display_df, width="stretch")

st.markdown("---")
st.caption("⚠️ 本工具僅供學習與量化策略研究，不構成任何投資建議。投資有風險，請自行判斷。")
