"""
app.py
台股條件選股 Web App（新版 - 五大條件）
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import time

from data_loader import (
    get_all_stocks,
    check_breakout_weekly_ma,
    check_ma_tangle_or_golden_cross,
    check_price_down_inst_buy,
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

st.title("📈 台股條件選股工具 v2")
st.caption("資料來源：FinMind API（免費版）｜技術面每日更新，籌碼面每週更新")

# ─────────────────────────────────────────────
# 側邊欄
# ─────────────────────────────────────────────
st.sidebar.header("🔧 選股條件設定")
st.sidebar.markdown("---")

# ── 條件 1：突破還原週均線 ──
st.sidebar.subheader("① 突破還原週均線（5MA）")
use_c1 = st.sidebar.checkbox("啟用條件 1", value=True)
c1_strict = st.sidebar.radio(
    "模式",
    ["突破當日（昨在下方，今在上方）", "只要今日在週均線上方"],
    disabled=not use_c1,
    key="c1_mode"
)

st.sidebar.markdown("---")

# ── 條件 2：100日線糾結或黃金交叉 ──
st.sidebar.subheader("② 100日線糾結或黃金交叉")
use_c2 = st.sidebar.checkbox("啟用條件 2", value=False)
c2_tangle = st.sidebar.checkbox("包含：均線糾結", value=True, disabled=not use_c2)
c2_golden = st.sidebar.checkbox("包含：黃金交叉（MA20穿MA100）", value=True, disabled=not use_c2)
c2_tangle_pct = st.sidebar.slider(
    "糾結判斷門檻（四線最大差距 < N%）",
    min_value=1.0, max_value=10.0, value=3.0, step=0.5,
    disabled=not use_c2,
)

st.sidebar.markdown("---")

# ── 條件 3：跌但法人買 ──
st.sidebar.subheader("③ 股價下跌但主力持續買入")
use_c3 = st.sidebar.checkbox("啟用條件 3", value=False)
st.sidebar.info(
    "💡 「主力」使用三大法人替代\n（分點資料需 FinMind 付費方案）",
    icon="ℹ️"
)
c3_days = st.sidebar.slider(
    "法人連續買超天數 ≥",
    min_value=1, max_value=10, value=3,
    disabled=not use_c3,
)

st.sidebar.markdown("---")

# ── 條件 4：大戶增加 ──
st.sidebar.subheader("④ 400張大戶人數持續增加")
use_c4 = st.sidebar.checkbox("啟用條件 4", value=False)
st.sidebar.warning("⚠️ 股權分散表為每週更新，非每日", icon="⚠️")
c4_weeks = st.sidebar.slider(
    "連續增加週數 ≥",
    min_value=1, max_value=6, value=2,
    disabled=not use_c4,
)

st.sidebar.markdown("---")

# ── 條件 5：股東人數下降 ──
st.sidebar.subheader("⑤ 總股東人數持續下降")
use_c5 = st.sidebar.checkbox("啟用條件 5", value=False)
st.sidebar.warning("⚠️ 股權分散表為每週更新，非每日", icon="⚠️")
c5_weeks = st.sidebar.slider(
    "連續下降週數 ≥",
    min_value=1, max_value=6, value=2,
    disabled=not use_c5,
)

st.sidebar.markdown("---")

# ── 其他設定 ──
st.sidebar.subheader("⚙️ 其他設定")
market_filter = st.sidebar.multiselect(
    "市場範圍",
    options=["上市", "上櫃"],
    default=["上市"],
)
use_quick = st.sidebar.checkbox(
    "⚡ 快速模式（只跑台灣 50 大權值股）",
    value=True,
    help="鎖定 0050 成分股，速度快且不易超過 API 限制",
)

# ─────────────────────────────────────────────
# 主畫面：條件摘要
# ─────────────────────────────────────────────
with st.expander("📋 目前已啟用的條件", expanded=True):
    enabled = []
    if use_c1:
        mode_text = "突破當日" if "突破當日" in c1_strict else "站上"
        enabled.append(f"✅ 條件1：{mode_text}還原週均線（5MA）")
    if use_c2:
        sub = []
        if c2_tangle: sub.append(f"糾結（差距<{c2_tangle_pct}%）")
        if c2_golden: sub.append("MA20黃金交叉MA100")
        enabled.append(f"✅ 條件2：100日線 {'或'.join(sub)}")
    if use_c3:
        enabled.append(f"✅ 條件3：股價下跌且三大法人連買 ≥ {c3_days} 天")
    if use_c4:
        enabled.append(f"✅ 條件4：400張大戶人數連增 ≥ {c4_weeks} 週（週資料）")
    if use_c5:
        enabled.append(f"✅ 條件5：總股東人數連降 ≥ {c5_weeks} 週（週資料）")

    if enabled:
        for e in enabled:
            st.write(e)
        if not any([use_c1, use_c2, use_c3, use_c4, use_c5]):
            st.warning("⚠️ 未啟用任何條件，將顯示所有股票")
    else:
        st.warning("⚠️ 未啟用任何條件，按下按鈕後將顯示所有股票")

# ─────────────────────────────────────────────
# 開始選股按鈕
# ─────────────────────────────────────────────
start_btn = st.button("🔍 開始選股", type="primary", width="stretch")

# ─────────────────────────────────────────────
# 執行選股邏輯
# ─────────────────────────────────────────────
if start_btn:
    # 取得 Token (終極大掃除版：清除換行與空白)
    try:
        raw_token = st.secrets.get("FINMIND_TOKEN", "")
        token = raw_token.replace("\n", "").replace("\r", "").replace(" ", "").strip()
    except Exception:
        token = ""

    if not token:
        st.warning("⚠️ 未設定 FinMind Token，使用免費限流版（300次/小時）")

    # 決定資料日期範圍
    end_date = datetime.today().strftime("%Y-%m-%d")
    start_date_tech = (datetime.today() - timedelta(days=180)).strftime("%Y-%m-%d")
    start_date_inst = (datetime.today() - timedelta(days=60)).strftime("%Y-%m-%d")

    progress = st.progress(0)
    status = st.empty()

    # 取得股票清單
    status.text("📋 取得股票清單中...")
    stocks_df = get_all_stocks(token, market_filter)

    if stocks_df is None or len(stocks_df) == 0:
        st.error("❌ 無法取得股票清單，請確認網路或稍後再試")
        st.stop()

    if use_quick:
        # 台灣 50 大權值股代號 (0050成分股)
        top_50_ids = [
            '2330', '2317', '2454', '2308', '2382', '2881', '2882', '2891', '2886', '2303', 
            '2884', '1216', '2002', '2885', '2892', '3231', '2357', '2880', '2883', '2887', 
            '2912', '5871', '2379', '2345', '2395', '3711', '1301', '1303', '2327', '2356', 
            '2603', '2609', '2615', '3008', '3034', '3037', '3045', '4904', '4938', '5880', 
            '6505', '6669', '1101', '1590', '2207', '2301', '2412', '3042', '5876', '9910'
        ]
        stocks_df = stocks_df[stocks_df['stock_id'].isin(top_50_ids)]
        st.info(f"⚡ 快速模式：自動分析台灣前 {len(stocks_df)} 大權值股 (0050成分股)")

    stock_ids = stocks_df["stock_id"].tolist()
    name_map = dict(zip(stocks_df["stock_id"], stocks_df["stock_name"]))
    total = len(stock_ids)

    results = []

    for i, sid in enumerate(stock_ids):
        pct = int((i + 1) / total * 90) + 5
        progress.progress(pct)
        status.text(f"🔍 分析 {sid} {name_map.get(sid, '')} ({i+1}/{total})")

        try:
            row = {
                "股票代號": sid,
                "股票名稱": name_map.get(sid, ""),
                "收盤(還原)": "-", "還原5MA": "-", "突破週均線": "-",
                "MA20": "-", "MA100": "-", "均線糾結": "-", "黃金交叉": "-", "糾結差距%": "-",
                "今日漲跌%": "-", "法人連買天數": "-", "今日法人淨買(張)": "-",
                "大戶人數": "-", "大戶週變化": "-", "總股東人數": "-", "股東週變化": "-",
                "符合條件": "",
            }

            passed_list = []
            any_failed = False

            # ── 條件 1 ──
            if use_c1:
                c1 = check_breakout_weekly_ma(sid, start_date_tech, end_date, token)
                row["收盤(還原)"] = c1["close"]
                row["還原5MA"] = c1["ma5_adj"]

                if "突破當日" in c1_strict:
                    row["突破週均線"] = "🚀突破" if c1["breakout"] else ("上方" if c1["above_ma5"] else "下方")
                    if c1["breakout"]: passed_list.append("突破週均線")
                    else: any_failed = True
                else:
                    row["突破週均線"] = "✅上方" if c1["above_ma5"] else "❌下方"
                    if c1["above_ma5"]: passed_list.append("站上週均線")
                    else: any_failed = True

            # ── 條件 2 ──
            if use_c2:
                c2 = check_ma_tangle_or_golden_cross(sid, start_date_tech, end_date, tangle_threshold_pct=c2_tangle_pct, check_golden_cross=c2_golden, check_tangle=c2_tangle, token=token)
                row["MA20"] = c2["ma20"]
                row["MA100"] = c2["ma100"]
                row["均線糾結"] = "✅" if c2["is_tangle"] else "❌"
                row["黃金交叉"] = "✅" if c2["is_golden_cross"] else "❌"
                row["糾結差距%"] = c2["tangle_spread_pct"]

                if c2["passed"]:
                    detail = []
                    if c2["is_tangle"]: detail.append("糾結")
                    if c2["is_golden_cross"]: detail.append("黃金交叉")
                    passed_list.append(f"均線{'&'.join(detail)}")
                else:
                    any_failed = True

            # ── 條件 3 ──
            if use_c3:
                c3 = check_price_down_inst_buy(sid, start_date_inst, end_date, c3_days, token)
                row["今日漲跌%"] = c3["change_pct"]
                row["法人連買天數"] = c3["inst_consecutive_days"]
                row["今日法人淨買(張)"] = c3["inst_net_today"]

                if c3["passed"]: passed_list.append(f"跌+法人買{c3['inst_consecutive_days']}天")
                else: any_failed = True

            # ── 條件 4 & 5 ──
            if use_c4 or use_c5:
                c45 = check_shareholding_distribution(sid, token, whale_increase_weeks=c4_weeks, shareholder_decrease_weeks=c5_weeks)
                row["大戶人數"] = c45["latest_whale_people"]
                row["大戶週變化"] = f"{'+' if c45['whale_trend'] >= 0 else ''}{c45['whale_trend']}"
                row["總股東人數"] = c45["latest_total_people"]
                row["股東週變化"] = f"{'+' if c45['total_trend'] >= 0 else ''}{c45['total_trend']}"

                if use_c4:
                    if c45["whale_passed"]: passed_list.append(f"大戶增加{c4_weeks}週")
                    else: any_failed = True
                if use_c5:
                    if c45["shareholder_passed"]: passed_list.append(f"股東減少{c5_weeks}週")
                    else: any_failed = True

            # ── 判斷是否加入結果 ──
            if not any_failed:
                row["符合條件"] = "、".join(passed_list) if passed_list else "（未設條件）"
                results.append(row)

            # 避免 API 限流
            time.sleep(0.3)

        except Exception as e:
            print(f"[{sid}] 迴圈內發生未預期錯誤: {e}")
            continue

    progress.progress(100)
    status.text("✅ 分析完成！")

    # ─────────────────────────────────────────────
    # 顯示結果
    # ─────────────────────────────────────────────
    st.markdown("---")
    st.subheader(f"📊 選股結果：共 {len(results)} 支符合條件")

    if len(results) == 0:
        st.warning("😕 沒有符合所有條件的股票，建議放寬條件或切換到寬鬆模式")
    else:
        result_df = pd.DataFrame(results)
        
        # 🚀 加碼升級：直接把股票代號和名稱串成一行字印出來！
        stock_list_str = "、".join([f"{row['股票代號']} {row['股票名稱']}" for index, row in result_df.iterrows()])
        st.success(f"✅ 找到的股票清單：{stock_list_str}")

        base_cols = ["股票代號", "股票名稱", "符合條件"]
        tech_cols = ["收盤(還原)", "還原5MA", "突破週均線"] if use_c1 else []
        ma_cols = ["MA20", "MA100", "均線糾結", "黃金交叉", "糾結差距%"] if use_c2 else []
        inst_cols = ["今日漲跌%", "法人連買天數", "今日法人淨買(張)"] if use_c3 else []
        chip_cols = ["大戶人數", "大戶週變化", "總股東人數", "股東週變化"] if (use_c4 or use_c5) else []

        show_cols = base_cols + tech_cols + ma_cols + inst_cols + chip_cols
        show_cols = [c for c in show_cols if c in result_df.columns]

        st.dataframe(result_df[show_cols], width="stretch", hide_index=True)

        csv = result_df.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            label="📥 下載完整結果 CSV",
            data=csv,
            file_name=f"選股_{datetime.today().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

# ─────────────────────────────────────────────
# 頁尾說明
# ─────────────────────────────────────────────
st.markdown("---")
st.caption("⚠️ 本工具僅供學習與研究，不構成任何投資建議。投資有風險，請自行判斷。")
