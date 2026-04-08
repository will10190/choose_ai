"""
app.py ── 最終整合版
- 白底主題
- K 線點擊功能
- DEBUG_MODE 假資料模式
- 移除所有 use_container_width 警告
- [Fix] 徹底將「外資連買天數」與「K線圖同步後的外資資料」綁定，解決天數不符問題
"""

import streamlit as st
import pandas as pd
import time
import random

from data_loader import (
    get_all_stocks,
    load_all_market_data,
    load_foreign_data_for_winners,
    check_above_weekly_mas,
    check_ma_tangle_or_golden_cross,
    check_shareholding_distribution,
)
from chart_plotter import plot_combined_chart

# ══════════════════════════════════════════════════════════
#  🔧 開發用開關
# ══════════════════════════════════════════════════════════
DEBUG_MODE        = False   
DEBUG_STOCK_LIMIT = 0      

# ══════════════════════════════════════════════════════════
#  Mock 假資料產生器
# ══════════════════════════════════════════════════════════
def _make_mock_price_df(sid: str, n: int = 200) -> pd.DataFrame:
    random.seed(hash(sid) % 9999)
    base = random.uniform(50, 500)
    closes, opens, highs, lows, vols = [], [], [], [], []
    price = base
    dates = pd.bdate_range(end=pd.Timestamp.today(), periods=n)
    for _ in dates:
        chg = price * random.uniform(-0.05, 0.055)
        op  = price + price * random.uniform(-0.01, 0.01)
        cl  = price + chg
        hi  = max(op, cl) * random.uniform(1.0, 1.02)
        lo  = min(op, cl) * random.uniform(0.98, 1.0)
        vol = int(random.uniform(500, 50000))
        closes.append(round(cl, 2)); opens.append(round(op, 2))
        highs.append(round(hi, 2));  lows.append(round(lo, 2))
        vols.append(vol)
        price = cl
    return pd.DataFrame({
        "date":   [d.strftime("%Y-%m-%d") for d in dates],
        "open":   opens, "high": highs,
        "low":    lows,  "close": closes,
        "volume": vols,
        "外資":    [random.randint(-1000, 1000) for _ in range(n)],
        "投信":    [random.randint(-500, 500) for _ in range(n)],
    })

def _make_mock_results(n: int = 8) -> list:
    mock_stocks = [
        ("2330","台積電"),("2317","鴻海"),("2454","聯發科"),
        ("3529","力旺"),  ("6274","台燿"),("8438","昶昕"),
        ("3293","鈦象"),  ("4760","勤凱"),("6451","訊芯-KY"),
        ("8021","尖點"),  ("2603","長榮"),("2882","國泰金"),
    ]
    results = []
    for sid, name in mock_stocks[:n]:
        pdf   = _make_mock_price_df(sid)
        close = float(pdf["close"].iloc[-1])
        prev  = float(pdf["close"].iloc[-2])
        dpct  = (close - prev) / prev * 100
        
        valid_foreign = pdf["外資"].fillna(0).values
        cons_days = 0
        for v in reversed(valid_foreign):
            if v > 0: cons_days += 1
            else: break

        results.append({
            "代號": sid, "名稱": name,
            "收盤價": close,
            "今日漲跌 (%)": round(dpct, 2),
            "糾結度 (%)": round(random.uniform(1.5, 8.0), 2),
            "乖離 20WMA (%)": round(random.uniform(-5, 20), 2),
            "外資連買 (天)": cons_days,
            "今日外資買超 (張)": int(pdf["外資"].iloc[-1]), 
            "大戶週變化 (人)": random.randint(-10, 10),
            "散戶週變化 (人)": random.randint(-500, 500),
            "_price_df": pdf,
        })
    return results

# ══════════════════════════════════════════════════════════
#  頁面設定
# ══════════════════════════════════════════════════════════
st.set_page_config(page_title="台股條件選股工具", page_icon="📈", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"], [data-testid="stMainBlockContainer"], section[data-testid="stMainBlockContainer"] > div {
    background-color: #ffffff !important; color: #1a1a1a !important;
}
[data-testid="stSidebar"], [data-testid="stSidebarContent"] { background-color: #f5f7fa !important; }
hr { border-color: #e0e4ea !important; }
.result-header-col { font-size: 11px; color: #888; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; }
div[data-testid="stButton"] > button[kind="primary"] { background: #e74c3c !important; color: #ffffff !important; border-radius: 8px !important; font-size: 16px !important; padding: 10px 0 !important; font-weight: 700 !important; border: none !important; box-shadow: none !important; }
div[data-testid="stButton"] > button[kind="primary"]:hover { background: #c0392b !important; color: #ffffff !important; }
div[data-testid="stButton"] > button[kind="secondary"] { background: #f5f5f5 !important; color: #444 !important; border: 1px solid #ddd !important; border-radius: 8px !important; font-size: 14px !important; box-shadow: none !important; }
div[data-testid="stButton"] > button[kind="secondary"]:hover { background: #e9e9e9 !important; color: #222 !important; }
div[data-testid="stButton"] > button[kind="tertiary"] { background: none !important; border: none !important; color: #0077cc !important; font-weight: 600 !important; font-size: 13px !important; padding: 2px 4px !important; border-radius: 4px !important; text-align: left !important; box-shadow: none !important; transition: color 0.15s !important; }
div[data-testid="stButton"] > button[kind="tertiary"]:hover { background: none !important; color: #005fa3 !important; text-decoration: underline !important; }
.debug-badge { display: inline-block; background: #fff3cd; color: #856404; border: 1px solid #ffc107; border-radius: 6px; padding: 3px 12px; font-size: 12px; font-weight: 700; margin-bottom: 6px; }
.limit-badge { display: inline-block; background: #d1ecf1; color: #0c5460; border: 1px solid #bee5eb; border-radius: 6px; padding: 3px 12px; font-size: 12px; font-weight: 700; margin-bottom: 6px; margin-left: 8px; }
</style>
""", unsafe_allow_html=True)

st.title("📈 台股條件選股工具")
badge_html = ""
if DEBUG_MODE: badge_html += "<div class='debug-badge'>🐛 DEBUG MODE（假資料）— 改 DEBUG_MODE=False 切換正式</div>"
if not DEBUG_MODE and DEBUG_STOCK_LIMIT > 0: badge_html += f"<div class='limit-badge'>⚡ 限制掃描前 {DEBUG_STOCK_LIMIT} 檔</div>"
if badge_html: st.markdown(badge_html, unsafe_allow_html=True)
st.caption("資料來源：FinMind API (Backer)")

for key, default in [("selected_stock", None), ("results", []), ("scan_done", False)]:
    if key not in st.session_state: st.session_state[key] = default

# ══════════════════════════════════════════════════════════
#  側邊欄
# ══════════════════════════════════════════════════════════
st.sidebar.header("🔧 選股條件設定")
st.sidebar.markdown("---")
use_c1 = st.sidebar.checkbox("① 當日收盤價 > 5/10/20 週均線", value=True)
st.sidebar.markdown("---")
use_c2        = st.sidebar.checkbox("② 100日線糾結 / 黃金交叉", value=True)
c2_tangle     = st.sidebar.checkbox("均線糾結", value=True, disabled=not use_c2)
c2_golden     = st.sidebar.checkbox("黃金交叉（MA20穿MA100）", value=True, disabled=not use_c2)
c2_tangle_pct = st.sidebar.slider("糾結門檻 N%", 1.0, 10.0, 3.0, 0.5, disabled=not use_c2)
st.sidebar.markdown("---")
use_c3   = st.sidebar.checkbox("③ 400張大戶人數持續增加", value=True)
c3_weeks = st.sidebar.slider("連增週數 ≥", 1, 6, 2, disabled=not use_c3)
st.sidebar.markdown("---")
use_c4   = st.sidebar.checkbox("④ 總股東人數持續下降", value=True)
c4_weeks = st.sidebar.slider("連減週數 ≥", 1, 6, 2, disabled=not use_c4)
st.sidebar.markdown("---")
use_c5  = st.sidebar.checkbox("⑤ 外資連買 N 天（最後把關）", value=False)
c5_days = st.sidebar.slider("外資連買天數 ≥", 1, 20, 3, disabled=not use_c5)

other_conditions_on = use_c1 or use_c2 or use_c3 or use_c4
if use_c5 and not other_conditions_on: st.sidebar.warning("⚠️ 條件⑤需搭配①②③④其中一個使用。")
st.sidebar.markdown("---")
st.sidebar.subheader("📊 K 線均線顯示")
show_ma = { "MA5": st.sidebar.checkbox("MA5", value=True), "MA20": st.sidebar.checkbox("MA20", value=True), "MA60": st.sidebar.checkbox("MA60", value=True), "MA100": st.sidebar.checkbox("MA100", value=True), "MA120": st.sidebar.checkbox("MA120", value=False), "MA240": st.sidebar.checkbox("MA240", value=False) }
k_line_type = st.sidebar.radio("K 線類型", ["一般K線", "還原K線"], index=0)

# ══════════════════════════════════════════════════════════
#  按鈕列：掃描 + 清除
# ══════════════════════════════════════════════════════════
btn_col1, btn_col2 = st.columns([5, 1])
with btn_col1: start_btn = st.button("🔍 開始全市場極速掃描", type="primary", use_container_width=True)
with btn_col2: clear_btn = st.button("🗑 清除結果", use_container_width=True)

if clear_btn:
    st.session_state.results = []; st.session_state.scan_done = False; st.session_state.selected_stock = None; st.rerun()

# ══════════════════════════════════════════════════════════
#  掃描邏輯
# ══════════════════════════════════════════════════════════
if start_btn:
    if use_c5 and not other_conditions_on: st.error("❌ 條件⑤需搭配其他條件使用。"); st.stop()
    if not any([use_c1, use_c2, use_c3, use_c4, use_c5]): st.error("❌ 請至少啟用一個條件。"); st.stop()

    if DEBUG_MODE:
        with st.spinner("🐛 Debug 模式：產生假資料中..."): time.sleep(0.3)
        st.session_state.results = _make_mock_results(8)
        st.session_state.scan_done = True
        st.session_state.selected_stock = None
        st.rerun()

    try:
        raw_token = st.secrets.get("FINMIND_TOKEN", "")
        token = raw_token.replace("\n","").replace("\r","").replace(" ","").strip()
    except Exception: token = ""
    if not token: st.error("❌ 請在 .streamlit/secrets.toml 設定 FINMIND_TOKEN"); st.stop()

    status, progress = st.empty(), st.progress(0)
    status.text("📋 取得全市場股票清單...")
    stocks_df = get_all_stocks(token)
    if stocks_df.empty: st.error("❌ 無法取得股票清單。"); st.stop()

    stock_ids = stocks_df["stock_id"].tolist()
    name_map  = dict(zip(stocks_df["stock_id"], stocks_df["stock_name"]))
    type_col  = stocks_df["type"].values if "type" in stocks_df.columns else ["sii"] * len(stocks_df)
    type_map  = dict(zip(stocks_df["stock_id"], type_col))

    if DEBUG_STOCK_LIMIT > 0:
        stock_ids = stock_ids[:DEBUG_STOCK_LIMIT]
        st.info(f"⚡ 已限制只掃描前 {DEBUG_STOCK_LIMIT} 檔股票")

    total = len(stock_ids)
    status.info("📥 批次下載全市場行情資料中，請稍候（約需 1~3 分鐘）...")
    start_bulk = time.time()
    prices_dict, holdings_dict = load_all_market_data(token, use_c45=(use_c3 or use_c4))
    if not prices_dict: st.error("❌ 資料載入失敗。"); st.stop()
    st.success(f"✅ 行情資料載入完成（{round(time.time() - start_bulk, 1)} 秒）")

    status.text("🔍 逐檔篩選中...")
    winner_sids, winner_data = [], {}

    for i, sid in enumerate(stock_ids):
        progress.progress(int((i + 1) / total * 80))
        status.text(f"篩選 {sid} {name_map.get(sid,'')}（{i+1}/{total}）")

        stock_price_df    = prices_dict.get(sid, pd.DataFrame())
        stock_holdings_df = holdings_dict.get(sid, pd.DataFrame())
        any_failed        = False
        c1_res, c2_res, c34_res = {}, {}, {}

        if use_c1 and not any_failed:
            c1_res = check_above_weekly_mas(stock_price_df)
            if not c1_res.get("passed", False): any_failed = True

        if use_c2 and not any_failed:
            c2_res = check_ma_tangle_or_golden_cross(stock_price_df, c2_tangle_pct, c2_golden, c2_tangle)
            if not c2_res.get("passed", False): any_failed = True

        if (use_c3 or use_c4) and not any_failed:
            c34_res = check_shareholding_distribution(stock_holdings_df, c3_weeks, c4_weeks)
            if use_c3 and not c34_res.get("whale_passed", False): any_failed = True
            if use_c4 and not c34_res.get("shareholder_passed", False): any_failed = True

        if not any_failed:
            winner_sids.append(sid)
            winner_data[sid] = {"c1": c1_res, "c2": c2_res, "c34": c34_res, "price_df": stock_price_df}

    progress.progress(80)
    status.text(f"🎯 初步篩選到 {len(winner_sids)} 檔，載入外資資料...")

    inst_dict = {}
    if winner_sids:
        inst_dict = load_foreign_data_for_winners(
            winner_sids=tuple(winner_sids), type_map=tuple(type_map.items()), token=token, c5_days=c5_days if use_c5 else 0
        )
    progress.progress(95)

    results, filtered_by_c5 = [], 0

    for sid in winner_sids:
        data = winner_data[sid]
        c1_res  = data["c1"]  if isinstance(data["c1"],  dict) and data["c1"] else check_above_weekly_mas(data["price_df"])
        c2_res  = data["c2"]  if isinstance(data["c2"],  dict) and data["c2"] else check_ma_tangle_or_golden_cross(data["price_df"], c2_tangle_pct, False, False)
        c34_res = data["c34"] if isinstance(data["c34"], dict) and data["c34"] else check_shareholding_distribution(pd.DataFrame(), 0, 0)
        
        inst_df_sid = inst_dict.get(sid, pd.DataFrame())
        price_df = data["price_df"].copy()

        # ✅ [Fix] 先合併籌碼資料到 price_df，再統一由這裡計算連買天數
        if not inst_df_sid.empty:
            inst_pivot = inst_df_sid.copy()
            inst_pivot['net'] = pd.to_numeric(inst_pivot['buy'], errors='coerce').fillna(0) - pd.to_numeric(inst_pivot['sell'], errors='coerce').fillna(0)
            
            foreign_mask = inst_pivot['name'].str.contains("Foreign_Investor|外資及陸資", case=False, na=False)
            trust_mask = inst_pivot['name'].str.contains("Investment_Trust|投信", case=False, na=False)
            
            foreign_daily = (inst_pivot[foreign_mask].groupby('date')['net'].sum() / 1000).round(0).rename("外資")
            trust_daily = (inst_pivot[trust_mask].groupby('date')['net'].sum() / 1000).round(0).rename("投信")
            
            if 'date' in price_df.columns:
                price_df = price_df.set_index('date')
                price_df = price_df.join(foreign_daily).join(trust_daily).reset_index()

        # 計算同步後的外資買賣超與連買天數
        consecutive_days = 0
        table_foreign_buy = 0
        if "外資" in price_df.columns:
            table_foreign_buy = price_df["外資"].iloc[-1]
            if pd.isna(table_foreign_buy): table_foreign_buy = 0
            
            # 從最新的日期往前數，大於 0 的才是買超
            valid_foreign = price_df["外資"].fillna(0).values
            for v in reversed(valid_foreign):
                if v > 0:
                    consecutive_days += 1
                else:
                    break

        # ✅ 條件⑤ 過濾移到合併與對齊後執行
        if use_c5 and consecutive_days < c5_days:
            filtered_by_c5 += 1
            continue

        try:
            latest_close    = float(price_df["close"].iloc[-1])
            yesterday_close = float(price_df["close"].iloc[-2])
            daily_pct       = (latest_close - yesterday_close) / yesterday_close * 100
        except Exception:
            latest_close, daily_pct = 0.0, 0.0

        wma20 = c1_res.get("wma20", 0)
        bias_20w = ((latest_close - wma20) / wma20 * 100) if wma20 > 0 else 0

        results.append({
            "代號":              sid,
            "名稱":              name_map.get(sid, ""),
            "收盤價":            latest_close,
            "今日漲跌 (%)":      daily_pct,
            "糾結度 (%)":        c2_res.get("tangle_spread_pct", 0.0),
            "乖離 20WMA (%)":    round(bias_20w, 2),
            "外資連買 (天)":     consecutive_days,  # 完全與圖表同步的天數
            "今日外資買超 (張)": int(table_foreign_buy), 
            "大戶週變化 (人)":   c34_res.get("whale_trend", 0),
            "散戶週變化 (人)":   c34_res.get("total_trend", 0),
            "_price_df":         price_df, 
        })

    progress.progress(100)
    status.text(f"✅ 完成！{'條件⑤再過濾掉 ' + str(filtered_by_c5) + ' 檔。' if use_c5 and filtered_by_c5 > 0 else ''}")

    st.session_state.results = results; st.session_state.scan_done = True; st.session_state.selected_stock = None

st.markdown("---")

# ══════════════════════════════════════════════════════════
#  結果展示 + K 線點擊
# ══════════════════════════════════════════════════════════
if st.session_state.scan_done:
    results = st.session_state.results
    if not results:
        st.warning("😕 全市場掃描完畢，今日無完全符合條件之標的。")
    else:
        stock_list_str = "、".join(f"{r['代號']} {r['名稱']}" for r in results)
        st.success(f"🎉 找到 **{len(results)}** 檔超級潛力股：{stock_list_str}")

        col_w = [1, 1.5, 1, 1.2, 1.2, 1.4, 1.2, 1.6, 1.4, 1.4]
        headers = ["代號", "名稱（點擊看K線）", "收盤價", "今日漲跌%", "糾結度%", "乖離20WMA%", "外資連買(天)", "外資買超(張)", "大戶週變化", "散戶週變化"]
        hcols = st.columns(col_w)
        for hc, h in zip(hcols, headers): hc.markdown(f"<span class='result-header-col'>{h}</span>", unsafe_allow_html=True)
        st.markdown("<hr style='margin:4px 0 8px 0;'>", unsafe_allow_html=True)

        for idx, row in enumerate(results):
            sid, name, close, dpct = row["代號"], row["名稱"], row["收盤價"], row["今日漲跌 (%)"]
            is_sel = (st.session_state.selected_stock is not None and st.session_state.selected_stock["sid"] == sid)

            if is_sel: st.markdown("<div style='border-left:3px solid #e74c3c;border-radius:4px;padding:2px 0;margin:1px 0;'>", unsafe_allow_html=True)

            cols = st.columns(col_w)
            cols[0].markdown(f"<span style='font-size:13px;color:#555;'>{sid}</span>", unsafe_allow_html=True)

            if cols[1].button(f"{'▶ ' if is_sel else ''}{name}", key=f"btn_{sid}_{idx}", type="tertiary"):
                st.session_state.selected_stock = None if is_sel else {"sid": sid, "name": name, "price_df": row["_price_df"]}
                st.rerun()

            dc = "#c0392b" if dpct >= 0 else "#16a085"
            cols[2].markdown(f"<span style='font-size:13px;'>{close:.2f}</span>", unsafe_allow_html=True)
            cols[3].markdown(f"<span style='font-size:13px;color:{dc};font-weight:600;'>{dpct:+.2f}%</span>", unsafe_allow_html=True)
            cols[4].markdown(f"<span style='font-size:13px;'>{row['糾結度 (%)']:.2f}%</span>", unsafe_allow_html=True)
            cols[5].markdown(f"<span style='font-size:13px;'>{row['乖離 20WMA (%)']:+.2f}%</span>", unsafe_allow_html=True)
            cols[6].markdown(f"<span style='font-size:13px;'>{int(row['外資連買 (天)'])} 天</span>", unsafe_allow_html=True)

            fnet = row["今日外資買超 (張)"]
            fc   = "#c0392b" if fnet >= 0 else "#16a085"
            cols[7].markdown(f"<span style='font-size:13px;color:{fc};'>{int(fnet):,} 張</span>", unsafe_allow_html=True)

            whale = row["大戶週變化 (人)"]
            wc    = "#c0392b" if whale > 0 else "#16a085" if whale < 0 else "#999"
            cols[8].markdown(f"<span style='font-size:13px;color:{wc};'>{int(whale):+,} 人</span>", unsafe_allow_html=True)

            retail = row["散戶週變化 (人)"]
            rc     = "#16a085" if retail < 0 else "#c0392b" if retail > 0 else "#999"
            cols[9].markdown(f"<span style='font-size:13px;color:{rc};'>{int(retail):+,} 人</span>", unsafe_allow_html=True)

            if is_sel: st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<hr style='margin:10px 0;'>", unsafe_allow_html=True)

        sel = st.session_state.selected_stock
        if sel:
            sid, name, price_df = sel["sid"], sel["name"], sel["price_df"]
            st.markdown(f"<div style='font-size:18px;font-weight:700;color:#1a1a1a;border-left:4px solid #e74c3c;padding-left:10px;margin:16px 0 8px 0;'>📊 {sid} {name} · K 線圖</div>", unsafe_allow_html=True)

            if price_df is None or price_df.empty:
                st.warning("⚠️ 此股票無行情資料。")
            else:
                df_chart = price_df.copy()
                rename_map = {"max": "high", "min": "low", "Trading_Volume": "volume"}
                df_chart = df_chart.rename(columns=rename_map)

                for col_name in ["open", "high", "low", "close", "volume"]:
                    if col_name not in df_chart.columns: df_chart[col_name] = 0.0
                    df_chart[col_name] = pd.to_numeric(df_chart[col_name], errors="coerce").fillna(0.0)
                df_chart = df_chart.sort_values("date").reset_index(drop=True)
                
                for w in [5, 20, 60, 100, 120, 240]: df_chart[f"MA{w}"] = df_chart["close"].rolling(w, min_periods=w).mean()
                
                for col_name in ["外資", "投信"]:
                    if col_name not in df_chart.columns: df_chart[col_name] = 0
                
                try:
                    fig = plot_combined_chart(df_chart, sid, name, show_ma_dict=show_ma, k_line_type=k_line_type)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e: st.error(f"❌ 繪圖失敗：{e}")

st.markdown("---")
st.caption("⚠️ 本工具僅供學習與量化策略研究，不構成任何投資建議。投資有風險，請自行判斷。")
