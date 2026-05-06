"""
app.py ── 最終整合版
- 引入模組化設計：總經儀表板分離至 macro_dashboard.py
- [Fix] 消除 HTML 縮排，解決表格被解析為 Markdown 程式碼區塊的問題
- [Update] 選股結果表格欄位重排：籌碼往前、技術指標往後
"""

import streamlit as st
import pandas as pd
import time
import random

from data_loader import (
    get_all_stocks, load_all_market_data, load_foreign_data_for_winners,
    check_above_weekly_mas, check_ma_tangle_or_golden_cross, check_shareholding_distribution,
    load_industry_chain_for_winners,
    check_kd_golden_cross, check_macd_near_zero,
)
from chart_plotter import plot_combined_chart
from broker_scraper import fetch_broker_list, lookup_branch_code, get_branch_data_cached  # kept for potential direct use
from broker_page import render_broker_page

# 🌟 匯入我們新寫好的獨立總經儀表板模組
from macro_dashboard import render_macro_dashboard

# ══════════════════════════════════════════════════════════
# 🔑 全域獲取 FinMind Token
# ══════════════════════════════════════════════════════════
try:
    raw_token = st.secrets.get("FINMIND_TOKEN", "")
    FINMIND_TOKEN = raw_token.replace("\n","").replace("\r","").replace(" ","").strip()
except Exception:
    FINMIND_TOKEN = ""

# ══════════════════════════════════════════════════════════
# 🔧 開發用開關
# ══════════════════════════════════════════════════════════
DEBUG_MODE        = False
DEBUG_STOCK_LIMIT = 0

# ══════════════════════════════════════════════════════════
# Mock 假資料產生器
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
        closes.append(round(cl, 2)); opens.append(round(op, 2)); highs.append(round(hi, 2)); lows.append(round(lo, 2)); vols.append(vol)
        price = cl
    return pd.DataFrame({"date": [d.strftime("%Y-%m-%d") for d in dates], "open": opens, "high": highs, "low": lows, "close": closes, "volume": vols, "外資": [random.randint(-1000, 1000) for _ in range(n)], "投信": [random.randint(-500, 500) for _ in range(n)]})

def _make_mock_results(n: int = 8) -> list:
    mock_stocks = [("2330","台積電","半導體業 / 晶圓代工"),("2317","鴻海","其他電子業 / EMS代工"),("2454","聯發科","半導體業 / IC設計"),("5443","均豪","平面顯示器 / 生產製程及檢測設備"),("6274","台燿","電子零組件業 / 銅箔基板 (CCL)"),("8438","昶昕","化學工業 / 特用化學"),("3293","鈊象","文化創意業 / 遊戲"),("1216","統一","食品工業 / 乳製品")]
    results = []
    for sid, name, industry in mock_stocks[:n]:
        pdf = _make_mock_price_df(sid)
        close = float(pdf["close"].iloc[-1])
        dpct = (close - float(pdf["close"].iloc[-2])) / float(pdf["close"].iloc[-2]) * 100
        cons_days = 0
        for v in reversed(pdf["外資"].fillna(0).values):
            if v > 0: cons_days += 1
            else: break
        results.append({
            "代號": sid, "名稱": name, "產業": industry, "收盤價": close, "今日漲跌 (%)": round(dpct, 2), "糾結度 (%)": round(random.uniform(1.5, 8.0), 2),
            "乖離 20WMA (%)": round(random.uniform(-5, 20), 2), "外資連買 (天)": cons_days, "今日外資買超 (張)": int(pdf["外資"].iloc[-1]),
            "大戶週變化 (人)": random.randint(-10, 10), "散戶週變化 (人)": random.randint(-500, 500), "_price_df": pdf,
        })
    return results

# ══════════════════════════════════════════════════════════
# 頁面設定與 CSS
# ══════════════════════════════════════════════════════════
st.set_page_config(page_title="台股條件選股工具", page_icon="📈", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"], [data-testid="stMainBlockContainer"] {
    background-color: #f8f9fa !important; color: #1a1a1a !important; 
}
[data-testid="stSidebar"] { background-color: #ffffff !important; border-right: 1px solid #eee; }
.custom-stock-table th { padding: 12px 15px; color: #555; border-bottom: 2px solid #ddd; background-color: #ffffff; }
.custom-stock-table td { padding: 12px; border-bottom: 1px solid #eee; text-align: center; vertical-align: middle; background-color: #ffffff; }
.custom-stock-table tr:hover td { background-color: #f9fafe; }
</style>
""", unsafe_allow_html=True)

st.title("📈 台股條件選股工具")
badge_html = ""
if DEBUG_MODE: badge_html += "<div class='debug-badge'>🐛 DEBUG MODE（假資料）— 改 DEBUG_MODE=False 切換正式</div>"
if not DEBUG_MODE and DEBUG_STOCK_LIMIT > 0: badge_html += f"<div class='limit-badge'>⚡ 限制掃描前 {DEBUG_STOCK_LIMIT} 檔</div>"
if badge_html: st.markdown(badge_html, unsafe_allow_html=True)
st.caption("資料來源：FinMind API (Backer) & 富邦 DJ 資訊網")

# =====================================================================
# 🌟 呼叫模組化的大盤總經儀表板
# =====================================================================
render_macro_dashboard(FINMIND_TOKEN)

# =====================================================================
# 以下為選股與券商查詢邏輯
# =====================================================================

# 初始化 session_state
for key, default in [("selected_stock", None), ("results", []), ("scan_done", False)]:
    if key not in st.session_state: st.session_state[key] = default

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
use_c6 = st.sidebar.checkbox("⑥ KD 黃金交叉（近 N 天）", value=False)
c6_lookback = st.sidebar.slider("黃金交叉在幾天內發生", 1, 5, 3, disabled=not use_c6)
st.sidebar.markdown("---")
use_c7 = st.sidebar.checkbox("⑦ MACD 柱狀體（DIF-DEA）絕對值 < N", value=False)
c7_band = st.sidebar.select_slider("柱狀體門檻", options=[0.1, 0.2, 0.3, 0.5, 1.0], value=0.1, disabled=not use_c7)
c7_hist_pos = st.sidebar.checkbox("同時要求柱狀體 > 0（紅柱翻正）", value=False, disabled=not use_c7)
st.sidebar.markdown("---")
st.sidebar.subheader("📊 K 線均線顯示")
show_ma = {"MA5": st.sidebar.checkbox("MA5", value=True), "MA20": st.sidebar.checkbox("MA20", value=True), "MA60": st.sidebar.checkbox("MA60", value=True), "MA100": st.sidebar.checkbox("MA100", value=True), "MA120": st.sidebar.checkbox("MA120", value=False), "MA240": st.sidebar.checkbox("MA240", value=False)}
k_line_type = st.sidebar.radio("K 線類型", ["一般K線", "還原K線"], index=0)

tab_screen, tab_branch = st.tabs(["🔍 條件選股", "🏦 券商分點查詢"])

with tab_screen:
    btn_col1, btn_col2 = st.columns([5, 1])
    with btn_col1: start_btn = st.button("🔍 開始全市場極速掃描", type="primary", use_container_width=True)
    with btn_col2: clear_btn = st.button("🗑 清除結果", use_container_width=True)

    if clear_btn:
        st.session_state.results = []; st.session_state.scan_done = False; st.session_state.selected_stock = None; st.rerun()

    if start_btn:
        if use_c5 and not other_conditions_on: st.error("❌ 條件⑤需搭配其他條件使用。"); st.stop()
        if not any([use_c1, use_c2, use_c3, use_c4, use_c5, use_c6, use_c7]): st.error("❌ 請至少啟用一個條件。"); st.stop()
        if DEBUG_MODE:
            with st.spinner("🐛 Debug 模式：產生假資料中..."): time.sleep(0.3)
            st.session_state.results = _make_mock_results(8); st.session_state.scan_done = True; st.session_state.selected_stock = None; st.rerun()

        if not FINMIND_TOKEN: st.error("❌ 請在 .streamlit/secrets.toml 設定 FINMIND_TOKEN"); st.stop()

        status, progress = st.empty(), st.progress(0)
        status.text("📋 取得全市場股票清單...")
        stocks_df = get_all_stocks(FINMIND_TOKEN)
        if stocks_df.empty: st.error("❌ 無法取得股票清單。"); st.stop()

        stock_ids = stocks_df["stock_id"].tolist()
        name_map  = dict(zip(stocks_df["stock_id"], stocks_df["stock_name"]))
        type_col  = stocks_df["type"].values if "type" in stocks_df.columns else ["sii"] * len(stocks_df)
        type_map  = dict(zip(stocks_df["stock_id"], type_col))
        
        ind_col = stocks_df["industry_category"].values if "industry_category" in stocks_df.columns else ["未知產業"] * len(stocks_df)
        basic_industry_map = dict(zip(stocks_df["stock_id"], ind_col))

        if DEBUG_STOCK_LIMIT > 0: stock_ids = stock_ids[:DEBUG_STOCK_LIMIT]; st.info(f"⚡ 已限制只掃描前 {DEBUG_STOCK_LIMIT} 檔股票")
        total = len(stock_ids)
        status.info("📥 批次下載全市場行情資料中，請稍候（約需 1~3 分鐘）...")
        start_bulk = time.time()
        prices_dict, holdings_dict = load_all_market_data(FINMIND_TOKEN, use_c45=(use_c3 or use_c4))
        if not prices_dict: st.error("❌ 資料載入失敗。"); st.stop()
        st.success(f"✅ 行情資料載入完成（{round(time.time() - start_bulk, 1)} 秒）")

        status.text("🔍 逐檔篩選中...")
        winner_sids, winner_data = [], {}

        for i, sid in enumerate(stock_ids):
            progress.progress(int((i + 1) / total * 80))
            status.text(f"篩選 {sid} {name_map.get(sid,'')}（{i+1}/{total}）")
            stock_price_df, stock_holdings_df = prices_dict.get(sid, pd.DataFrame()), holdings_dict.get(sid, pd.DataFrame())
            any_failed, c1_res, c2_res, c34_res, c6_res, c7_res = False, {}, {}, {}, {}, {}

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
            if use_c6 and not any_failed:
                c6_res = check_kd_golden_cross(stock_price_df, lookback=c6_lookback)
                if not c6_res.get("passed", False): any_failed = True
            if use_c7 and not any_failed:
                c7_res = check_macd_near_zero(stock_price_df, histogram_band=c7_band, require_positive_histogram=c7_hist_pos)
                if not c7_res.get("passed", False): any_failed = True

            if not any_failed:
                winner_sids.append(sid)
                winner_data[sid] = {"c1": c1_res, "c2": c2_res, "c34": c34_res, "c6": c6_res, "c7": c7_res, "price_df": stock_price_df}

        progress.progress(80); status.text(f"🎯 篩選到 {len(winner_sids)} 檔贏家！正連線取得外資與次產業資料...")
        
        inst_dict = load_foreign_data_for_winners(tuple(winner_sids), tuple(type_map.items()), FINMIND_TOKEN, c5_days=c5_days if use_c5 else 0) if winner_sids else {}
        chain_dict = load_industry_chain_for_winners(tuple(winner_sids), FINMIND_TOKEN) if winner_sids else {}
        progress.progress(95)

        results, filtered_by_c5 = [], 0
        for sid in winner_sids:
            data = winner_data[sid]
            c1_res = data["c1"] if isinstance(data["c1"], dict) and data["c1"] else check_above_weekly_mas(data["price_df"])
            c2_res = data["c2"] if isinstance(data["c2"], dict) and data["c2"] else check_ma_tangle_or_golden_cross(data["price_df"], c2_tangle_pct, False, False)
            c34_res = data["c34"] if isinstance(data["c34"], dict) and data["c34"] else check_shareholding_distribution(pd.DataFrame(), 0, 0)
            c6_res = data.get("c6", {})
            c7_res = data.get("c7", {})
            inst_df_sid, price_df = inst_dict.get(sid, pd.DataFrame()), data["price_df"].copy()

            if not inst_df_sid.empty:
                inst_pivot = inst_df_sid.copy()
                inst_pivot['net'] = pd.to_numeric(inst_pivot['buy'], errors='coerce').fillna(0) - pd.to_numeric(inst_pivot['sell'], errors='coerce').fillna(0)
                foreign_mask = inst_pivot['name'].str.contains("Foreign_Investor|外資及陸資", case=False, na=False)
                trust_mask = inst_pivot['name'].str.contains("Investment_Trust|投信", case=False, na=False)
                foreign_daily = (inst_pivot[foreign_mask].groupby('date')['net'].sum() / 1000).round(0).rename("外資")
                trust_daily = (inst_pivot[trust_mask].groupby('date')['net'].sum() / 1000).round(0).rename("投信")
                if 'date' in price_df.columns:
                    price_df = price_df.set_index('date').join(foreign_daily).join(trust_daily).reset_index()

            consecutive_days, table_foreign_buy = 0, 0
            if "外資" in price_df.columns:
                table_foreign_buy = price_df["外資"].iloc[-1]
                if pd.isna(table_foreign_buy): table_foreign_buy = 0
                for v in reversed(price_df["外資"].fillna(0).values):
                    if v > 0: consecutive_days += 1
                    else: break

            if use_c5 and consecutive_days < c5_days:
                filtered_by_c5 += 1; continue

            try:
                latest_close, yesterday_close = float(price_df["close"].iloc[-1]), float(price_df["close"].iloc[-2])
                daily_pct = (latest_close - yesterday_close) / yesterday_close * 100
            except Exception:
                latest_close, daily_pct = 0.0, 0.0

            wma20 = c1_res.get("wma20", 0)
            bias_20w = ((latest_close - wma20) / wma20 * 100) if wma20 > 0 else 0

            chain_info = chain_dict.get(sid, {})
            chain_ind = chain_info.get("industry", "")
            chain_sub = chain_info.get("sub_category", "")

            if chain_sub:
                final_industry = f"{chain_ind} / {chain_sub}" if chain_ind else chain_sub
            else:
                final_industry = basic_industry_map.get(sid, "未知產業")

            results.append({
                "代號": sid, 
                "名稱": name_map.get(sid, ""), 
                "產業": final_industry,
                "收盤價": latest_close, 
                "今日漲跌 (%)": daily_pct, 
                "外資連買 (天)": consecutive_days, 
                "今日外資買超 (張)": int(table_foreign_buy),
                "大戶週變化 (人)": c34_res.get("whale_trend", 0), 
                "散戶週變化 (人)": c34_res.get("total_trend", 0), 
                "糾結度 (%)": c2_res.get("tangle_spread_pct", 0.0),
                "乖離 20WMA (%)": round(bias_20w, 2), 
                "KD_K": c6_res.get("k", 0.0),
                "KD_D": c6_res.get("d", 0.0),
                "KD交叉天前": c6_res.get("cross_days_ago", -1),
                "MACD_柱狀": c7_res.get("histogram", 0.0),
                "MACD_DIF": c7_res.get("dif", 0.0),
                "_price_df": price_df,
            })

        progress.progress(100); status.text(f"✅ 完成！{'條件⑤再過濾掉 ' + str(filtered_by_c5) + ' 檔。' if use_c5 and filtered_by_c5 > 0 else ''}")
        st.session_state.results = results; st.session_state.scan_done = True; st.session_state.selected_stock = None

    st.markdown("---")

    if st.session_state.scan_done:
        results = st.session_state.results
        if not results: st.warning("😕 全市場掃描完畢，今日無完全符合條件之標的。")
        else:
            stock_list_str = "、".join(f"{r['代號']} {r['名稱']}" for r in results)
            st.success(f"🎉 找到 **{len(results)}** 檔超級潛力股：{stock_list_str}")

            # ── 表頭 ──────────────────────────────────────────
            COL_W = [0.7, 2.2, 0.8, 0.8, 0.9, 1.0, 0.9, 0.9, 0.8, 0.9]
            if use_c6: COL_W.extend([0.7, 0.7, 0.7])
            if use_c7: COL_W.extend([0.8, 0.7])
            hdr = st.columns(COL_W)
            HEADERS = ["代號", "名稱 / 產業", "收盤價", "漲跌%", "外資連買", "外資買超(張)", "大戶週變化", "散戶週變化", "糾結度%", "乖離20W%"]
            if use_c6: HEADERS.extend(["KD(K)", "KD(D)", "交叉天前"])
            if use_c7: HEADERS.extend(["MACD柱狀", "DIF"])
            for col, h in zip(hdr, HEADERS):
                col.markdown(f"<div style='font-size:12px;color:#888;font-weight:600;padding:4px 0;border-bottom:2px solid #ddd;'>{h}</div>", unsafe_allow_html=True)

            # ── 每一列 ────────────────────────────────────────
            selected_sid = st.session_state.get("selected_stock", {})
            selected_sid = selected_sid.get("sid") if isinstance(selected_sid, dict) else None

            for r in results:
                sid, name, close, dpct = r["代號"], r["名稱"], r["收盤價"], r["今日漲跌 (%)"]
                industry = r.get("產業", "")
                dc   = "#c0392b" if dpct >= 0 else "#16a085"
                fnet = r["今日外資買超 (張)"]
                fc   = "#c0392b" if fnet >= 0 else "#16a085"
                whale  = r["大戶週變化 (人)"]
                wc   = "#c0392b" if whale > 0 else "#16a085" if whale < 0 else "#999"
                retail = r["散戶週變化 (人)"]
                rc   = "#16a085" if retail < 0 else "#c0392b" if retail > 0 else "#999"
                is_selected = (sid == selected_sid)

                row_bg = "background:#f0f7ff; border-radius:6px;" if is_selected else ""
                st.markdown(f"<div style='height:1px;background:#f0f0f0;margin:0;'></div>", unsafe_allow_html=True)
                cols = st.columns(COL_W)
                cols[0].markdown(f"<div style='color:#555;font-size:13px;padding:6px 0;{row_bg}'>{sid}</div>", unsafe_allow_html=True)

                # 名稱欄：按鈕
                btn_label = f"{'▼ ' if is_selected else ''}{name}"
                if cols[1].button(btn_label, key=f"btn_{sid}", use_container_width=True):
                    if is_selected:
                        st.session_state.selected_stock = None
                    else:
                        st.session_state.selected_stock = {"sid": sid, "name": name, "price_df": r["_price_df"]}
                    st.rerun()

                cols[1].markdown(f"<div style='font-size:11px;color:#aaa;margin-top:-8px;padding-left:4px;'>{industry}</div>", unsafe_allow_html=True)
                cols[2].markdown(f"<div style='font-size:13px;padding:6px 0;'>{close:.2f}</div>", unsafe_allow_html=True)
                cols[3].markdown(f"<div style='color:{dc};font-weight:bold;font-size:13px;padding:6px 0;'>{dpct:+.2f}%</div>", unsafe_allow_html=True)
                cols[4].markdown(f"<div style='font-size:13px;padding:6px 0;'>{int(r['外資連買 (天)'])} 天</div>", unsafe_allow_html=True)
                cols[5].markdown(f"<div style='color:{fc};font-size:13px;padding:6px 0;'>{int(fnet):,}</div>", unsafe_allow_html=True)
                cols[6].markdown(f"<div style='color:{wc};font-size:13px;padding:6px 0;'>{int(whale):+,}</div>", unsafe_allow_html=True)
                cols[7].markdown(f"<div style='color:{rc};font-size:13px;padding:6px 0;'>{int(retail):+,}</div>", unsafe_allow_html=True)
                cols[8].markdown(f"<div style='color:#666;font-size:13px;padding:6px 0;'>{r['糾結度 (%)']:.2f}%</div>", unsafe_allow_html=True)
                cols[9].markdown(f"<div style='color:#666;font-size:13px;padding:6px 0;'>{r['乖離 20WMA (%)']:+.2f}%</div>", unsafe_allow_html=True)
                if use_c6:
                    kd_k = r.get("KD_K", 0.0); kd_d = r.get("KD_D", 0.0)
                    kd_c = "#c0392b" if kd_k > kd_d else "#16a085"
                    cross_ago = r.get("KD交叉天前", -1)
                    cross_txt = "今日" if cross_ago == 0 else (f"{cross_ago}天前" if cross_ago >= 0 else "-")
                    cols[10].markdown(f"<div style='color:{kd_c};font-weight:bold;font-size:13px;padding:6px 0;'>{kd_k:.1f}</div>", unsafe_allow_html=True)
                    cols[11].markdown(f"<div style='color:{kd_c};font-size:13px;padding:6px 0;'>{kd_d:.1f}</div>", unsafe_allow_html=True)
                    cols[12].markdown(f"<div style='color:#888;font-size:13px;padding:6px 0;'>{cross_txt}</div>", unsafe_allow_html=True)
                if use_c7:
                    hist = r.get("MACD_柱狀", 0.0); dif = r.get("MACD_DIF", 0.0)
                    ci = 10 + (3 if use_c6 else 0)
                    hist_c = "#c0392b" if hist >= 0 else "#16a085"
                    dif_c  = "#c0392b" if dif  >= 0 else "#16a085"
                    cols[ci].markdown(f"<div style='color:{hist_c};font-weight:bold;font-size:13px;padding:6px 0;'>{hist:.3f}</div>", unsafe_allow_html=True)
                    cols[ci+1].markdown(f"<div style='color:{dif_c};font-size:13px;padding:6px 0;'>{dif:.3f}</div>", unsafe_allow_html=True)

                # ── K 線圖（展開在名稱正下方）─────────────────
                if is_selected:
                    price_df = r["_price_df"]
                    st.markdown(f"<div style='font-size:16px;font-weight:700;color:#1a1a1a;border-left:4px solid #e74c3c;padding-left:10px;margin:12px 0 6px 0;'>📊 {sid} {name} · K 線圖</div>", unsafe_allow_html=True)
                    if price_df is None or price_df.empty:
                        st.warning("⚠️ 此股票無行情資料。")
                    else:
                        df_chart = price_df.copy().rename(columns={"max": "high", "min": "low", "Trading_Volume": "volume"})
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
                        except Exception as e:
                            st.error(f"❌ 繪圖失敗：{e}")
    else:
        st.info("👈 請在左側設定條件後，點擊「開始全市場極速掃描」。")

    st.markdown("---")

# ──────────────────────────────────────────────────────────
# Tab 2：券商分點查詢（獨立模組）
# ──────────────────────────────────────────────────────────
with tab_branch:
    render_broker_page()
