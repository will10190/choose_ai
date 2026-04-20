"""
broker_page.py
券商分點查詢頁面（獨立模組）
[Update] 增加條件：最新一天必須是買超，且修復新版 Pandas 的樣式函數錯誤
"""

import streamlit as st
import pandas as pd
from broker_scraper import (
    fetch_broker_list,
    lookup_branch_code,
    get_branch_data_cached,
    get_branch_multi_day_cached,
)

def _calc_buy_days(multi_df: pd.DataFrame) -> pd.DataFrame:
    """
    根據多日資料，計算買超天數。
    條件：最新一天必須是買超 (>0)，且 5 天內買超天數 >= 2。
    """
    if multi_df.empty: return pd.DataFrame()
    multi_df = multi_df.copy()
    multi_df["資料日期"] = pd.to_datetime(multi_df["資料日期"], format="%Y/%m/%d", errors="coerce")
    multi_df = multi_df.sort_values("資料日期")
    dates_sorted = sorted(multi_df["資料日期"].dropna().unique())
    latest_date = dates_sorted[-1] if dates_sorted else None

    results = []
    for sid, grp in multi_df.groupby("股票代號"):
        grp = grp.sort_values("資料日期").drop_duplicates("資料日期")
        daily_net = dict(zip(grp["資料日期"], grp["買賣超"]))
        buy_days = sum(1 for d in dates_sorted if daily_net.get(d, 0) > 0)
        latest_net = daily_net.get(latest_date, 0) if latest_date else 0
        results.append({
            "股票代號": sid, "股票名稱": grp["股票名稱"].iloc[-1], "買超天數": buy_days,
            "最新日買賣超(張)": int(latest_net), "最新資料日期": latest_date.strftime("%Y/%m/%d") if latest_date else "-"
        })

    df_result = pd.DataFrame(results)
    if df_result.empty: return df_result
    # 🌟 核心過濾：天數 >= 2 且 最新日 > 0
    df_result = df_result[(df_result["買超天數"] >= 2) & (df_result["最新日買賣超(張)"] > 0)]
    return df_result.sort_values("買超天數", ascending=False).reset_index(drop=True)

def render_broker_page():
    st.header("🏦 券商分點查詢")
    st.caption("資料來源：富邦 DJ 資訊網（不需要 Token，每 30 分鐘快取一次）")

    col_status, col_reload = st.columns([4, 1])
    with col_reload:
        if st.button("🔄 重新載入清單", key="reload_broker"):
            st.cache_data.clear(); st.rerun()

    broker_map = None
    try:
        with st.spinner("載入分點清單中..."):
            broker_map = fetch_broker_list()
        col_status.success(f"✅ 已載入 {len(broker_map):,} 個券商分點")
    except Exception as e:
        col_status.error(f"❌ 分點清單載入失敗：{e}"); return

    st.markdown("---")
    st.markdown("##### 💡 熱門分點快速選擇")
    popular = [("富邦仁愛", "9676"), ("國泰敦南", "8888")]
    cols = st.columns(len(popular))
    for i, (bname, bcode) in enumerate(popular):
        with cols[i]:
            if st.button(f"🔥 {bname}", key=f"bp_pop_{i}", use_container_width=True):
                st.session_state["bp_keyword"] = ""; st.session_state["bp_candidates"] = lookup_branch_code(bcode, broker_map)
                st.session_state["bp_selected"] = None; st.session_state["bp_result_df"] = None; st.session_state["bp_multi_df"] = None; st.rerun()

    col_input, col_search = st.columns([4, 1])
    with col_input:
        branch_keyword = st.text_input("輸入名稱或代碼", key="bp_keyword", placeholder="例如：富邦仁愛、8888...")
    with col_search:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔍 搜尋", type="primary", key="bp_search_btn"):
            st.session_state["bp_candidates"] = lookup_branch_code(branch_keyword, broker_map)
            st.session_state["bp_selected"] = None; st.session_state["bp_result_df"] = None; st.session_state["bp_multi_df"] = None

    candidates = st.session_state.get("bp_candidates", [])
    if candidates:
        if len(candidates) == 1:
            if st.session_state.get("bp_selected") != candidates[0]:
                st.session_state["bp_selected"] = candidates[0]; st.session_state["bp_result_df"] = None; st.session_state["bp_multi_df"] = None; st.rerun()
        else:
            names = [f"{c['name']} (代碼: {c['b']})" for c in candidates]
            sel_name = st.selectbox("請選擇分點：", names, key="bp_selector")
            idx = names.index(sel_name)
            if st.session_state.get("bp_selected") != candidates[idx]:
                st.session_state["bp_selected"] = candidates[idx]; st.session_state["bp_result_df"] = None; st.session_state["bp_multi_df"] = None; st.rerun()

    selected_branch = st.session_state.get("bp_selected")
    if not selected_branch: return

    st.markdown(f"### 📌 {selected_branch['name']} — 查詢結果")
    if st.button("🔄 重新整理", key="bp_refresh"):
        st.session_state["bp_result_df"] = None; st.session_state["bp_multi_df"] = None; st.cache_data.clear(); st.rerun()

    a_code, b_code, branch_name = selected_branch["a"], selected_branch["b"], selected_branch["name"]
    multi_df = st.session_state.get("bp_multi_df")
    if multi_df is None:
        with st.spinner(f"抓取 {branch_name} 近 5 個交易日資料中⋯"):
            try:
                multi_df = get_branch_multi_day_cached(branch_name, a_code, b_code, 5)
                st.session_state["bp_multi_df"] = multi_df
            except Exception: multi_df = pd.DataFrame()

    st.markdown("#### 🔥 近 5 個交易日買超 2 天以上 (且最新日為買超)")
    if multi_df is not None and not multi_df.empty:
        buy_df = _calc_buy_days(multi_df)
        if buy_df.empty: st.info("😔 目前無符合條件之標的。")
        else:
            def _style(val):
                if val >= 5: return "background-color: #ffd6d6; color: #c0392b; font-weight: bold;"
                elif val >= 3: return "background-color: #fff3cd; color: #856404; font-weight: bold;"
                return ""
            # 🌟 這裡使用 .map 代替 .applymap
            styled = buy_df.style.map(_style, subset=["買超天數"]).format({"最新日買賣超(張)": "{:,}"})
            st.dataframe(styled, use_container_width=True, hide_index=True)
            csv = buy_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button("⬇️ 下載清單 CSV", data=csv, file_name=f"{branch_name}_買超.csv", mime="text/csv")
    else: st.warning("⚠️ 未能取得多日資料。")

    st.markdown("---")
    st.markdown("#### 📋 今日完整買賣超明細")
    res_df = st.session_state.get("bp_result_df")
    if res_df is None:
        try:
            with st.spinner("抓取明細中..."): res_df = get_branch_data_cached(branch_name, a_code, b_code)
            st.session_state["bp_result_df"] = res_df
        except Exception: res_df = None

    if res_df is not None and not res_df.empty:
        data_date = res_df["資料日期"].iloc[0]
        m1, m2, m3 = st.columns(3)
        m1.metric("資料日期", data_date); m2.metric("標的數", f"{len(res_df)} 支")
        try: m3.metric("合計買賣超", f"{int(pd.to_numeric(res_df['買賣超'], errors='coerce').sum()):,} 張")
        except: pass
        disp = res_df.copy(); disp["股票"] = disp["股票代號"] + " " + disp["股票名稱"]
        disp = disp.rename(columns={"買張": "買進", "賣張": "賣出", "買賣超": "差額"})
        df_buy = disp[disp["差額"] > 0].sort_values("差額", ascending=False); df_sell = disp[disp["差額"] < 0].sort_values("差額")
        cb, cs = st.columns(2)
        with cb: st.markdown("<b style='color:#ef5350;'>📈 買超</b>", unsafe_allow_html=True); st.dataframe(df_buy[["股票", "買進", "賣出", "差額"]], use_container_width=True, hide_index=True)
        with cs: st.markdown("<b style='color:#26a69a;'>📉 賣超</b>", unsafe_allow_html=True); st.dataframe(df_sell[["股票", "買進", "賣出", "差額"]], use_container_width=True, hide_index=True)
    elif res_df is not None: st.info("📭 查無今日明細。")
