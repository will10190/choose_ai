"""
broker_page.py
券商分點查詢頁面（獨立模組）

功能：
- 搜尋券商分點
- 顯示最新買賣超明細
- 🆕 列出「近 5 天買超 2 天以上，且最新一天為買超」的股票
"""

import streamlit as st
import pandas as pd
from broker_scraper import (
    fetch_broker_list,
    lookup_branch_code,
    get_branch_data_cached,
    fetch_branch_buy_list,
    get_branch_multi_day_cached,
)

def _calc_buy_days(multi_df: pd.DataFrame) -> pd.DataFrame:
    """
    根據多日資料，計算每支股票在所有日期中買超的天數（不要求連續）。
    [Update] 增加條件：最新一天必須是買超 (>0)，且總買超天數 >= 2。
    回傳 DataFrame：股票代號、股票名稱、買超天數、最新日買賣超、最新資料日期
    """
    if multi_df.empty:
        return pd.DataFrame()

    multi_df = multi_df.copy()
    multi_df["資料日期"] = pd.to_datetime(multi_df["資料日期"], format="%Y/%m/%d", errors="coerce")
    multi_df = multi_df.sort_values("資料日期")

    dates_sorted = sorted(multi_df["資料日期"].dropna().unique())
    latest_date = dates_sorted[-1] if dates_sorted else None

    results = []
    for sid, grp in multi_df.groupby("股票代號"):
        grp = grp.sort_values("資料日期").drop_duplicates("資料日期")
        daily_net = dict(zip(grp["資料日期"], grp["買賣超"]))
        stock_name = grp["股票名稱"].iloc[-1]

        # 計算有出現在資料中且買超 > 0 的天數
        buy_days = sum(1 for d in dates_sorted if daily_net.get(d, 0) > 0)

        latest_net = daily_net.get(latest_date, 0) if latest_date else 0
        results.append({
            "股票代號": sid,
            "股票名稱": stock_name,
            "買超天數": buy_days,
            "最新日買賣超(張)": int(latest_net),
            "最新資料日期": latest_date.strftime("%Y/%m/%d") if latest_date else "-",
        })

    df_result = pd.DataFrame(results)
    if df_result.empty:
        return df_result

    # 🌟 關鍵修改：只保留買超天數 >= 2 且「最新一天必須是買超」
    df_result = df_result[(df_result["買超天數"] >= 2) & (df_result["最新日買賣超(張)"] > 0)]
    
    df_result = df_result.sort_values("買超天數", ascending=False).reset_index(drop=True)
    return df_result


# ──────────────────────────────────────────────────────────
# 主渲染函數（供 app.py 呼叫）
# ──────────────────────────────────────────────────────────

def render_broker_page():
    st.header("🏦 券商分點查詢")
    st.caption("資料來源：富邦 DJ 資訊網（不需要 FinMind Token，每 30 分鐘快取一次）")

    # ── 載入分點清單 ──────────────────────────────────────
    col_status, col_reload = st.columns([4, 1])
    with col_reload:
        if st.button("🔄 重新載入清單", key="reload_broker"):
            st.cache_data.clear()
            st.rerun()

    broker_map = None
    try:
        with st.spinner("載入分點清單中..."):
            broker_map = fetch_broker_list()
        col_status.success(f"✅ 已載入 {len(broker_map):,} 個券商分點")
    except Exception as e:
        col_status.error(f"❌ 分點清單載入失敗：{e}")

    st.markdown("---")
    if not broker_map:
        return

    # ── 熱門分點快捷鍵 ────────────────────────────────────
    st.markdown("##### 💡 熱門分點快速選擇")
    popular = [("富邦仁愛", "9676"), ("國泰敦南", "8888")]
    cols = st.columns(len(popular))
    for i, (bname, bcode) in enumerate(popular):
        with cols[i]:
            if st.button(f"🔥 {bname}", key=f"bp_pop_{i}", use_container_width=True):
                st.session_state["bp_keyword"]    = ""
                st.session_state["bp_candidates"] = lookup_branch_code(bcode, broker_map)
                st.session_state["bp_selected"]   = None
                st.session_state["bp_result_df"]  = None
                st.session_state["bp_multi_df"]   = None
                st.rerun()

    st.markdown("---")

    # ── 搜尋輸入 ──────────────────────────────────────────
    col_input, col_search = st.columns([4, 1])
    with col_input:
        branch_keyword = st.text_input(
            "輸入券商分點名稱或代碼（支援忽略 - 連字號，例如：國泰敦南、8888）",
            key="bp_keyword",
            placeholder="例如：富邦仁愛、國泰敦南、8888...",
        )
    with col_search:
        st.markdown("<br>", unsafe_allow_html=True)
        search_btn = st.button("🔍 搜尋", type="primary", key="bp_search_btn")

    if search_btn and branch_keyword:
        st.session_state["bp_candidates"] = lookup_branch_code(branch_keyword, broker_map)
        st.session_state["bp_selected"]   = None
        st.session_state["bp_result_df"]  = None
        st.session_state["bp_multi_df"]   = None

    candidates = st.session_state.get("bp_candidates", [])

    if candidates:
        if len(candidates) == 1:
            if st.session_state.get("bp_selected") != candidates[0]:
                st.session_state["bp_selected"]  = candidates[0]
                st.session_state["bp_result_df"] = None
                st.session_state["bp_multi_df"]  = None
                st.rerun()
            st.info(
                f"✅ 自動對應：**{candidates[0]['name']}**"
                f"（代碼：a={candidates[0]['a']}, b={candidates[0]['b']}）"
            )
        else:
            names = [f"{c['name']} (代碼: {c['b']})" for c in candidates]
            selected_name = st.selectbox("找到多個符合的分點，請選擇：", names, key="bp_selector")
            idx = names.index(selected_name)
            if st.session_state.get("bp_selected") != candidates[idx]:
                st.session_state["bp_selected"]  = candidates[idx]
                st.session_state["bp_result_df"] = None
                st.session_state["bp_multi_df"]  = None
                st.rerun()
    elif search_btn and branch_keyword:
        st.warning(f"😔 找不到包含「{branch_keyword}」的分點，請確認名稱或代碼是否正確。")

    selected_branch = st.session_state.get("bp_selected")
    if not selected_branch:
        return

    st.markdown("---")
    st.markdown(f"### 📌 {selected_branch['name']} — 查詢結果")

    col_r1, col_r2 = st.columns([3, 1])
    with col_r2:
        if st.button("🔄 重新整理", key="bp_refresh"):
            st.session_state["bp_result_df"] = None
            st.session_state["bp_multi_df"]  = None
            st.cache_data.clear()
            st.rerun()

    a_code = selected_branch["a"]
    b_code = selected_branch["b"]
    branch_name = selected_branch["name"]

    # ── 自動抓取多日資料（固定 5 天，無需手動觸發）─────────
    multi_df = st.session_state.get("bp_multi_df")
    if multi_df is None:
        with st.spinner(f"抓取 {branch_name} 近 5 個交易日資料中⋯"):
            try:
                multi_df = get_branch_multi_day_cached(branch_name, a_code, b_code, 5)
                st.session_state["bp_multi_df"] = multi_df
            except Exception as e:
                st.error(f"❌ 多日資料抓取失敗：{e}")
                multi_df = pd.DataFrame()

    # ══════════════════════════════════════════════════════
    # 區塊 1：連買兩天以上（置頂顯示）
    # ══════════════════════════════════════════════════════
    st.markdown("#### 🔥 近 5 個交易日買超 2 天以上 (且最新日為買超)")

    if multi_df is not None and not multi_df.empty:
        buy_df = _calc_buy_days(multi_df)
        if buy_df.empty:
            st.info("😔 近 5 個交易日內，該分點無買超 ≥ 2 天且最新日買超的股票。")
        else:
            st.success(f"找到 **{len(buy_df)}** 支符合條件的股票")

            def _highlight_days(val):
                if val >= 5:
                    return "background-color: #ffd6d6; color: #c0392b; font-weight: bold;"
                elif val >= 3:
                    return "background-color: #fff3cd; color: #856404; font-weight: bold;"
                return ""

            styled = (
                buy_df.style
                .applymap(_highlight_days, subset=["買超天數"])
                .format({"最新日買賣超(張)": "{:,}"})
            )
            st.dataframe(styled, use_container_width=True, hide_index=True)

            csv_consec = buy_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            latest_date_str = buy_df["最新資料日期"].iloc[0].replace("/", "") if not buy_df.empty else ""
            st.download_button(
                "⬇️ 下載買超清單 CSV",
                data=csv_consec,
                file_name=f"{branch_name}_連買清單_{latest_date_str}.csv",
                mime="text/csv",
                key="bp_consec_csv",
            )
    else:
        st.warning("⚠️ 未能取得多日資料，請按右上角「🔄 重新整理」重試。")

    st.markdown("---")

    # ══════════════════════════════════════════════════════
    # 區塊 2：今日完整買賣超明細
    # ══════════════════════════════════════════════════════
    st.markdown("#### 📋 今日完整買賣超明細")

    # 今日資料直接從 multi_df 的最新日取，省一次 request
    result_df = st.session_state.get("bp_result_df")
    if result_df is None:
        try:
            with st.spinner(f"抓取 {branch_name} 今日明細中..."):
                result_df = get_branch_data_cached(branch_name, a_code, b_code)
            st.session_state["bp_result_df"] = result_df
        except Exception as e:
            st.error(f"❌ 抓取失敗：{e}")
            result_df = None

    if result_df is not None:
        if result_df.empty:
            st.info("📭 該分點目前無最新的買賣超記錄。")
        else:
            data_date = result_df["資料日期"].iloc[0] if "資料日期" in result_df.columns else "N/A"
            m1, m2, m3 = st.columns(3)
            m1.metric("資料日期", data_date)
            m2.metric("進出股票數", f"{len(result_df)} 支")
            try:
                total_net = pd.to_numeric(result_df["買賣超"], errors="coerce").sum()
                m3.metric("合計買賣超 (張)", f"{int(total_net):,}")
            except Exception:
                m3.metric("合計買賣超 (張)", "N/A")

            display_df = result_df.copy()
            for col in ["買張", "賣張", "買賣超"]:
                if col in display_df.columns:
                    display_df[col] = pd.to_numeric(display_df[col], errors="coerce").fillna(0).astype(int)

            display_df["股票"] = display_df["股票代號"].astype(str) + " " + display_df["股票名稱"]
            display_df = display_df.rename(columns={"買張": "買進張數", "賣張": "賣出張數", "買賣超": "差額"})
            df_buy  = display_df[display_df["差額"] > 0].sort_values("差額", ascending=False).reset_index(drop=True)
            df_sell = display_df[display_df["差額"] < 0].sort_values("差額").reset_index(drop=True)

            col_buy, col_sell = st.columns(2)
            dynamic_height = max(max(len(df_buy), len(df_sell)) * 36 + 43, 200)

            with col_buy:
                st.markdown("<div style='color:#ef5350; font-weight:bold; font-size:15px;'>📈 買超</div>", unsafe_allow_html=True)
                st.dataframe(df_buy[["股票", "買進張數", "賣出張數", "差額"]], use_container_width=True, hide_index=True, height=dynamic_height)

            with col_sell:
                st.markdown("<div style='color:#26a69a; font-weight:bold; font-size:15px;'>📉 賣超</div>", unsafe_allow_html=True)
                st.dataframe(df_sell[["股票", "買進張數", "賣出張數", "差額"]], use_container_width=True, hide_index=True, height=dynamic_height)

            st.markdown("<br>", unsafe_allow_html=True)
            csv_bytes = result_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button(
                "⬇️ 下載完整 CSV",
                data=csv_bytes,
                file_name=f"{branch_name}_買賣超_{data_date.replace('/','')}.csv",
                mime="text/csv",
                key="bp_dl_csv",
            )
