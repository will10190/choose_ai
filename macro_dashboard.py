import streamlit as st
import pandas as pd
import requests
import math
import plotly.graph_objects as go
from datetime import datetime, timedelta

# ══════════════════════════════════════════════════════════
# API 獲取函數 (真實串接版 + 雙重 Token 保險 + 最強防呆過濾)
# ══════════════════════════════════════════════════════════
@st.cache_data(ttl=3600) 
def fetch_fear_greed_data(token: str):
    """獲取 恐懼與貪婪指數 (真實 API)"""
    default_data = {
        "current": {"date": "-", "score": 50},
        "prev": {"date": "-", "score": 50},
        "week": {"date": "-", "score": 50},
        "month": {"date": "-", "score": 50},
        "year": {"date": "-", "score": 50}
    }
    if not token: return default_data

    start_date = (datetime.today() - timedelta(days=370)).strftime("%Y-%m-%d")
    end_date = datetime.today().strftime("%Y-%m-%d")
    url = "https://api.finmindtrade.com/api/v4/data"
    
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "dataset": "CnnFearGreedIndex", 
        "start_date": start_date, 
        "end_date": end_date,
        "token": token
    }
    
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                df = pd.DataFrame(data)
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date").reset_index(drop=True)
                score_col = "fear_greed" if "fear_greed" in df.columns else "value"
                if score_col in df.columns:
                    latest_date = df.iloc[-1]["date"]
                    def get_closest(target_date):
                        diffs = abs(df["date"] - target_date)
                        idx = diffs.idxmin()
                        return {"date": df.iloc[idx]["date"].strftime("%Y/%m/%d"), "score": int(df.iloc[idx][score_col])}
                    return {
                        "current": {"date": latest_date.strftime("%Y/%m/%d"), "score": int(df.iloc[-1][score_col])},
                        "prev": {"date": df.iloc[-2]["date"].strftime("%Y/%m/%d") if len(df)>1 else "-", "score": int(df.iloc[-2][score_col]) if len(df)>1 else 50},
                        "week": get_closest(latest_date - timedelta(days=7)),
                        "month": get_closest(latest_date - timedelta(days=30)),
                        "year": get_closest(latest_date - timedelta(days=365))
                    }
    except Exception as e:
        print(f"🚨 [恐懼貪婪指數] API 取得失敗: {e}")
    return default_data

@st.cache_data(ttl=86400)
def fetch_taiwan_business_indicator(token: str):
    """
    獲取台灣景氣對策信號
    FinMind schema (wide format，每列一個月份):
    date | leading | leading_notrend | coincident | coincident_notrend |
    lagging | lagging_notrend | monitoring | monitoring_color
    → 直接取 monitoring 欄（燈號分數）與 monitoring_color（燈色）
    """
    if not token:
        return pd.DataFrame(), "尚未設定 API Token"

    start_date = (datetime.today() - timedelta(days=730)).strftime("%Y-%m-%d")
    end_date = datetime.today().strftime("%Y-%m-%d")

    try:
        resp = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            headers={"Authorization": f"Bearer {token}"},
            params={"dataset": "TaiwanBusinessIndicator", "start_date": start_date, "end_date": end_date, "token": token},
            timeout=15
        )
        if resp.status_code != 200:
            return pd.DataFrame(), f"API 錯誤 {resp.status_code}: {resp.text}"

        data = resp.json().get("data", [])
        if not data:
            return pd.DataFrame(), f"API 回傳空資料：{resp.json().get('msg', '')}"

        df = pd.DataFrame(data)

        # wide format：直接取 monitoring（分數）與 monitoring_color（燈色）
        if "monitoring" not in df.columns:
            # 備案：萬一欄位名稱有變，找最接近的數字欄
            return pd.DataFrame(), f"找不到 monitoring 欄位，實際欄位：{list(df.columns)}"

        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y/%m")
        df["score"] = pd.to_numeric(df["monitoring"], errors="coerce")
        df["color_name"] = df.get("monitoring_color", "")
        df = df.dropna(subset=["score"])
        df = df.drop_duplicates(subset=["date"], keep="last")
        return df.sort_values("date").tail(12).reset_index(drop=True)[["date", "score", "color_name"]], ""

    except Exception as e:
        return pd.DataFrame(), f"網路連線異常: {e}"

# ══════════════════════════════════════════════════════════
# UI 輔助函數
# ══════════════════════════════════════════════════════════
def get_biz_indicator_color(score):
    if score >= 38: return "#ff0000" # 紅燈 
    elif score >= 32: return "#ff9900" # 黃紅燈
    elif score >= 23: return "#00ff00" # 綠燈
    elif score >= 17: return "#ffea00" # 黃藍燈
    else: return "#0000ff" # 藍燈

def get_fg_style(score):
    if score <= 25: return "極度恐懼", "#fca5a5", "#c0392b"
    elif score <= 45: return "恐懼", "#ffc9c9", "#c0392b"
    elif score <= 55: return "中立", "#e9ecef", "#666666"
    elif score <= 75: return "貪婪", "#bcf0ce", "#16a085"
    else: return "極度貪婪", "#8ae3ab", "#16a085"

# ══════════════════════════════════════════════════════════
# 主渲染函數
# ══════════════════════════════════════════════════════════
def render_macro_dashboard(token: str):
    st.markdown("<div style='background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); margin-bottom: 20px;'>", unsafe_allow_html=True)
    st.markdown("<h4 style='margin-top: 0; color: #333;'>🌍 總經與市場情緒儀表板</h4>", unsafe_allow_html=True)

    col_left, col_right = st.columns(2)

    # ────────────── 左半部：恐懼與貪婪指數 ──────────────
    with col_left:
        st.markdown("##### 恐懼與貪婪指數")
        
        fg_data = fetch_fear_greed_data(token)
        current_score = fg_data["current"]["score"]
        
        fig_gauge = go.Figure()
        fig_gauge.add_trace(go.Pie(
            values=[100, 25, 20, 10, 20, 25], 
            labels=['', '極度恐懼<br>EXTREME FEAR', '恐懼<br>FEAR', '中立<br>NEUTRAL', '貪婪<br>GREED', '極度貪婪<br>EXTREME GREED'],
            marker=dict(colors=['rgba(0,0,0,0)', '#fca5a5', '#ffc9c9', '#e9ecef', '#bcf0ce', '#8ae3ab']),
            textinfo='label', textposition='inside', insidetextorientation='horizontal',
            hole=0.45, rotation=90, direction='clockwise', sort=False, showlegend=False, hoverinfo='none',
            textfont=dict(size=11, color='#444', family='sans-serif'), domain=dict(x=[0, 1], y=[0, 1])
        ))
        
        theta = math.radians(180 - (current_score / 100.0) * 180)
        head_x = 0.5 + 0.35 * math.cos(theta)
        head_y = 0.5 + 0.35 * math.sin(theta)
        
        fig_gauge.update_layout(
            shapes=[
                dict(
                    type="line", xref="paper", yref="paper", x0=0.5, y0=0.5, x1=head_x, y1=head_y,
                    line=dict(color="#2c3e50", width=5)
                )
            ],
            annotations=[
                dict(
                    x=0.5, y=0.5, xref="paper", yref="paper", text=f"<b>{current_score}</b>", 
                    showarrow=False, font=dict(size=36, color="#1a1a1a"), 
                    bgcolor="white", bordercolor="#eee", borderwidth=2, borderpad=10
                )
            ],
            margin=dict(t=10, b=0, l=10, r=10), height=320,
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
        )
        
        st.plotly_chart(fig_gauge, use_container_width=True)
        # 顯示資料日期
        current_date = fg_data["current"]["date"]
        current_name, current_bg, current_text = get_fg_style(current_score)
        st.markdown(
            f"<div style='text-align:center; font-size:13px; color:#888; margin-top:-10px;'>"
            f"資料日期：{current_date} &nbsp;|&nbsp; "
            f"<span style='background:{current_bg}; color:{current_text}; padding:2px 10px; border-radius:4px; font-weight:bold;'>{current_name} {current_score}</span>"
            f"</div>",
            unsafe_allow_html=True
        )

    # ────────────── 右半部：台灣景氣燈號 ──────────────
    with col_right:
        st.markdown("##### 台灣景氣對策信號 (景氣燈號)")
        
        tw_biz_df, error_msg = fetch_taiwan_business_indicator(token)
        
        if tw_biz_df.empty:
            st.error(f"無法載入景氣燈號資料：{error_msg}")
        else:
            # 用 monitoring_color 欄位決定燈色（FinMind 回傳的顏色字串）
            COLOR_MAP = {
                "red": "#e53935",
                "yellow_red": "#ff9800",
                "green": "#43a047",
                "yellow_blue": "#fdd835",
                "blue": "#1e88e5",
                # 中文備案
                "紅燈": "#e53935", "黃紅燈": "#ff9800", "綠燈": "#43a047",
                "黃藍燈": "#fdd835", "藍燈": "#1e88e5",
            }
            def _get_color(row):
                c = str(row.get("color_name", "")).strip().lower()
                if c in COLOR_MAP:
                    return COLOR_MAP[c]
                # 備案：用分數換算
                return get_biz_indicator_color(row["score"])

            colors = [_get_color(row) for _, row in tw_biz_df.iterrows()]

            fig_biz = go.Figure()
            fig_biz.add_trace(go.Scatter(
                x=tw_biz_df["date"], y=tw_biz_df["score"],
                mode='lines+markers+text',
                line=dict(color='#90caf9', width=2),
                marker=dict(size=22, color=colors, line=dict(width=2, color='white')),
                text=tw_biz_df["score"].astype(int).astype(str),
                textposition="top center",
                textfont=dict(size=12, color="#333", family="Arial Black"),
                hovertemplate="日期: %{x}<br>分數: %{y}<extra></extra>"
            ))
            # 畫燈號分界線
            for y_val, label in [(38, "紅燈"), (32, "黃紅燈"), (23, "綠燈"), (17, "黃藍燈")]:
                fig_biz.add_hline(y=y_val, line_dash="dot", line_color="#bbb", line_width=1,
                                  annotation_text=label, annotation_position="left",
                                  annotation_font_size=10, annotation_font_color="#888")
            fig_biz.update_layout(
                height=320, margin=dict(t=10, b=20, l=60, r=20),
                xaxis=dict(showgrid=False, tickangle=-45),
                yaxis=dict(range=[5, 48], showgrid=False),
                plot_bgcolor='white', hovermode="x unified"
            )
            st.plotly_chart(fig_biz, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)
