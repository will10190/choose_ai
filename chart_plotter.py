import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import streamlit as st

def _get_gp_range(df_quarterly, pad_abs=8.0, pad_ratio=0.20, pad_cap=18.0, min_span=16.0):
    try:
        s = pd.to_numeric(df_quarterly.get('毛利率'), errors='coerce').dropna()
        if s.empty: return None
        vmin, vmax = float(s.min()), float(s.max())
        span = vmax - vmin
        pad = min(max(pad_abs, span * pad_ratio), pad_cap)
        rmin, rmax = max(-100.0, vmin - pad), min(100.0, vmax + pad)
        if (rmax - rmin) < min_span:
            mid = (rmax + rmin) / 2.0
            rmin, rmax = max(-100.0, mid - min_span/2.0), min(100.0, mid + min_span/2.0)
        return [rmin, rmax]
    except Exception: return None

def _get_revenue_range(revenue_series, pad_ratio=0.15):
    try:
        values = (revenue_series / 1000).round(0)
        vmin, vmax = float(values.min()), float(values.max())
        if vmin < 0:
            span = vmax - vmin
            pad = span * pad_ratio
            rmin, rmax = vmin - pad, vmax + pad
        else:
            rmin, rmax = 0, vmax * (1 + pad_ratio)
        return [rmin, rmax]
    except Exception: return None

def _get_yoy_range(df_revenue, pad_abs=8.0, pad_ratio=0.25, pad_cap=20.0, min_span=40.0):
    try:
        col = '年增率' if '年增率' in df_revenue.columns else ('yoy' if 'yoy' in df_revenue.columns else None)
        if col is None: return None
        s = pd.to_numeric(df_revenue[col], errors='coerce').dropna()
        if s.empty: return None
        vmin, vmax = float(s.min()), float(s.max())
        span = vmax - vmin
        pad = min(max(pad_abs, span * pad_ratio), pad_cap)
        rmin, rmax = min(vmin - pad, -5.0), max(vmax + pad, 5.0)
        if (rmax - rmin) < min_span:
            mid = (rmax + rmin) / 2.0
            rmin, rmax = mid - min_span/2.0, mid + min_span/2.0
        return [rmin, rmax]
    except Exception: return None


def plot_combined_chart(df, stock_id, stock_name, show_ma_dict, k_line_type="一般K線"):
    """
    四子圖完整版：K線、成交量、外資、投信 (徹底清除第五張圖的殘留)
    """
    df = df.copy()
    df = df.sort_values('date').reset_index(drop=True)
    chart_revision = f"{stock_id}"
    
    # 嚴格定義只有 4 列，並且標題陣列只有 4 個
    fig = make_subplots(
        rows=4, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.08, 
        row_heights=[0.55, 0.15, 0.15, 0.15],
        specs=[
            [{"secondary_y": False}], 
            [{"secondary_y": False}], 
            [{"secondary_y": False}], 
            [{"secondary_y": False}]
        ],
        subplot_titles=(
            f"{stock_id} {stock_name} 股價走勢 ({k_line_type})", 
            "成交量 (張)", 
            "外資買賣超 (張)", 
            "投信買賣超 (張)"
        )
    )

    # 1. K線
    fig.add_trace(go.Candlestick(
        x=df['date'], open=df['open'], high=df['high'], low=df['low'], close=df['close'],
        name='K線', increasing_line_color='#ef5350', decreasing_line_color='#26a69a', showlegend=False
    ), row=1, col=1)

    ma_colors = {'MA5': '#FFD700', 'MA20': '#FF69B4', 'MA60': '#9370DB', 'MA100': '#00CED1', 'MA120': '#FFA500', 'MA240': '#FF4500'}
    for ma_name, show in show_ma_dict.items():
        if show and ma_name in df.columns:
            valid_ma = df[df[ma_name] > 0]
            if not valid_ma.empty:
                fig.add_trace(go.Scatter(x=valid_ma['date'], y=valid_ma[ma_name], name=ma_name, line=dict(color=ma_colors.get(ma_name, 'white'), width=1.5), showlegend=True), row=1, col=1)

    # 2. 成交量
    if 'volume' in df.columns:
        vol_colors = ['#ef5350' if c >= o else '#26a69a' for c, o in zip(df['close'], df['open'])]
        fig.add_trace(go.Bar(x=df['date'], y=df['volume'], name='成交量', marker_color=vol_colors, showlegend=False), row=2, col=1)

    # 3. 外資
    if '外資' in df.columns:
        f_colors = ['#ef5350' if v >= 0 else '#26a69a' for v in df['外資']]
        fig.add_trace(go.Bar(x=df['date'], y=df['外資'], name='外資', marker_color=f_colors, showlegend=False), row=3, col=1)

    # 4. 投信
    if '投信' in df.columns:
        t_colors = ['#ef5350' if v >= 0 else '#26a69a' for v in df['投信']]
        fig.add_trace(go.Bar(x=df['date'], y=df['投信'], name='投信', marker_color=t_colors, showlegend=False), row=4, col=1)

    # 日期與版面配置
    dt_all = pd.to_datetime(df['date']).dt.date
    missing_days = [d for d in pd.date_range(dt_all.min(), dt_all.max()).date if d not in set(dt_all)]
    
    total_days = len(df)
    display_days = min(125, total_days)
    initial_range = [df['date'].iloc[-display_days], df['date'].iloc[-1]]
    
    fig.update_layout(
        height=1000, # 高度進一步縮減，讓四張圖更緊湊
        plot_bgcolor='#0e1117', paper_bgcolor='#0e1117', font=dict(color='white', size=16),
        hovermode='x unified', margin=dict(l=10, r=10, t=100, b=20), uirevision=chart_revision,
        xaxis_rangeslider_visible=False,
        xaxis=dict(rangebreaks=[dict(values=missing_days)], range=initial_range if not st.session_state.get(f'__init_range__{stock_id}', False) else None, type='date', fixedrange=False),
        xaxis2=dict(matches='x', rangebreaks=[dict(values=missing_days)], fixedrange=False),
        xaxis3=dict(matches='x', rangebreaks=[dict(values=missing_days)], fixedrange=False),
        xaxis4=dict(matches='x', rangebreaks=[dict(values=missing_days)], fixedrange=False),
        legend=dict(orientation="h", y=1.04, x=0.5, xanchor="center"),
        autosize=True, dragmode='zoom'
    )

    st.session_state[f'__init_range__{stock_id}'] = True
    
    fig.update_yaxes(row=1, col=1, gridcolor='#333', fixedrange=False)
    fig.update_yaxes(title_text="張", row=2, col=1, gridcolor='#333', tickformat=',d', fixedrange=False)
    fig.update_yaxes(title_text="張", row=3, col=1, gridcolor='#333', tickformat=',d', fixedrange=False)
    fig.update_yaxes(title_text="張", row=4, col=1, gridcolor='#333', tickformat=',d', fixedrange=False)
    fig.update_xaxes(showgrid=True, gridcolor='#333')

    return fig

# 下方營收/毛利率圖表函數保持不變...
def plot_revenue_chart(df_revenue, stock_id, stock_name):
    fig = make_subplots(rows=1, cols=1, specs=[[{"secondary_y": True}]])
    df_revenue = df_revenue.copy()
    col_date = '日期' if '日期' in df_revenue.columns else ('date' if 'date' in df_revenue.columns else None)
    col_rev  = '營收' if '營收' in df_revenue.columns else ('revenue' if 'revenue' in df_revenue.columns else None)
    col_mom  = '月增率' if '月增率' in df_revenue.columns else ('mom' if 'mom' in df_revenue.columns else None)

    if col_date is None or col_rev is None: raise ValueError("月營收資料缺少日期/營收欄位")
    if col_mom is None:
        df_revenue['__mom'] = pd.to_numeric(df_revenue[col_rev], errors='coerce').pct_change() * 100.0
        col_mom = '__mom'

    mom_series = pd.to_numeric(df_revenue[col_mom], errors='coerce')
    colors = ['#888888' if pd.isna(m) else ('#3b82f6' if m < 0 else '#ef5350') for m in mom_series]
    revenue_display = (pd.to_numeric(df_revenue[col_rev], errors='coerce') / 1000).round(0).astype('Int64')
    
    fig.add_trace(go.Bar(x=df_revenue[col_date], y=revenue_display, name='月營收', marker_color=colors, hovertemplate='<b>%{x|%Y-%m}</b><br>營收: %{y:,d} 千元<extra></extra>', yaxis='y', showlegend=True), secondary_y=False)
    
    col_yoy = '年增率' if '年增率' in df_revenue.columns else ('yoy' if 'yoy' in df_revenue.columns else None)
    df_yoy = df_revenue[df_revenue[col_yoy].notna()].copy() if col_yoy else df_revenue.iloc[0:0].copy()
    
    fig.add_trace(go.Scatter(x=df_yoy[col_date], y=df_yoy[col_yoy], name='年增率', mode='lines+markers', line=dict(color='#FFD700', width=2.5), marker=dict(size=6, color='#FFD700'), hovertemplate='<b>%{x|%Y-%m}</b><br>年增率: %{y:.2f}%<extra></extra>', yaxis='y2', showlegend=True), secondary_y=True)
    fig.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.3, secondary_y=True)
    
    fig.update_layout(
        height=550, plot_bgcolor='#0e1117', paper_bgcolor='#0e1117', font=dict(color='white', size=16),
        hovermode='x unified', margin=dict(l=60, r=60, t=100, b=40),
        title={'text': f"{stock_id} {stock_name} 月營收與年增率", 'y': 0.98, 'x': 0.5, 'xanchor': 'center', 'yanchor': 'top', 'font': dict(size=20, color='white')},
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5)
    )
    
    fig.update_yaxes(title_text="營收 (千元)", secondary_y=False, gridcolor='#333', tickformat=',d', fixedrange=False)
    fig.update_yaxes(title_text="年增率 (%)", secondary_y=True, gridcolor='#333', tickformat='.1f', fixedrange=False, showgrid=False, range=_get_yoy_range(df_revenue))
    fig.update_xaxes(showgrid=True, gridcolor='#333', dtick="M3", tickformat="%Y-%m")
    
    return fig

def plot_quarterly_chart(df_quarterly, stock_id, stock_name):
    fig = make_subplots(rows=1, cols=1, specs=[[{"secondary_y": True}]])
    revenue_display = (df_quarterly['營收'] / 1000).round(0).astype('Int64')
    revenue_values = revenue_display.astype(float).tolist()
    colors = ['#ef5350' if val < 0 else '#4A90E2' for val in revenue_values]

    has_gm = ('毛利率' in df_quarterly.columns) and (pd.to_numeric(df_quarterly.get('毛利率'), errors='coerce').notna().any())
    fig.add_trace(go.Bar(x=df_quarterly['季度標籤'], y=revenue_values, name='季營收', marker_color=colors, hovertemplate='<b>%{x}</b><br>營收: %{y:,.0f} 千元<extra></extra>', yaxis='y', showlegend=True), secondary_y=False)
    fig.update_traces(base=0, selector=dict(name='季營收'))
    
    df_gp = df_quarterly[df_quarterly['毛利率'].notna()].copy()
    gp_available = (not df_gp.empty)

    if gp_available:
        fig.add_trace(go.Scatter(x=df_gp['季度標籤'], y=df_gp['毛利率'], name='毛利率', mode='lines+markers', line=dict(color='#FF6B6B', width=2.5), marker=dict(size=7, color='#FF6B6B'), hovertemplate='<b>%{x}</b><br>毛利率: %{y:.2f}%<extra></extra>', yaxis='y2', showlegend=True), secondary_y=True)
    
    fig.add_hline(y=0, line_dash="solid", line_color="white", opacity=0.5, line_width=2, secondary_y=False)
    y_range = _get_revenue_range(df_quarterly['營收'])
    
    fig.update_layout(
        height=500, plot_bgcolor='#0e1117', paper_bgcolor='#0e1117', font=dict(color='white', size=16),
        hovermode='x unified', margin=dict(l=60, r=60, t=100, b=40),
        title={'text': (f"{stock_id} {stock_name} 季營收與毛利率" if gp_available else f"{stock_id} {stock_name} 季營收"), 'y': 0.98, 'x': 0.5, 'xanchor': 'center', 'yanchor': 'top', 'font': dict(size=20, color='white')},
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5)
    )
    
    update_dict = {'title_text': "營收 (千元)", 'gridcolor': '#333', 'tickformat': ',d', 'fixedrange': False, 'zeroline': True, 'zerolinewidth': 2, 'zerolinecolor': 'rgba(255,255,255,0.5)', 'showline': True, 'linewidth': 1, 'linecolor': 'white'}
    if y_range: update_dict['range'] = y_range; update_dict['autorange'] = False
    else: update_dict['rangemode'] = 'tozero'
    
    fig.update_yaxes(secondary_y=False, **update_dict)
    fig.update_yaxes(title_text="毛利率 (%)" if gp_available else "", secondary_y=True, gridcolor='#333', tickformat='.1f', fixedrange=False, showgrid=False, visible=gp_available, range=_get_gp_range(df_quarterly) if gp_available else None)
    fig.update_xaxes(showgrid=True, gridcolor='#333')
    
    return fig
