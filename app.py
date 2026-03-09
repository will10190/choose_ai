import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import datetime

# --- 頁面基本設定 ---
st.set_page_config(page_title="台股條件選股 App (MVP)", layout="wide")
st.title("📈 台股條件選股 Web App (MVP版)")
st.write("輸入你的觀察清單，設定條件，一鍵篩選！")

# --- 側邊欄：設定選股條件 ---
st.sidebar.header("設定選股條件")
stock_list_input = st.sidebar.text_area("輸入要篩選的股票代號 (用逗號隔開)", "2330, 2317, 2454, 2881, 2882")
vol_threshold = st.sidebar.number_input("成交量大於 (張)", min_value=0, value=1000)
foreign_buy_days = st.sidebar.number_input("外資連續買超天數", min_value=0, value=2)

# --- 資料獲取函式 ---
def get_stock_data(stock_id):
    """取得 yfinance 價量資料並計算月線"""
    try:
        # yfinance 台股代號需要加上 .TW
        ticker = f"{stock_id.strip()}.TW"
        stock = yf.Ticker(ticker)
        # 抓取過去 30 天資料確保夠算 20MA
        hist = stock.history(period="1mo") 
        if hist.empty:
            return None
        
        latest_close = hist['Close'].iloc[-1]
        latest_vol = hist['Volume'].iloc[-1] / 1000 # 換算成張
        ma20 = hist['Close'].rolling(window=20).mean().iloc[-1]
        
        return {
            "最新收盤價": round(latest_close, 2),
            "成交量(張)": int(latest_vol),
            "站上月線(20MA)": latest_close > ma20
        }
    except Exception as e:
        return None

def get_foreign_buy_days(stock_id):
    """取得 FinMind 外資買賣超資料 (MVP 使用免費無 Token 呼叫)"""
    try:
        # 抓過去 10 天資料來判斷連買
        url = "https://api.finmindtrade.com/api/v4/data"
        parameter = {
            "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
            "data_id": stock_id.strip(),
            "start_date": (datetime.date.today() - datetime.timedelta(days=15)).strftime("%Y-%m-%d")
        }
        resp = requests.get(url, params=parameter)
        data = resp.json()
        
        if data["msg"] != "success":
            return 0
            
        df = pd.DataFrame(data["data"])
        # 篩選外資(Foreign_Investor)
        df_foreign = df[df['name'] == 'Foreign_Investor'].sort_values('date', ascending=False)
        
        buy_days = 0
        for val in df_foreign['buy'] - df_foreign['sell']:
            if val > 0:
                buy_days += 1
            else:
                break # 只要有一天沒買超，中斷連買計算
        return buy_days
    except Exception as e:
        return 0

# --- 主程式執行區塊 ---
if st.button("🚀 開始篩選", type="primary"):
    stock_ids = [s.strip() for s in stock_list_input.split(',')]
    results = []
    
    # 建立進度條
    progress_text = "資料抓取中，請稍候..."
    my_bar = st.progress(0, text=progress_text)
    
    for i, stock_id in enumerate(stock_ids):
        # 更新進度條
        my_bar.progress((i + 1) / len(stock_ids), text=f"正在分析: {stock_id}")
        
        # 抓資料
        tech_data = get_stock_data(stock_id)
        if tech_data is None:
            continue
            
        f_buy_days = get_foreign_buy_days(stock_id)
        
        # 判斷是否符合條件
        cond1 = tech_data["成交量(張)"] > vol_threshold
        cond2 = f_buy_days >= foreign_buy_days
        cond3 = tech_data["站上月線(20MA)"] # MVP 預設條件
        
        if cond1 and cond2 and cond3:
            results.append({
                "股票代號": stock_id,
                "收盤價": tech_data["最新收盤價"],
                "成交量(張)": tech_data["成交量(張)"],
                "外資連買天數": f_buy_days,
                "站上月線": "✅" if tech_data["站上月線(20MA)"] else "❌"
            })
            
    my_bar.empty() # 清除進度條
    
    # --- 顯示結果 ---
    if results:
        st.success(f"篩選完成！共找到 {len(results)} 檔符合條件的股票。")
        df_results = pd.DataFrame(results)
        st.dataframe(df_results, use_container_width=True)
        
        # 下載 CSV 功能
        csv = df_results.to_csv(index=False).encode('utf-8-sig') # utf-8-sig 解決 Excel 中文亂碼
        st.download_button(
            label="📥 下載結果 CSV",
            data=csv,
            file_name='stock_screener_results.csv',
            mime='text/csv',
        )
    else:
        st.warning("沒有找到符合條件的股票，請嘗試放寬篩選條件。")
