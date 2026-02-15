import app
import time

def test_backend():
    print("Testing S&P 500 ticker fetch...")
    tickers = app.get_sp500_tickers()
    if not tickers:
        print("FAIL: Could not fetch tickers.")
        return
    print(f"SUCCESS: Fetched {len(tickers)} tickers.")
    # Check if Name exists
    if 'Name' not in tickers[0]:
        print("FAIL: 'Name' field missing in tickers.")
        return
    else:
         print(f"SUCCESS: Found Company Name: {tickers[0]['Name']}")
    
    print(f"SUCCESS: Fetched {len(tickers)} tickers.")
    
    print("\nTesting single stock data fetch (AAPL)...")
    ticker_obj = next((t for t in tickers if t['Symbol'] == 'AAPL'), None)
    if not ticker_obj:
        print("FAIL: AAPL not found in ticker list.")
        return

    data = app.get_stock_data(ticker_obj)
    if not data:
        print("FAIL: Could not fetch AAPL data.")
        return
        
    print("\nTesting History API for AAPL...")
    with app.test_client() as client:
        res = client.get('/api/history/AAPL')
        if res.status_code == 200:
            hist_data = res.get_json()
            if 'dates' in hist_data and len(hist_data['dates']) > 0:
                print(f"SUCCESS: Fetched {len(hist_data['dates'])} days of history for AAPL.")
            else:
                print("FAIL: History data empty.")
        else:
             print(f"FAIL: History API returned {res.status_code}")
    
    print("SUCCESS: Fetched AAPL data:")
    for key, value in data.items():
        print(f"  {key}: {value}")
        
    print("\nVerifying Trend Strength and Returns...")
    ma50 = data['50D MA']
    ma200 = data['200D MA']
    trend = data['Trend Strength']
    
    print(f"  5D Return: {data.get('5D Return')}%")
    print(f"  1M Return: {data.get('1M Return')}%")
    print(f"  6M Return: {data.get('6M Return')}%")
    
    expected_trend = round(((ma50 / ma200) - 1) * 100, 2)
    if abs(trend - expected_trend) < 0.1:
        print(f"SUCCESS: Trend Strength matches logic: {trend}% vs {expected_trend}%")
    else:
        print(f"FAIL: Trend Strength mismatch: {trend}% vs {expected_trend}%")

if __name__ == "__main__":
    test_backend()
