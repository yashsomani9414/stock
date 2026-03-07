import pandas as pd
import yfinance as yf
import datetime
import os
import sys

# Add current directory to path so we can import fetch_sp500
sys.path.append(os.getcwd())

from fetch_sp500 import calculate_score, calculate_rsi

def backtest_ticker(symbol, period="2y"):
    print(f"\n--- Backtesting {symbol} ({period}) ---")
    df = yf.download(symbol, period=period, interval="1d", progress=False)
    if df.empty:
        print(f"No data found for {symbol}")
        return
    
    # Flatten multi-index if necessary (yf sometimes returns multi-index)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Initial indicators needed for scoring
    # We need at least 200 days to start calculating MA200 and scoring accurately
    df['50D MA'] = df['Close'].rolling(window=50).mean()
    df['200D MA'] = df['Close'].rolling(window=200).mean()
    
    # Calculate RSI (we'll do it manually here to avoid dependencies if needed, but we have it)
    # Actually, we can just apply our function day-by-day
    
    trades = []
    in_position = False
    buy_price = 0
    buy_date = None
    
    # Skip the first 200 days to ensure MAs are populated
    for i in range(200, len(df)):
        current_date = df.index[i]
        hist_slice = df.iloc[:i+1] # All data up to today
        
        # Calculate row for scoring
        close = hist_slice['Close']
        volume = hist_slice['Volume']
        
        ma50 = close.iloc[-1] / hist_slice['50D MA'].iloc[-1] if hist_slice['50D MA'].iloc[-1] > 0 else 1
        ma200 = hist_slice['200D MA'].iloc[-1]
        
        # Performance/Indicators for the score function
        row = {
            'Price': close.iloc[-1],
            '50D MA': hist_slice['50D MA'].iloc[-1],
            '200D MA': ma200,
            'Trend Strength': ((hist_slice['50D MA'].iloc[-1] / ma200) - 1) * 100 if ma200 > 0 else 0,
            '1D Return': (close.iloc[-1]/close.iloc[-2]-1)*100 if len(close)>=2 else 0,
            '5D Return': (close.iloc[-1]/close.iloc[-6]-1)*100 if len(close)>=6 else 0,
            '1M Return': (close.iloc[-1]/close.iloc[-21]-1)*100 if len(close)>=21 else 0,
            '6M Return': (close.iloc[-1]/close.iloc[-126]-1)*100 if len(close)>=126 else 0,
            '6M Volatility': close.tail(126).pct_change().std()*(252**0.5)*100 if len(close)>=126 else 25,
            'Volume': volume.iloc[-1],
            'Vol Change 1D': (volume.iloc[-1] / volume.iloc[-20: -1].mean() - 1) * 100 if len(volume)>20 else 0,
            'Vol Change 5D': (volume.iloc[-5:].mean() / volume.iloc[-20:-5].mean() -1) * 100 if len(volume)>20 else 0,
            'RSI': calculate_rsi(close),
            'DistFromMA50': ((close.iloc[-1] / hist_slice['50D MA'].iloc[-1]) - 1) *100 if hist_slice['50D MA'].iloc[-1] > 0 else 0,
            'Market Cap': 50e9, # Dummy for backtest filter
            'Sector': 'Technology' # Dummy
        }
        
        # Apply score
        # Dummy medians
        pe_med = {'Technology': 25}
        vol_med = {'Technology': 25}
        
        score, decision, _ = calculate_score(pd.Series(row), pe_med, vol_med)
        
        # Log signals (for debugging)
        # if decision == "Strong Buy": print(f"{current_date.date()}: {decision} ({score}) @ {row['Price']:.2f}")

        # TRADING LOGIC
        if not in_position:
            if decision == "Strong Buy":
                in_position = True
                buy_price = row['Price']
                buy_date = current_date
        else:
            # EXIT LOGIC: Sell if decision becomes "Sell" or score drops significantly
            # Or simplified: if score < 60
            if decision == "Sell" or score < 60:
                sell_price = row['Price']
                profit_pct = (sell_price / buy_price - 1) * 100
                trades.append({
                    'BuyDate': buy_date.date(),
                    'SellDate': current_date.date(),
                    'BuyPrice': round(buy_price, 2),
                    'SellPrice': round(sell_price, 2),
                    'Profit%': round(profit_pct, 2)
                })
                in_position = False

    # Summary
    if not trades:
        print("No trades executed.")
        return
    
    trades_df = pd.DataFrame(trades)
    win_rate = (trades_df['Profit%'] > 0).mean() * 100
    total_return = trades_df['Profit%'].sum()
    avg_trade = trades_df['Profit%'].mean()
    
    print(f"Total Trades: {len(trades_df)}")
    print(f"Win Rate: {win_rate:.1f}%")
    print(f"Total Cumulative Return: {total_return:.2f}%")
    print(f"Average Profit per Trade: {avg_trade:.2f}%")
    print("\nTrades:")
    print(trades_df.to_string(index=False))

if __name__ == "__main__":
    test_symbols = ["NVDA", "AAPL", "MSFT", "GOOGL"]
    for s in test_symbols:
        try:
            backtest_ticker(s)
        except Exception as e:
            print(f"Error testing {s}: {e}")
