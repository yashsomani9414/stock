"""
Portfolio-Level Backtester for S&P 500 Trading Strategy
========================================================
Simulates equal-weight allocation across "Strong Buy" signals,
tracks daily portfolio value with proper compounding,
and compares against SPY (buy-and-hold benchmark).

Usage:
    python3 backtest_portfolio.py                     # Default: 6 months, top 50 liquid stocks
    python3 backtest_portfolio.py --months 12         # 12-month backtest
    python3 backtest_portfolio.py --universe 100      # Top 100 stocks by volume
"""

import pandas as pd
import numpy as np
import yfinance as yf
import datetime
import argparse
import time
import sys
import os

sys.path.append(os.getcwd())
from fetch_sp500 import calculate_score, calculate_rsi, get_all_potential_tickers

# ─── CONFIG ───────────────────────────────────────────────────────────────────

REBALANCE_FREQ = 5          # Rebalance every N trading days
INITIAL_CAPITAL = 100_000   # Starting portfolio value
MIN_HISTORY_DAYS = 200      # Need 200 days for MA200
COMMISSION_PCT = 0.001      # 0.1% round-trip commission estimate


def load_historical_data(tickers, start_date, end_date):
    """Download historical price data for all tickers at once."""
    print(f"Downloading data for {len(tickers)} tickers from {start_date} to {end_date}...")
    
    all_data = {}
    batch_size = 20
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        try:
            data = yf.download(batch, start=start_date, end=end_date, 
                             group_by='ticker', progress=False)
            if not data.empty:
                for sym in batch:
                    try:
                        if len(batch) > 1:
                            df = data[sym][['Close', 'Volume']].dropna()
                        else:
                            df = data[['Close', 'Volume']].dropna()
                        if len(df) >= MIN_HISTORY_DAYS:
                            all_data[sym] = df
                    except:
                        continue
        except Exception as e:
            print(f"  Error downloading batch {i}: {e}")
        time.sleep(2)
    
    print(f"  Successfully loaded {len(all_data)} tickers with sufficient history.")
    return all_data


def score_stock_on_date(symbol, hist_data, date_idx, sector_pe_med, sector_vol_med):
    """Calculate the score for a stock on a given date using historical data."""
    df = hist_data[symbol]
    slice_df = df.iloc[:date_idx + 1]
    close = slice_df['Close']
    volume = slice_df['Volume']
    
    if len(close) < MIN_HISTORY_DAYS:
        return None, None
    
    ma50 = float(close.tail(50).mean())
    ma200 = float(close.tail(200).mean())
    rsi = calculate_rsi(close)
    curr_price = float(close.iloc[-1])
    dist_ma50 = ((curr_price / ma50) - 1) * 100 if ma50 > 0 else 0
    
    ret1d = (float(close.iloc[-1]) / float(close.iloc[-2]) - 1) * 100 if len(close) >= 2 else 0
    ret5d = (float(close.iloc[-1]) / float(close.iloc[-6]) - 1) * 100 if len(close) >= 6 else 0
    ret1m = (float(close.iloc[-1]) / float(close.iloc[-21]) - 1) * 100 if len(close) >= 21 else 0
    ret6m = (float(close.iloc[-1]) / float(close.iloc[-126]) - 1) * 100 if len(close) >= 126 else 0
    vol6m = float(close.tail(126).pct_change().std() * (252**0.5) * 100) if len(close) >= 126 else 25
    
    avg_v20 = float(volume.tail(20).mean())
    curr_v = float(volume.iloc[-1])
    avg_v5 = float(volume.tail(5).mean())
    
    row = pd.Series({
        'Price': curr_price,
        '50D MA': ma50,
        '200D MA': ma200,
        'Trend Strength': ((ma50 / ma200) - 1) * 100 if ma200 > 0 else 0,
        '1D Return': ret1d,
        '5D Return': ret5d,
        '1M Return': ret1m,
        '6M Return': ret6m,
        '6M Volatility': vol6m,
        'Volume': curr_v,
        'Vol Change 1D': ((curr_v / avg_v20) - 1) * 100 if avg_v20 > 0 else 0,
        'Vol Change 5D': ((avg_v5 / avg_v20) - 1) * 100 if avg_v20 > 0 else 0,
        'RSI': rsi,
        'DistFromMA50': dist_ma50,
        'Market Cap': 50e9,  # We don't have historical market cap; assume passes filter
        'Sector': 'Unknown',
        'P/E Ratio': None,  # Not available historically
    })
    
    score, decision, _ = calculate_score(row, sector_pe_med, sector_vol_med)
    return score, decision


def run_backtest(months=6, universe_size=50):
    """Run a portfolio-level backtest."""
    end_date = datetime.date.today()
    # Need extra history for MA200 calculation
    start_date = end_date - datetime.timedelta(days=months * 30 + MIN_HISTORY_DAYS + 30)
    backtest_start = end_date - datetime.timedelta(days=months * 30)
    
    print(f"\n{'='*60}")
    print(f"PORTFOLIO BACKTEST")
    print(f"{'='*60}")
    print(f"Period: {backtest_start} to {end_date} ({months} months)")
    print(f"Universe: Top {universe_size} most liquid S&P stocks")
    print(f"Initial Capital: ${INITIAL_CAPITAL:,.0f}")
    print(f"Rebalance: Every {REBALANCE_FREQ} trading days")
    print(f"{'='*60}\n")
    
    # Get tickers
    tickers = get_all_potential_tickers()
    if not tickers:
        print("ERROR: Could not fetch tickers.")
        return
    
    # Download all historical data
    hist_data = load_historical_data(tickers, start_date.isoformat(), end_date.isoformat())
    if not hist_data:
        print("ERROR: No historical data loaded.")
        return
    
    # Filter to top N most liquid (by average volume over last 60 days)
    avg_volumes = {}
    for sym, df in hist_data.items():
        try:
            avg_volumes[sym] = float(df['Volume'].tail(60).mean())
        except:
            avg_volumes[sym] = 0
    
    sorted_syms = sorted(avg_volumes, key=avg_volumes.get, reverse=True)[:universe_size]
    print(f"Selected {len(sorted_syms)} stocks for backtesting.\n")
    
    # Download SPY for benchmark
    spy_data = yf.download("SPY", start=start_date.isoformat(), end=end_date.isoformat(), progress=False)
    if isinstance(spy_data.columns, pd.MultiIndex):
        spy_data.columns = spy_data.columns.get_level_values(0)
    spy_close = spy_data['Close'].dropna()
    
    # Find common trading dates within backtest window
    common_dates = None
    for sym in sorted_syms:
        dates = hist_data[sym].index
        if common_dates is None:
            common_dates = dates
        else:
            common_dates = common_dates.intersection(dates)
    
    common_dates = common_dates.sort_values()
    backtest_mask = common_dates >= pd.Timestamp(backtest_start)
    bt_dates = common_dates[backtest_mask]
    
    if len(bt_dates) == 0:
        print("ERROR: No overlapping trading dates in backtest window.")
        return
    
    print(f"Backtest trading days: {len(bt_dates)}")
    print(f"First: {bt_dates[0].date()}, Last: {bt_dates[-1].date()}\n")
    
    # ─── SIMULATE ─────────────────────────────────────────────────────────────
    portfolio_value = INITIAL_CAPITAL
    holdings = {}  # symbol -> num_shares
    cash = INITIAL_CAPITAL
    
    daily_values = []
    trades_log = []
    monthly_returns = {}
    
    sector_pe_med = {'Unknown': 25}
    sector_vol_med = {'Unknown': 25}
    
    for day_i, date in enumerate(bt_dates):
        # Get absolute index in each stock's dataframe
        date_indices = {}
        for sym in sorted_syms:
            idx = hist_data[sym].index.get_loc(date) if date in hist_data[sym].index else None
            if idx is not None:
                date_indices[sym] = idx
        
        # Calculate current portfolio value
        portfolio_value = cash
        for sym, shares in holdings.items():
            if sym in date_indices:
                price = float(hist_data[sym]['Close'].iloc[date_indices[sym]])
                portfolio_value += shares * price
        
        daily_values.append({
            'Date': date,
            'Portfolio': portfolio_value,
            'Holdings': len(holdings),
        })
        
        # Track monthly returns
        month_key = date.strftime('%Y-%m')
        if month_key not in monthly_returns:
            monthly_returns[month_key] = {'start': portfolio_value, 'end': portfolio_value}
        monthly_returns[month_key]['end'] = portfolio_value
        
        # ─── REBALANCE ────────────────────────────────────────────────────────
        if day_i % REBALANCE_FREQ == 0:
            # Score all stocks
            scores = {}
            for sym in sorted_syms:
                if sym not in date_indices:
                    continue
                try:
                    score, decision = score_stock_on_date(
                        sym, hist_data, date_indices[sym],
                        sector_pe_med, sector_vol_med
                    )
                    if score is not None:
                        scores[sym] = (score, decision)
                except:
                    continue
            
            # Find Strong Buy and Buy signals
            buy_signals = [sym for sym, (sc, dec) in scores.items() 
                         if dec in ("Strong Buy", "Buy (Small)")]
            
            sell_signals = [sym for sym, (sc, dec) in scores.items() 
                          if dec in ("Sell", "Reduce")]
            
            # ─── SELL positions that turned Sell/Reduce ────────────────────
            for sym in list(holdings.keys()):
                if sym in sell_signals or sym not in scores:
                    if sym in date_indices:
                        price = float(hist_data[sym]['Close'].iloc[date_indices[sym]])
                        proceeds = holdings[sym] * price * (1 - COMMISSION_PCT)
                        cash += proceeds
                        trades_log.append({
                            'Date': date.date(),
                            'Symbol': sym,
                            'Action': 'SELL',
                            'Price': round(price, 2),
                            'Shares': holdings[sym],
                            'Value': round(proceeds, 2),
                            'Decision': scores.get(sym, (0, 'N/A'))[1]
                        })
                        del holdings[sym]
            
            # ─── BUY new positions ────────────────────────────────────────
            if buy_signals:
                # Equal weight across all buy signals (including existing)
                new_buys = [s for s in buy_signals if s not in holdings]
                
                if new_buys:
                    # Allocate available cash equally
                    per_stock = cash / len(new_buys) if len(new_buys) > 0 else 0
                    
                    for sym in new_buys:
                        if per_stock < 100:  # Skip tiny allocations
                            continue
                        if sym in date_indices:
                            price = float(hist_data[sym]['Close'].iloc[date_indices[sym]])
                            shares = int((per_stock * (1 - COMMISSION_PCT)) / price)
                            if shares > 0:
                                cost = shares * price * (1 + COMMISSION_PCT)
                                cash -= cost
                                holdings[sym] = shares
                                trades_log.append({
                                    'Date': date.date(),
                                    'Symbol': sym,
                                    'Action': 'BUY',
                                    'Price': round(price, 2),
                                    'Shares': shares,
                                    'Value': round(cost, 2),
                                    'Decision': scores[sym][1]
                                })
    
    # ─── RESULTS ──────────────────────────────────────────────────────────────
    
    daily_df = pd.DataFrame(daily_values)
    trades_df = pd.DataFrame(trades_log) if trades_log else pd.DataFrame()
    
    # SPY benchmark return
    spy_start_price = float(spy_close.loc[spy_close.index >= pd.Timestamp(backtest_start)].iloc[0])
    spy_end_price = float(spy_close.iloc[-1])
    spy_return = (spy_end_price / spy_start_price - 1) * 100
    
    # Portfolio metrics
    final_value = daily_df['Portfolio'].iloc[-1]
    total_return = (final_value / INITIAL_CAPITAL - 1) * 100
    
    # Daily returns for Sharpe
    daily_df['DailyReturn'] = daily_df['Portfolio'].pct_change()
    sharpe = (daily_df['DailyReturn'].mean() / daily_df['DailyReturn'].std()) * (252**0.5) if daily_df['DailyReturn'].std() > 0 else 0
    
    # Max Drawdown
    running_max = daily_df['Portfolio'].cummax()
    drawdown = (daily_df['Portfolio'] - running_max) / running_max * 100
    max_drawdown = drawdown.min()
    
    # Print Results
    print(f"\n{'='*60}")
    print(f"BACKTEST RESULTS")
    print(f"{'='*60}")
    print(f"{'Strategy Return:':<30} {total_return:>+8.2f}%")
    print(f"{'SPY Benchmark Return:':<30} {spy_return:>+8.2f}%")
    print(f"{'Alpha (vs SPY):':<30} {total_return - spy_return:>+8.2f}%")
    print(f"{'─'*40}")
    print(f"{'Final Portfolio Value:':<30} ${final_value:>12,.2f}")
    print(f"{'Sharpe Ratio (annualized):':<30} {sharpe:>8.2f}")
    print(f"{'Max Drawdown:':<30} {max_drawdown:>8.2f}%")
    print(f"{'Total Trades:':<30} {len(trades_df):>8}")
    
    if not trades_df.empty:
        buys = trades_df[trades_df['Action'] == 'BUY']
        sells = trades_df[trades_df['Action'] == 'SELL']
        print(f"{'Buy Trades:':<30} {len(buys):>8}")
        print(f"{'Sell Trades:':<30} {len(sells):>8}")
    
    # Monthly breakdown
    print(f"\n{'─'*40}")
    print(f"MONTHLY P&L")
    print(f"{'─'*40}")
    print(f"{'Month':<12} {'Return':>10} {'Value':>15}")
    print(f"{'─'*40}")
    for month, vals in sorted(monthly_returns.items()):
        ret = (vals['end'] / vals['start'] - 1) * 100
        indicator = "🟢" if ret >= 0 else "🔴"
        print(f"{month:<12} {ret:>+9.2f}% ${vals['end']:>12,.2f} {indicator}")
    
    # Last N trades
    if not trades_df.empty:
        print(f"\n{'─'*40}")
        print(f"LAST 20 TRADES")
        print(f"{'─'*40}")
        print(trades_df.tail(20).to_string(index=False))
    
    print(f"\n{'='*60}")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Portfolio-level backtester")
    parser.add_argument("--months", type=int, default=6, help="Number of months to backtest (default: 6)")
    parser.add_argument("--universe", type=int, default=50, help="Number of stocks in universe (default: 50)")
    args = parser.parse_args()
    
    run_backtest(months=args.months, universe_size=args.universe)
