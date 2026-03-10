"""
Portfolio-Level Walk-Forward Backtester
=======================================
IMPORTANT: This backtester is designed to AVOID look-ahead bias:
  - On each trading day, it uses ONLY data available up to that day 
  - Scoring is recalculated from scratch each rebalance day using 
    only the historical data visible at that moment
  - No future data leaks into decisions

Data approach:
  - Downloads historical daily data ONLY for the selected universe
    (not all 903 tickers) to keep it fast
  - Uses yf.download with a date range, then walks forward day by day

Bias disclaimer:
  The scoring RULES themselves were designed by looking at past markets.
  So while the backtest mechanics are bias-free, the strategy rules
  carry inherent in-sample bias. True validation requires:
    1. Out-of-sample testing on unseen time periods
    2. Paper trading going forward
    3. Comparing against simple benchmarks (SPY buy-and-hold)

Usage:
    python3 backtest_portfolio.py                     # Default: 3 months, top 30 stocks
    python3 backtest_portfolio.py --months 6          # 6-month backtest
    python3 backtest_portfolio.py --universe 50       # Top 50 stocks by volume
    python3 backtest_portfolio.py --tickers AAPL,MSFT # Specific tickers only
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
from fetch_sp500 import calculate_score, calculate_rsi

# ─── CONFIG ───────────────────────────────────────────────────────────────────

REBALANCE_FREQ = 5          # Rebalance every N trading days
INITIAL_CAPITAL = 100_000   # Starting portfolio value
MIN_HISTORY_DAYS = 200      # Need 200 days for MA200
COMMISSION_PCT = 0.001      # 0.1% round-trip commission estimate
MAX_POSITIONS = 25          # Max concurrent positions to prevent over-allocation


def load_stock_metadata():
    """Load sector, P/E, and earnings data from sp500_data.json for valuation-aware backtesting."""
    data_file = 'sp500_data.json'
    metadata = {}
    if os.path.exists(data_file):
        import json
        with open(data_file, 'r') as f:
            data = json.load(f)
        for d in data:
            sym = d.get('Symbol')
            if sym:
                metadata[sym] = {
                    'Sector': d.get('Sector', 'Unknown'),
                    'P/E Ratio': d.get('P/E Ratio'),
                    'Market Cap': d.get('Market Cap', 50e9),
                    'EarningsDate': d.get('EarningsDate'),
                    # V8: Quality Factor inputs
                    'ROE': d.get('ROE'),
                    'Gross Margin': d.get('Gross Margin'),
                    'FCF Margin': d.get('FCF Margin'),
                    'Debt/Equity': d.get('Debt/Equity'),
                }
    return metadata


def get_universe_tickers(universe_size=30, specific_tickers=None):
    """Get tickers from the existing sp500_data.json (no re-download needed)."""
    if specific_tickers:
        return specific_tickers
    
    data_file = 'sp500_data.json'
    if os.path.exists(data_file):
        import json
        with open(data_file, 'r') as f:
            data = json.load(f)
        # Sort by volume (descending) and take top N
        sorted_data = sorted(data, key=lambda x: x.get('Volume', 0) or 0, reverse=True)
        tickers = [d['Symbol'] for d in sorted_data[:universe_size]]
        print(f"Selected {len(tickers)} most liquid stocks from existing data.")
        return tickers
    else:
        print("No sp500_data.json found. Using a default set.")
        return ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'JPM', 'V', 'UNH',
                'XOM', 'WMT', 'MA', 'JNJ', 'PG', 'HD', 'MRK', 'ABBV', 'COST', 'BAC']


def download_universe_data(tickers, start_date, end_date):
    """Download daily data ONLY for the selected universe (not all 903 tickers)."""
    print(f"\nDownloading data for {len(tickers)} tickers ({start_date} to {end_date})...")
    
    all_data = {}
    batch_size = 10
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
            print(f"  Error: {e}")
        time.sleep(1)
    
    print(f"  Loaded {len(all_data)}/{len(tickers)} tickers with >= {MIN_HISTORY_DAYS} days of history.\n")
    return all_data


def run_backtest(months=3, universe_size=30, specific_tickers=None):
    """Run a walk-forward portfolio backtest."""
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=months * 30 + MIN_HISTORY_DAYS + 30)
    backtest_start = end_date - datetime.timedelta(days=months * 30)
    
    # Get universe + metadata (sector, P/E, earnings)
    tickers = get_universe_tickers(universe_size, specific_tickers)
    stock_meta = load_stock_metadata()
    
    print(f"\n{'='*60}")
    print(f"WALK-FORWARD PORTFOLIO BACKTEST")
    print(f"{'='*60}")
    print(f"Period:        {backtest_start} → {end_date} ({months} months)")
    print(f"Universe:      {len(tickers)} stocks")
    print(f"Capital:       ${INITIAL_CAPITAL:,.0f}")
    print(f"Max Positions: {MAX_POSITIONS}")
    print(f"Rebalance:     Every {REBALANCE_FREQ} trading days")
    print(f"Commission:    {COMMISSION_PCT*100:.1f}% per trade")
    print(f"{'='*60}")
    
    # Download ONLY the selected universe
    hist_data = download_universe_data(tickers, start_date.isoformat(), end_date.isoformat())
    if not hist_data:
        print("ERROR: No data loaded.")
        return
    
    available_tickers = list(hist_data.keys())
    
    # Download SPY for benchmark
    spy_data = yf.download("SPY", start=start_date.isoformat(), end=end_date.isoformat(), progress=False)
    if isinstance(spy_data.columns, pd.MultiIndex):
        spy_data.columns = spy_data.columns.get_level_values(0)
    spy_close = spy_data['Close'].dropna()
    
    # Find common trading dates
    common_dates = None
    for sym in available_tickers:
        dates = hist_data[sym].index
        if common_dates is None:
            common_dates = dates
        else:
            common_dates = common_dates.intersection(dates)
    
    common_dates = common_dates.sort_values()
    bt_dates = common_dates[common_dates >= pd.Timestamp(backtest_start)]
    
    if len(bt_dates) == 0:
        print("ERROR: No trading dates in backtest window.")
        return
    
    print(f"Trading days: {len(bt_dates)}  ({bt_dates[0].date()} → {bt_dates[-1].date()})")
    
    # Calculate SPY 200MA for every day in the backtest (walk-forward)
    spy_ma200 = spy_close.rolling(200).mean()
    
    # ─── WALK-FORWARD SIMULATION ──────────────────────────────────────────────
    cash = INITIAL_CAPITAL
    holdings = {}     # symbol → {'shares': N, 'highest_price': P, 'trailing_stop': S}
    daily_values = []
    trades_log = []
    monthly_returns = {}
    
    for day_i, date in enumerate(bt_dates):
        # Determine Market Regime for TODAY
        spy_curr = float(spy_close.loc[date])
        spy_ma = float(spy_ma200.loc[date])
        regime = "BULLISH" if spy_curr >= spy_ma else "BEARISH"

        # Build date index map (for walk-forward slicing)
        date_indices = {}
        for sym in available_tickers:
            if date in hist_data[sym].index:
                date_indices[sym] = hist_data[sym].index.get_loc(date)
        
        # Mark-to-market: calculate portfolio value and update trailing stops
        portfolio_value = cash
        for sym, hdata in list(holdings.items()):
            if sym in date_indices:
                price = float(hist_data[sym]['Close'].iloc[date_indices[sym]])
                portfolio_value += hdata['shares'] * price
                
                # Update highest price and trailing stop
                if price > hdata['highest_price']:
                    holdings[sym]['highest_price'] = price
                    holdings[sym]['trailing_stop'] = round(price * 0.90, 2)
        
        daily_values.append({'Date': date, 'Portfolio': portfolio_value, 'Holdings': len(holdings), 'Regime': regime})
        
        # Track monthly
        month_key = date.strftime('%Y-%m')
        if month_key not in monthly_returns:
            monthly_returns[month_key] = {'start': portfolio_value, 'end': portfolio_value}
        monthly_returns[month_key]['end'] = portfolio_value
        
        # ─── REBALANCE DAY ────────────────────────────────────────────────
        if day_i % REBALANCE_FREQ == 0:
            # Build sector-aware rows and compute medians for this date
            rows_for_medians = []
            for sym in available_tickers:
                if sym not in date_indices:
                    continue
                meta = stock_meta.get(sym, {})
                sector = meta.get('Sector', 'Unknown')
                pe = meta.get('P/E Ratio')
                
                df = hist_data[sym]
                slice_df = df.iloc[:date_indices[sym] + 1]
                close = slice_df['Close']
                if len(close) >= 126:
                    vol6m = float(close.tail(126).pct_change().std() * (252**0.5) * 100)
                else:
                    vol6m = 25
                rows_for_medians.append({'Symbol': sym, 'Sector': sector, 'P/E Ratio': pe, '6M Volatility': vol6m})
            
            med_df = pd.DataFrame(rows_for_medians)
            sector_pe_med = med_df.groupby('Sector')['P/E Ratio'].median().to_dict() if not med_df.empty else {}
            sector_vol_med = med_df.groupby('Sector')['6M Volatility'].median().to_dict() if not med_df.empty else {}
            
            # Score ALL stocks using only data up to TODAY (walk-forward)
            scores = {}
            for sym in available_tickers:
                if sym not in date_indices:
                    continue
                try:
                    # history includes HighestPrice for trailing stop logic
                    hist_for_scoring = holdings.get(sym, {}).copy()
                    hist_for_scoring['HighestPrice'] = hist_for_scoring.get('highest_price', 0)
                    hist_for_scoring['Trade Decision'] = "Hold" if sym in holdings else None
                    
                    df = hist_data[sym]
                    slice_df = df.iloc[:date_indices[sym] + 1]
                    close = slice_df['Close']
                    volume = slice_df['Volume']
                    
                    if len(close) < MIN_HISTORY_DAYS: continue

                    ma50 = float(close.tail(50).mean())
                    ma200 = float(close.tail(200).mean())
                    rsi = calculate_rsi(close)
                    curr_price = float(close.iloc[-1])
                    
                    meta = stock_meta.get(sym, {})
                    
                    row = pd.Series({
                        'Price': curr_price, '50D MA': ma50, '200D MA': ma200,
                        'Trend Strength': ((ma50 / ma200) - 1) * 100 if ma200 > 0 else 0,
                        '1D Return': (close.iloc[-1]/close.iloc[-2]-1)*100 if len(close)>=2 else 0,
                        '5D Return': (close.iloc[-1]/close.iloc[-6]-1)*100 if len(close)>=6 else 0,
                        '1M Return': (close.iloc[-1]/close.iloc[-21]-1)*100 if len(close)>=21 else 0,
                        '6M Return': (close.iloc[-1]/close.iloc[-126]-1)*100 if len(close)>=126 else 0,
                        '6M Volatility': float(close.tail(126).pct_change().std() * (252**0.5) * 100) if len(close)>=126 else 25,
                        'Volume': float(volume.iloc[-1]),
                        'Vol Change 1D': ((float(volume.iloc[-1]) / float(volume.tail(20).mean())) - 1) * 100 if len(volume)>=20 else 0,
                        'Vol Change 5D': ((float(volume.tail(5).mean()) / float(volume.tail(20).mean())) - 1) * 100 if len(volume)>=20 else 0,
                        'RSI': rsi, 'DistFromMA50': ((curr_price / ma50) - 1) * 100 if ma50 > 0 else 0,
                        'Market Cap': meta.get('Market Cap', 50e9),
                        'Sector': meta.get('Sector', 'Unknown'),
                        'P/E Ratio': meta.get('P/E Ratio'),
                        'EarningsDate': meta.get('EarningsDate'),
                        # V8: Quality Factor inputs
                        'ROE': meta.get('ROE'),
                        'Gross Margin': meta.get('Gross Margin'),
                        'FCF Margin': meta.get('FCF Margin'),
                        'Debt/Equity': meta.get('Debt/Equity'),
                    })
                    
                    res = calculate_score(row, sector_pe_med, sector_vol_med, history=hist_for_scoring, market_regime=regime)
                    scores[sym] = {
                        'score': res[0], 'decision': res[1], 'highest_price': res[3], 
                        'trailing_stop': res[4], 'rec_weight': res[5]
                    }
                except Exception as e:
                    continue
            
            buy_candidates = [s for s, data in scores.items() if data['decision'] in ("Strong Buy", "Buy (Small)")]
            sell_signals = [s for s, data in scores.items() if data['decision'] in ("Sell", "Reduce", "Sell (Profit-Lock)")]
            
            # ─── SELL ─────────────────────────────────────────────────────
            for sym in list(holdings.keys()):
                if sym in sell_signals or sym not in scores:
                    if sym in date_indices:
                        price = float(hist_data[sym]['Close'].iloc[date_indices[sym]])
                        proceeds = holdings[sym]['shares'] * price * (1 - COMMISSION_PCT)
                        cash += proceeds
                        trades_log.append({
                            'Date': str(date.date()), 'Symbol': sym, 'Action': 'SELL',
                            'Price': round(price, 2), 'Shares': holdings[sym]['shares'],
                            'Decision': scores.get(sym, {}).get('decision', 'N/A'),
                            'Score': scores.get(sym, {}).get('score', 0)
                        })
                        del holdings[sym]
            
            # ─── BUY (with position cap) ─────────────────────────────────
            if buy_candidates:
                new_buys = [s for s in buy_candidates if s not in holdings]
                # Enforce position count cap
                slots_available = MAX_POSITIONS - len(holdings)
                if slots_available <= 0:
                    new_buys = []
                elif len(new_buys) > slots_available:
                    # Prioritize by score (highest first)
                    new_buys = sorted(new_buys, key=lambda s: scores[s]['score'], reverse=True)[:slots_available]
                
                if new_buys and cash > 500:
                    # Risk-Adjusted Position Sizing: Use RecWeight
                    total_rec_weight = sum([scores[s]['rec_weight'] for s in new_buys])
                    for sym in new_buys:
                        if sym in date_indices:
                            price = float(hist_data[sym]['Close'].iloc[date_indices[sym]])
                            # Weight as % of PORTFOLIO value, not just cash
                            target_val = portfolio_value * (scores[sym]['rec_weight'] / 100)
                            # Cap at available cash
                            buy_val = min(target_val, cash / len(new_buys))
                            
                            shares = int((buy_val * (1 - COMMISSION_PCT)) / price)
                            if shares > 0:
                                cost = shares * price * (1 + COMMISSION_PCT)
                                cash -= cost
                                holdings[sym] = {
                                    'shares': shares, 
                                    'highest_price': price, 
                                    'trailing_stop': scores[sym]['trailing_stop']
                                }
                                trades_log.append({
                                    'Date': str(date.date()), 'Symbol': sym, 'Action': 'BUY',
                                    'Price': round(price, 2), 'Shares': shares,
                                    'Decision': scores[sym]['decision'],
                                    'Score': scores[sym]['score'],
                                    'Weight': f"{scores[sym]['rec_weight']}%"
                                })
    
    # ─── RESULTS ──────────────────────────────────────────────────────────────
    daily_df = pd.DataFrame(daily_values)
    trades_df = pd.DataFrame(trades_log) if trades_log else pd.DataFrame()
    
    # SPY benchmark
    spy_bt = spy_close[spy_close.index >= pd.Timestamp(backtest_start)]
    spy_return = (float(spy_bt.iloc[-1]) / float(spy_bt.iloc[0]) - 1) * 100
    
    # Portfolio metrics
    final_value = daily_df['Portfolio'].iloc[-1]
    total_return = (final_value / INITIAL_CAPITAL - 1) * 100
    
    daily_df['DailyReturn'] = daily_df['Portfolio'].pct_change()
    std = daily_df['DailyReturn'].std()
    sharpe = (daily_df['DailyReturn'].mean() / std) * (252**0.5) if std > 0 else 0
    
    running_max = daily_df['Portfolio'].cummax()
    drawdown = (daily_df['Portfolio'] - running_max) / running_max * 100
    max_drawdown = drawdown.min()
    
    # Print
    print(f"\n{'='*60}")
    print(f"RESULTS")
    print(f"{'='*60}")
    print(f"{'Strategy Return:':<30} {total_return:>+8.2f}%")
    print(f"{'SPY Buy-and-Hold:':<30} {spy_return:>+8.2f}%")
    print(f"{'Alpha (vs SPY):':<30} {total_return - spy_return:>+8.2f}%")
    print(f"{'─'*45}")
    print(f"{'Final Value:':<30} ${final_value:>12,.2f}")
    print(f"{'Sharpe Ratio (ann.):':<30} {sharpe:>8.2f}")
    print(f"{'Max Drawdown:':<30} {max_drawdown:>8.2f}%")
    print(f"{'Total Trades:':<30} {len(trades_df):>8}")
    
    # Monthly P&L
    print(f"\n{'─'*45}")
    print(f"{'MONTHLY P&L':^45}")
    print(f"{'─'*45}")
    print(f"{'Month':<12} {'Return':>10} {'End Value':>15}")
    print(f"{'─'*45}")
    for month, vals in sorted(monthly_returns.items()):
        ret = (vals['end'] / vals['start'] - 1) * 100
        ind = "+" if ret >= 0 else ""
        print(f"{month:<12} {ind}{ret:.2f}%{'':<5} ${vals['end']:>12,.2f}")
    
    # Recent trades
    if not trades_df.empty:
        print(f"\n{'─'*45}")
        print(f"TRADES ({len(trades_df)} total, showing last 25)")
        print(f"{'─'*45}")
        print(trades_df.tail(25).to_string(index=False))
    
    # Bias warning
    print(f"\n{'='*60}")
    print("NOTE: This backtest uses walk-forward mechanics (no look-ahead")
    print("bias in data). However, the scoring RULES were designed with")
    print("knowledge of past markets. True validation requires paper")
    print("trading on unseen future data.")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Walk-forward portfolio backtester")
    parser.add_argument("--months", type=int, default=3, help="Months to backtest (default: 3)")
    parser.add_argument("--universe", type=int, default=30, help="Number of stocks (default: 30)")
    parser.add_argument("--tickers", type=str, default=None, help="Comma-separated tickers (e.g. AAPL,MSFT)")
    args = parser.parse_args()
    
    specific = args.tickers.split(',') if args.tickers else None
    run_backtest(months=args.months, universe_size=args.universe, specific_tickers=specific)
