"""
Paper Trading Validator
========================
Validates the model's recommendations against S&P 500 (SPY) over time.

Usage:
    python3 paper_trade.py snapshot     # Take a snapshot of today's Buy signals
    python3 paper_trade.py check        # Check performance vs SPY since snapshot
    python3 paper_trade.py report       # Full report with per-stock breakdown

How it works:
    1. `snapshot` freezes today's Strong Buy & Buy (Small) signals with their
       entry prices into paper_portfolio.json
    2. `check` downloads current prices and compares weighted portfolio return
       vs SPY return over the same period
    3. Run `check` daily, weekly, or at the end of the month
"""

import json
import os
import sys
import datetime
import yfinance as yf
import pandas as pd
from zoneinfo import ZoneInfo

PORTFOLIO_FILE = 'paper_portfolio.json'
DATA_FILE = 'sp500_data.json'


def take_snapshot():
    """Freeze today's Buy signals into a paper portfolio."""
    if not os.path.exists(DATA_FILE):
        print("ERROR: sp500_data.json not found. Run fetch_sp500.py first.")
        return
    
    with open(DATA_FILE, 'r') as f:
        data = json.load(f)
    
    buys = [d for d in data if d.get('Trade Decision') in ('Strong Buy', 'Buy (Small)')]
    
    if not buys:
        print("No Buy signals found. Nothing to snapshot.")
        return
    
    # Sort by score descending
    buys.sort(key=lambda x: x.get('Score', 0), reverse=True)
    
    # Cap at 25 positions (matching MAX_POSITIONS)
    buys = buys[:25]
    
    # Normalize weights so they sum to ~100%
    total_weight = sum(b.get('RecWeight', 3.0) for b in buys)
    
    now = datetime.datetime.now(ZoneInfo("America/Los_Angeles"))
    
    portfolio = {
        'snapshot_date': now.strftime('%Y-%m-%d'),
        'snapshot_time': now.strftime('%Y-%m-%d %H:%M:%S PT'),
        'spy_entry_price': None,
        'total_stocks': len(buys),
        'market_regime': buys[0].get('MarketRegime', 'UNKNOWN') if buys else 'UNKNOWN',
        'positions': []
    }
    
    # Get SPY price at snapshot
    try:
        spy = yf.download("SPY", period="5d", progress=False)
        if isinstance(spy.columns, pd.MultiIndex):
            spy.columns = spy.columns.get_level_values(0)
        portfolio['spy_entry_price'] = round(float(spy['Close'].iloc[-1]), 2)
    except:
        print("WARNING: Could not fetch SPY price.")
    
    for b in buys:
        weight_normalized = round((b.get('RecWeight', 3.0) / total_weight) * 100, 2)
        portfolio['positions'].append({
            'symbol': b['Symbol'],
            'decision': b['Trade Decision'],
            'score': b['Score'],
            'entry_price': b.get('Price'),
            'sector': b.get('Sector', 'Unknown'),
            'rec_weight': b.get('RecWeight', 3.0),
            'portfolio_weight': weight_normalized,
            'rsi_at_entry': round(b.get('RSI') or 0, 1),
            '6m_return_at_entry': round(b.get('6M Return') or 0, 1),
            'trailing_stop': b.get('TrailingStop', 0),
        })
    
    with open(PORTFOLIO_FILE, 'w') as f:
        json.dump(portfolio, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"📸 PAPER PORTFOLIO SNAPSHOT")
    print(f"{'='*60}")
    print(f"Date:          {portfolio['snapshot_date']}")
    print(f"SPY Entry:     ${portfolio['spy_entry_price']}")
    print(f"Positions:     {portfolio['total_stocks']}")
    print(f"Market Regime: {portfolio['market_regime']}")
    print(f"{'─'*60}")
    print(f"{'Sym':6s} {'Score':>5s} {'Decision':15s} {'Entry $':>9s} {'Wt%':>6s} {'Sector':>18s}")
    print(f"{'─'*60}")
    for p in portfolio['positions']:
        print(f"{p['symbol']:6s} {p['score']:5d} {p['decision']:15s} ${p['entry_price']:>8.2f} {p['portfolio_weight']:>5.1f}% {p['sector']:>18s}")
    print(f"{'='*60}")
    print(f"Saved to {PORTFOLIO_FILE}")
    print(f"Run 'python3 paper_trade.py check' anytime to see performance vs SPY.\n")


def check_performance(full_report=False):
    """Check current performance of the paper portfolio vs SPY."""
    if not os.path.exists(PORTFOLIO_FILE):
        print("ERROR: No snapshot found. Run 'python3 paper_trade.py snapshot' first.")
        return
    
    with open(PORTFOLIO_FILE, 'r') as f:
        portfolio = json.load(f)
    
    positions = portfolio['positions']
    snapshot_date = portfolio['snapshot_date']
    spy_entry = portfolio['spy_entry_price']
    
    if not positions:
        print("No positions in portfolio.")
        return
    
    # Get current prices
    symbols = [p['symbol'] for p in positions]
    print(f"Fetching prices for {len(symbols)} stocks + SPY...")
    
    try:
        current_data = yf.download(symbols + ['SPY'], period='5d', progress=False)
        if isinstance(current_data.columns, pd.MultiIndex):
            # Multi-ticker download
            prices = {}
            for sym in symbols + ['SPY']:
                try:
                    prices[sym] = float(current_data[sym]['Close'].dropna().iloc[-1])
                except:
                    pass
        else:
            # Single ticker
            prices = {symbols[0]: float(current_data['Close'].dropna().iloc[-1])}
    except Exception as e:
        print(f"ERROR fetching prices: {e}")
        return
    
    spy_current = prices.get('SPY')
    
    # Calculate returns
    now = datetime.datetime.now(ZoneInfo("America/Los_Angeles"))
    days_elapsed = (now.date() - datetime.datetime.strptime(snapshot_date, '%Y-%m-%d').date()).days
    
    # Weighted portfolio return
    total_return_weighted = 0
    total_weight = 0
    winners = 0
    losers = 0
    results = []
    
    for p in positions:
        sym = p['symbol']
        entry = p['entry_price']
        curr = prices.get(sym)
        weight = p['portfolio_weight']
        
        if curr and entry and entry > 0:
            ret = ((curr / entry) - 1) * 100
            total_return_weighted += ret * (weight / 100)
            total_weight += weight
            if ret > 0:
                winners += 1
            else:
                losers += 1
            
            # Check if trailing stop was hit
            stop_hit = "🛑" if curr < p.get('trailing_stop', 0) else ""
            
            results.append({
                'symbol': sym,
                'entry': entry,
                'current': round(curr, 2),
                'return': round(ret, 2),
                'weight': weight,
                'contribution': round(ret * (weight / 100), 3),
                'decision': p['decision'],
                'sector': p['sector'],
                'stop_hit': stop_hit,
            })
    
    # SPY return
    spy_return = ((spy_current / spy_entry) - 1) * 100 if spy_current and spy_entry else 0
    alpha = total_return_weighted - spy_return
    
    # Print Results
    print(f"\n{'='*65}")
    print(f"📊 PAPER PORTFOLIO PERFORMANCE")
    print(f"{'='*65}")
    print(f"Snapshot Date:    {snapshot_date}")
    print(f"Days Elapsed:     {days_elapsed}")
    print(f"{'─'*65}")
    print(f"{'Portfolio Return:':30s} {total_return_weighted:>+8.2f}%")
    print(f"{'SPY Return:':30s} {spy_return:>+8.2f}%")
    print(f"{'Alpha (Portfolio - SPY):':30s} {alpha:>+8.2f}%  {'✅' if alpha > 0 else '❌'}")
    print(f"{'─'*65}")
    print(f"{'Winners:':30s} {winners:>8d}")
    print(f"{'Losers:':30s} {losers:>8d}")
    print(f"{'Win Rate:':30s} {winners/(winners+losers)*100 if (winners+losers) > 0 else 0:>7.1f}%")
    
    if full_report:
        print(f"\n{'─'*65}")
        print(f"{'POSITION DETAIL':^65}")
        print(f"{'─'*65}")
        print(f"{'Sym':6s} {'Entry':>8s} {'Now':>8s} {'Ret%':>7s} {'Wt%':>5s} {'Contrib':>8s} {'Stop':>4s}")
        print(f"{'─'*65}")
        for r in sorted(results, key=lambda x: x['return'], reverse=True):
            sign = "+" if r['return'] >= 0 else ""
            print(f"{r['symbol']:6s} ${r['entry']:>7.2f} ${r['current']:>7.2f} {sign}{r['return']:>6.2f}% {r['weight']:>4.1f}% {r['contribution']:>+7.3f}% {r['stop_hit']}")
        
        # Sector breakdown
        from collections import Counter
        sector_returns = {}
        for r in results:
            sec = r['sector']
            if sec not in sector_returns:
                sector_returns[sec] = {'total_ret': 0, 'count': 0, 'total_wt': 0}
            sector_returns[sec]['total_ret'] += r['return'] * r['weight']
            sector_returns[sec]['count'] += 1
            sector_returns[sec]['total_wt'] += r['weight']
        
        print(f"\n{'─'*65}")
        print(f"{'SECTOR BREAKDOWN':^65}")
        print(f"{'─'*65}")
        print(f"{'Sector':20s} {'Stocks':>6s} {'Weight':>8s} {'Avg Ret':>8s}")
        print(f"{'─'*65}")
        for sec, v in sorted(sector_returns.items(), key=lambda x: -x[1]['total_wt']):
            avg_ret = v['total_ret'] / v['total_wt'] if v['total_wt'] > 0 else 0
            print(f"{sec:20s} {v['count']:>6d} {v['total_wt']:>7.1f}% {avg_ret:>+7.2f}%")
    
    print(f"\n{'='*65}")
    
    # Save check results
    check_log_file = 'paper_checks.json'
    checks = []
    if os.path.exists(check_log_file):
        with open(check_log_file, 'r') as f:
            checks = json.load(f)
    
    checks.append({
        'check_date': now.strftime('%Y-%m-%d %H:%M'),
        'days_elapsed': days_elapsed,
        'portfolio_return': round(total_return_weighted, 3),
        'spy_return': round(spy_return, 3),
        'alpha': round(alpha, 3),
        'winners': winners,
        'losers': losers,
    })
    
    with open(check_log_file, 'w') as f:
        json.dump(checks, f, indent=2)
    
    print(f"Check logged to {check_log_file}")
    
    # Show historical checks if >1
    if len(checks) > 1:
        print(f"\n{'─'*50}")
        print(f"{'TRACKING HISTORY':^50}")
        print(f"{'─'*50}")
        print(f"{'Date':15s} {'Day':>4s} {'Port%':>8s} {'SPY%':>8s} {'Alpha':>8s}")
        print(f"{'─'*50}")
        for c in checks:
            print(f"{c['check_date']:15s} {c['days_elapsed']:>4d} {c['portfolio_return']:>+7.2f}% {c['spy_return']:>+7.2f}% {c['alpha']:>+7.2f}%")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 paper_trade.py snapshot   — Freeze today's Buy signals")
        print("  python3 paper_trade.py check      — Quick performance vs SPY")
        print("  python3 paper_trade.py report      — Full per-stock breakdown")
        sys.exit(1)
    
    cmd = sys.argv[1].lower()
    if cmd == 'snapshot':
        take_snapshot()
    elif cmd == 'check':
        check_performance(full_report=False)
    elif cmd == 'report':
        check_performance(full_report=True)
    else:
        print(f"Unknown command: {cmd}")
        print("Use: snapshot, check, or report")
