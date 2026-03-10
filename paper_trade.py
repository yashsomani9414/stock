"""
Dynamic Paper Trading Validator
================================
Simulates a live trading account with $10,000 starting cash.
Applies real trades based on daily model signals.

Usage:
    python3 paper_trade.py auto       # Runs daily update: mark-to-market, logs P&L, trades new signals
    python3 paper_trade.py report     # Shows current holdings, cash, and performance vs SPY
    python3 paper_trade.py reset      # Resets account back to $10,000 cash
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
STARTING_CASH = 10000.0


def initialize_portfolio():
    """Start a fresh paper trading account."""
    portfolio = {
        'start_date': datetime.datetime.now(ZoneInfo("America/Los_Angeles")).strftime('%Y-%m-%d'),
        'cash': STARTING_CASH,
        'benchmark_spy_shares': 0.0,
        'benchmark_spy_entry': 0.0,
        'positions': {},  # symbol -> {shares, avg_entry, current_value, sector}
        'history': []
    }
    
    # Get SPY start price
    try:
        spy = yf.download("SPY", period="5d", progress=False)
        if isinstance(spy.columns, pd.MultiIndex):
            spy.columns = spy.columns.get_level_values(0)
        spy_price = float(spy['Close'].dropna().iloc[-1])
        portfolio['benchmark_spy_entry'] = spy_price
        portfolio['benchmark_spy_shares'] = STARTING_CASH / spy_price
    except:
        pass
        
    with open(PORTFOLIO_FILE, 'w') as f:
        json.dump(portfolio, f, indent=2)
    print(f"Account reset. Starting Cash: ${STARTING_CASH}")
    return portfolio


def load_portfolio():
    if not os.path.exists(PORTFOLIO_FILE):
        return initialize_portfolio()
    with open(PORTFOLIO_FILE, 'r') as f:
        portfolio = json.load(f)
        
    # Migrate old static format to dynamic format if necessary
    if 'cash' not in portfolio:
        print("Migrating old snapshot to dynamic format...")
        return initialize_portfolio()
        
    return portfolio


def auto_update():
    """Daily job: mark-to-market current portfolio, log performance, then trade new signals."""
    portfolio = load_portfolio()
    
    if not os.path.exists(DATA_FILE):
        print(f"ERROR: {DATA_FILE} not found.")
        return
        
    with open(DATA_FILE, 'r') as f:
        data = json.load(f)
    signal_map = {d['Symbol']: d for d in data}
    
    now_pt = datetime.datetime.now(ZoneInfo("America/Los_Angeles"))
    date_str = now_pt.strftime('%Y-%m-%d %H:%M')
    
    # 1. Update prices for current positions & SPY
    symbols = list(portfolio['positions'].keys())
    prices = {}
    
    try:
        curr_data = yf.download(symbols + ['SPY'] if symbols else ['SPY'], period='5d', progress=False)
        if isinstance(curr_data.columns, pd.MultiIndex):
            for sym in symbols + ['SPY']:
                try: prices[sym] = float(curr_data[sym]['Close'].dropna().iloc[-1])
                except: pass
        else:
            prices[symbols[0] if symbols else 'SPY'] = float(curr_data['Close'].dropna().iloc[-1])
    except Exception as e:
        print("Error fetching live prices, falling back to JSON data:", e)
    
    # Fallback to JSON data if yfinance fails
    for sym in symbols:
        if sym not in prices and sym in signal_map:
            prices[sym] = signal_map[sym].get('Price', 0)
    if 'SPY' not in prices:
        prices['SPY'] = portfolio['benchmark_spy_entry']
        
    spy_price = prices.get('SPY', portfolio['benchmark_spy_entry'])

    # 2. Mark to Market (Log performance before trading)
    total_value = portfolio['cash']
    for sym, p in portfolio['positions'].items():
        curr_p = prices.get(sym, p['avg_entry'])
        total_value += p['shares'] * curr_p
        
    spy_value = portfolio['benchmark_spy_shares'] * spy_price
    
    portfolio['history'].append({
        'date': date_str,
        'total_value': round(total_value, 2),
        'cash': round(portfolio['cash'], 2),
        'spy_value': round(spy_value, 2)
    })
    
    # 3. Execute Trades based on new signals
    print(f"\nEvaluating Trades ({date_str})...")
    trades_made = 0
    
    for sym, signal_data in signal_map.items():
        decision = signal_data.get('Trade Decision', 'Hold')
        price = signal_data.get('Price', 0)
        
        if not price or price <= 0:
            continue
            
        pos = portfolio['positions'].get(sym)
        
        if decision == 'Sell' and pos:
            # Sell completely
            value = pos['shares'] * price
            portfolio['cash'] += value
            print(f"SELL: {sym} (Sold {pos['shares']:.2f} shares at ${price:.2f} for ${value:.2f})")
            del portfolio['positions'][sym]
            trades_made += 1
            
        elif decision == 'Reduce' and pos:
            # Sell half
            shares_to_sell = pos['shares'] / 2.0
            value = shares_to_sell * price
            portfolio['cash'] += value
            portfolio['positions'][sym]['shares'] -= shares_to_sell
            print(f"REDUCE: {sym} (Sold {shares_to_sell:.2f} shares at ${price:.2f} for ${value:.2f})")
            trades_made += 1
            
        elif decision == 'Strong Buy' and not pos:
            # Buy $100
            if portfolio['cash'] >= 100:
                shares = 100.0 / price
                portfolio['cash'] -= 100.0
                portfolio['positions'][sym] = {
                    'shares': shares, 
                    'avg_entry': price,
                    'sector': signal_data.get('Sector', 'Unknown')
                }
                print(f"STRONG BUY: {sym} (Bought {shares:.2f} shares at ${price:.2f} for $100.00)")
                trades_made += 1
                
        elif decision == 'Buy (Small)' and not pos:
            # Buy $50
            if portfolio['cash'] >= 50:
                shares = 50.0 / price
                portfolio['cash'] -= 50.0
                portfolio['positions'][sym] = {
                    'shares': shares, 
                    'avg_entry': price,
                    'sector': signal_data.get('Sector', 'Unknown')
                }
                print(f"BUY (SMALL): {sym} (Bought {shares:.2f} shares at ${price:.2f} for $50.00)")
                trades_made += 1

    if trades_made == 0:
        print("No trades triggered.")

    with open(PORTFOLIO_FILE, 'w') as f:
        json.dump(portfolio, f, indent=2)
    print(f"Auto-update complete. Total Portfolio Value: ${total_value:.2f}")


def report():
    portfolio = load_portfolio()
    
    symbols = list(portfolio['positions'].keys())
    prices = {}
    try:
        if symbols:
            curr_data = yf.download(symbols + ['SPY'], period='5d', progress=False)
            if isinstance(curr_data.columns, pd.MultiIndex):
                for sym in symbols + ['SPY']:
                    try: prices[sym] = float(curr_data[sym]['Close'].dropna().iloc[-1])
                    except: pass
            else:
                prices[symbols[0]] = float(curr_data['Close'].dropna().iloc[-1])
        else:
            prices['SPY'] = float(yf.download('SPY', period='5d', progress=False)['Close'].dropna().iloc[-1])
    except Exception as e:
        print("Error fetching prices:", e)
        
    total_val = portfolio['cash']
    results = []
    
    for sym, p in portfolio['positions'].items():
        curr_p = prices.get(sym, p['avg_entry'])
        val = p['shares'] * curr_p
        total_val += val
        
        ret_pct = ((curr_p / p['avg_entry']) - 1) * 100 if p['avg_entry'] > 0 else 0
        results.append({
            'sym': sym,
            'shares': p['shares'],
            'entry': p['avg_entry'],
            'current': curr_p,
            'value': val,
            'ret_pct': ret_pct,
            'sector': p.get('sector', 'Unknown')
        })

    spy_price = prices.get('SPY', portfolio['benchmark_spy_entry'])
    spy_value = portfolio['benchmark_spy_shares'] * spy_price
    
    port_ret = ((total_val / STARTING_CASH) - 1) * 100
    spy_ret = ((spy_value / STARTING_CASH) - 1) * 100
    alpha = port_ret - spy_ret
    
    print(f"\n{'='*70}")
    print(f"📊 DYNAMIC PAPER PORTFOLIO REPORT")
    print(f"{'='*70}")
    print(f"Start Date:       {portfolio['start_date']}")
    print(f"Available Cash:   ${portfolio['cash']:.2f}")
    print(f"Total Value:      ${total_val:.2f}")
    print(f"{'─'*70}")
    print(f"Portfolio Return: {port_ret:>+7.2f}%")
    print(f"SPY Return:       {spy_ret:>+7.2f}%")
    print(f"Alpha vs SPY:     {alpha:>+7.2f}%  {'✅' if alpha > 0 else '❌'}")
    
    if results:
        print(f"\n{'─'*70}")
        print(f"{'OPEN POSITIONS':^70}")
        print(f"{'─'*70}")
        print(f"{'Sym':6s} {'Shares':>8s} {'Entry':>8s} {'Current':>8s} {'Value':>9s} {'Ret%':>8s}")
        print(f"{'─'*70}")
        for r in sorted(results, key=lambda x: x['ret_pct'], reverse=True):
            sign = "+" if r['ret_pct'] > 0 else ""
            print(f"{r['sym']:6s} {r['shares']:>8.2f} ${r['entry']:>7.2f} ${r['current']:>7.2f} ${r['value']:>8.2f} {sign}{r['ret_pct']:>7.2f}%")
            
    print(f"\n{'='*70}")
    
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 paper_trade.py auto       — Daily run: mark-to-market, trades signals")
        print("  python3 paper_trade.py report     — Show portfolio metrics and open positions")
        print("  python3 paper_trade.py reset      — Clear portfolio and start over with $10,000")
        sys.exit(1)
        
    cmd = sys.argv[1].lower()
    if cmd == 'auto':
        auto_update()
    elif cmd == 'report':
        report()
    elif cmd == 'reset':
        initialize_portfolio()
    else:
        print(f"Unknown command: {cmd}")
        print("Use: auto, report, or reset")
