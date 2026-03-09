from flask import Flask, render_template, jsonify, request
import pandas as pd
import datetime
import json
import os
import requests
import time
import yfinance as yf
from bs4 import BeautifulSoup
import threading
import re
from zoneinfo import ZoneInfo
from fetch_sp500 import load_sp500_data as load_local_data, calculate_sector_data, fetch_and_save, DATA_FILE, sanitize_data

GITHUB_DATA_URL = "https://raw.githubusercontent.com/yashsomani9414/stock/main/sp500_data.json"

app = Flask(__name__)

# Global state for background refresh
refresh_status = {
    "is_running": False,
    "current": 0,
    "total": 0,
    "status": "idle",
    "message": ""
}
refresh_lock = threading.Lock()

def load_sp500_data():
    """Load data with GitHub fallback if local is stale or missing."""
    local_data = load_local_data()
    
    try:
        # Try to fetch latest from GitHub to see if it's newer
        # We do this to ensure Cloud Run (ephemeral) stays updated with the Action's work
        resp = requests.get(GITHUB_DATA_URL, timeout=5)
        if resp.status_code == 200:
            github_data = resp.json()
            if github_data and len(github_data) > 0:
                gh_updated = github_data[0].get('LastUpdated')
                loc_updated = local_data[0].get('LastUpdated') if local_data else None
                
                if not loc_updated or (gh_updated and gh_updated > loc_updated):
                    print("Updating local cache from GitHub Raw.")
                    with open(DATA_FILE, 'w') as f:
                        json.dump(github_data, f)
                    return github_data
    except Exception as e:
        print(f"GitHub fallback failed: {e}")
        
    return local_data

def check_stale_and_refresh():
    """Trigger background refresh if data is older than 24h OR never refreshed.
    Safe for Cloud Run (non-blocking)."""
    global refresh_status
    try:
        data = load_sp500_data()
        needs_refresh = False
        
        if not data or len(data) == 0:
            needs_refresh = True
        else:
            last_updated = data[0].get('LastUpdated')
            if last_updated:
                # LastUpdated is "2024-03-03 17:00:00"
                last_dt = datetime.datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("America/Los_Angeles"))
                # If more than 24 hours since last refresh
                now_pt = datetime.datetime.now(ZoneInfo("America/Los_Angeles"))
                if (now_pt - last_dt).total_seconds() > 86400:
                    needs_refresh = True
            else:
                needs_refresh = True
        
        if needs_refresh:
            with refresh_lock:
                if not refresh_status["is_running"]:
                    thread = threading.Thread(target=fetch_and_update_data_wrapper)
                    thread.daemon = True
                    thread.start()
                    print("On-visit refresh triggered.")
    except Exception as e:
        print(f"Error in on-visit check: {e}")


@app.route('/')
def index():
    check_stale_and_refresh()
    return render_template('index.html')

@app.route('/sector')
def sector_page():
    return render_template('sector.html')

@app.route('/earnings')
def earnings_page():
    return render_template('earnings.html')

@app.route('/stock/<symbol>')
def stock_detail(symbol):
    return render_template('stock_detail.html', symbol=symbol.upper())

@app.route('/market-news')
def market_news_page():
    return render_template('market_news.html')

@app.route('/api/data')
def api_data():
    check_stale_and_refresh()
    data = load_sp500_data()
    return jsonify(sanitize_data(data))

@app.route('/api/sector_data')
def api_sector_data():
    data = load_sp500_data()
    sector_data = calculate_sector_data(data)
    return jsonify(sanitize_data(sector_data))

@app.route('/api/history/<symbol>')
def api_history(symbol):
    """Return historical prices from Yahoo Finance for the chart."""
    try:
        stock = yf.Ticker(symbol)
        # Fetch 1 year of daily data
        hist = stock.history(period="1y")
        
        if hist is None or hist.empty:
            return jsonify({'error': 'No history found'}), 404
            
        # Format for Chart.js
        # Check if 'Close' exists
        if 'Close' not in hist.columns:
            return jsonify({'error': 'Close price data missing'}), 404

        prices = hist['Close'].tolist()
        dates = hist.index.strftime('%Y-%m-%d').tolist()
        
        return jsonify({
            'symbol': symbol,
            'prices': [round(p, 2) for p in prices],
            'dates': dates
        })
    except Exception as e:
        print(f"Error fetching history for {symbol}: {e}")
        return jsonify({'error': str(e)}), 500

def parse_yf_news(news_list):
    """Helper to parse yfinance news list into a consistent format."""
    if not news_list:
        return []
        
    formatted = []
    for item in news_list:
        if not item or not isinstance(item, dict):
            continue
            
        content = item.get("content", {})
        if not content:
            content = {}
            
        # Title
        title = content.get("title") or item.get("title")
        
        # Link - Robust nested check
        link_obj = content.get("clickThroughUrl")
        link = (link_obj.get("url") if isinstance(link_obj, dict) else None) or item.get("link")
        
        # Publisher - Robust nested check
        provider_obj = content.get("provider")
        publisher = (provider_obj.get("displayName") if isinstance(provider_obj, dict) else None) or item.get("publisher")
        
        # Date - try content.pubDate (ISO string), then providerPublishTime (epoch)
        pub_date_str = content.get("pubDate") or content.get("displayTime")
        pub_time = item.get("providerPublishTime")
        
        # If we only have pub_time (epoch), convert to ISO string
        if not pub_date_str and pub_time:
            try:
                pub_date_str = datetime.datetime.fromtimestamp(pub_time).isoformat()
            except:
                pass
        
        formatted.append({
            "title": title,
            "link": link,
            "publisher": publisher,
            "source": publisher, # Legacy
            "pubDate": pub_date_str,
            "providerPublishTime": pub_time or 0
        })
    return formatted

@app.route('/api/news/<symbol>')
def api_stock_news(symbol):
    """API endpoint for stock news, expected by some frontend components."""
    try:
        ticker = yf.Ticker(symbol)
        news = ticker.news
        if not news:
            return jsonify([])
        return jsonify(parse_yf_news(news))
    except Exception as e:
        print(f"Error fetching news for {symbol}: {e}")
        return jsonify({"error": str(e)}), 500


# News route for symbol
@app.route('/news/<symbol>')
def stock_news_page(symbol):
    return render_template('stock_news.html', symbol=symbol)


def fetch_and_update_data_wrapper():
    """Wrapper to maintain background status reporting if needed, 
    or just call fetch_and_save directly."""
    global refresh_status
    try:
        with refresh_lock:
            refresh_status["is_running"] = True
            refresh_status["status"] = "running"
            refresh_status["message"] = "Refreshing S&P 500 data (PT schedule)..."
        
        count = fetch_and_save()
        
        with refresh_lock:
            refresh_status["is_running"] = False
            refresh_status["status"] = "success"
            refresh_status["message"] = f"Successfully refreshed data."
    except Exception as e:
        with refresh_lock:
            refresh_status["is_running"] = False
            refresh_status["status"] = "error"
            refresh_status["message"] = f"Error: {str(e)}"

@app.route('/api/refresh')
def api_refresh():
    """Trigger data refresh (Asynchronous)."""
    global refresh_status
    force = request.args.get('force', 'false').lower() == 'true'
    
    # Check if this is a Cloud Scheduler trigger (optional security)
    is_scheduler = request.headers.get('X-Cloud-Scheduler') == 'true'
    
    with refresh_lock:
        if refresh_status["is_running"]:
            return jsonify({"status": "error", "message": "Refresh already in progress."}), 400

    if not force and not is_scheduler:
        data = load_sp500_data()
        if data and len(data) > 0:
            last_updated = data[0].get('LastUpdated')
            if last_updated:
                # Basic check: if updated today, skip unless forced
                last_date = last_updated.split(' ')[0]
                pt_today = datetime.datetime.now(ZoneInfo("America/Los_Angeles")).date()
                if last_date == pt_today.isoformat():
                    return jsonify({"status": "success", "message": "Data is already up to date."}), 200

    # In Cloud Run, background threads freeze when the HTTP request finishes.
    # A 2.5-hour task cannot run here. Manual trigger should only be used locally 
    # or via GitHub Actions workflow_dispatch.
    if os.environ.get('K_SERVICE'):  # Cloud Run specific env var
        return jsonify({
            "status": "error", 
            "message": "Manual refresh is disabled on the live site. Data refreshes automatically daily at 5 PM PT via GitHub Actions."
        }), 400

    # Fallback for local execution
    thread = threading.Thread(target=fetch_and_update_data_wrapper)
    thread.daemon = True
    thread.start()
    return jsonify({"status": "success", "message": "Data refresh started in background."}), 202

@app.route('/api/refresh_status')
def api_refresh_status():
    """Return the current refresh status with local time context."""
    data = load_sp500_data()
    last_updated = "Unknown"
    if data and len(data) > 0:
        last_updated = data[0].get('LastUpdated', "Unknown")
    
    return jsonify({
        **refresh_status,
        "last_updated_raw": last_updated,
        "is_scheduler": request.headers.get('X-Cloud-Scheduler') == 'true'
    })

def _get_clean_filing_lines(url):
    """Fetch a SEC filing, strip XBRL/metadata, return clean text lines."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code != 200:
        return []
    soup = BeautifulSoup(response.text, 'html.parser')
    # Remove XBRL inline tags
    for tag in soup.find_all(True):
        if ':' in tag.name:
            tag.decompose()
    text = soup.get_text(separator='\n')
    lines = text.split('\n')
    clean = []
    for line in lines:
        s = line.strip()
        if not s or len(s) < 3:
            continue
        # Skip XBRL metadata lines
        if s.startswith('http://') or s.startswith('false') or s.startswith('true'):
            continue
        if 'xbrli:' in s or 'us-gaap/' in s or ':pure' in s or 'iso4217' in s:
            continue
        clean.append(s)
    return clean

def _find_section(lines, heading_patterns, stop_patterns, max_chars=12000):
    """Find a filing section by its heading, validating real content follows."""
    candidates = []
    for i, line in enumerate(lines):
        s = line.strip()
        for pat in heading_patterns:
            if re.match(pat, s, re.IGNORECASE):
                # Verify substantial text follows (not a ToC entry)
                text_after = ''
                for j in range(1, min(20, len(lines) - i)):
                    text_after += lines[i + j].strip() + ' '
                if len(text_after) > 200:
                    candidates.append((i + 1, len(text_after)))
                break
    if not candidates:
        return ""
    # Pick the candidate with the most text after it
    best_idx = max(candidates, key=lambda x: x[1])[0]
    collected = []
    total_chars = 0
    for i in range(best_idx, len(lines)):
        s = lines[i].strip()
        for spat in stop_patterns:
            if re.match(spat, s, re.IGNORECASE) and len(s) < 100:
                return '\n'.join(collected)
        collected.append(s)
        total_chars += len(s)
        if total_chars > max_chars:
            break
    return '\n'.join(collected)

def _extract_points(text, keywords, limit=5, exclude_phrases=None):
    """Extract meaningful sentences containing keywords, filtering boilerplate."""
    if not text:
        return []
    sentences = re.split(r'(?<=[.!?])\s+', text)
    results = []
    seen = set()
    skip_phrases = [
        'check mark', 'check one', 'accelerated filer', 'emerging growth',
        'exchange act', 'rule 12b', 'form 10-k', 'form 10-q', 'section 13',
        'table of contents', 'page number', 'incorporated by reference',
        'not applicable', 'see note', '☐', '☑', '☒', '¨'
    ]
    if exclude_phrases:
        skip_phrases.extend(exclude_phrases)
    for sent in sentences:
        sent = sent.strip()
        if len(sent) < 50 or len(sent) > 500:
            continue
        lower = sent.lower()
        if any(bp in lower for bp in skip_phrases):
            continue
        # Skip lines that are mostly numbers/tables
        alpha_ratio = sum(c.isalpha() for c in sent) / max(len(sent), 1)
        if alpha_ratio < 0.5:
            continue
        if any(re.search(r'\b' + kw + r'\b', sent, re.IGNORECASE) for kw in keywords):
            if lower not in seen:
                seen.add(lower)
                results.append(sent)
        if len(results) >= limit:
            break
    return results

def extract_comprehensive_insights(symbol):
    """Scrape and parse the latest SEC filing (10-K or 10-Q) using section-aware extraction."""
    try:
        ticker = yf.Ticker(symbol)
        filings = ticker.sec_filings
        if not filings:
            return None

        # Find the latest 10-K or 10-Q
        latest_filing = None
        for f in filings:
            if f['type'] in ['10-K', '10-Q']:
                latest_filing = f
                break
        if not latest_filing:
            return None

        url = latest_filing.get('exhibits', {}).get(latest_filing['type']) or latest_filing.get('edgarUrl')
        if not url:
            return None

        lines = _get_clean_filing_lines(url)
        if not lines:
            return None

        # --- Extract key sections ---
        # Business (Item 1)
        business_text = _find_section(
            lines,
            [r'^BUSINESS\s*OVERVIEW', r'^OUR\s*BUSINESS', r'^GENERAL\s*$',
             r'^BUSINESS\s*$', r'^BUSINESS\s+DESCRIPTION'],
            [r'^RISK\s+FACTORS', r'^PROPERTIES', r'^LEGAL\s+PROCEEDINGS',
             r'^UNRESOLVED', r'^CYBERSECURITY'],
            max_chars=12000
        )

        # Risk Factors (Item 1A)
        risk_text = _find_section(
            lines,
            [r'^RISK\s+FACTORS\.?$', r'^STRATEGIC\s+RISKS'],
            [r'^UNRESOLVED\s+STAFF', r'^PROPERTIES', r'^CYBERSECURITY',
             r'^LEGAL\s+PROCEEDINGS', r'^MINE\s+SAFETY',
             r"^MANAGEMENT.S\s+DISCUSSION"],
            max_chars=15000
        )

        # MD&A (Item 7) — try multiple heading patterns
        mda_text = _find_section(
            lines,
            [r'^CONSOLIDATED\s+RESULTS', r'^OVERVIEW\s*$',
             r'^RESULTS\s+OF\s+OPERATIONS', r'^SEGMENT\s+OPERATIONS',
             r'^EXECUTIVE\s+SUMMARY'],
            [r'^QUANTITATIVE', r'^FINANCIAL\s+STATEMENTS',
             r'^CHANGES\s+IN', r'^CONTROLS', r'^CRITICAL\s+ACCOUNTING'],
            max_chars=15000
        )

        combined = business_text + '\n' + mda_text
        # Full text for tariff search (tariff mentions can appear anywhere)
        full_text = '\n'.join(lines)

        # Negative-sentiment phrases to exclude from opportunities
        neg_exclude = [
            'adversely', 'negatively impact', 'negatively affect',
            'decline', 'could affect', 'may affect',
            'uncertain', 'threat', 'challenged', 'headwind',
            'disruption', 'decrease our revenue', 'increase our costs',
            'could also adversely', 'nonexistent', 'compliance costs',
            'material adverse', 'regulations or changes', 'subject to risk',
            'penalties', 'litigation', 'loss of', 'damage to',
            'bad actors', 'social engineering', 'cybersecurity',
            'data breach', 'ransomware', 'vulnerability'
        ]

        # --- Extract insights from the correct sections ---
        insights = {
            "opportunities": _extract_points(
                combined,
                ['growth', 'opportunity', 'expansion', 'innovation',
                 'strategic', 'new product', 'new market', 'demand',
                 'ramp', 'increase', 'momentum', 'invest', 'capacity',
                 'backlog', 'order book', 'delivered', 'revenue grew',
                 'profit grew', 'margin improvement'],
                limit=5,
                exclude_phrases=neg_exclude
            ),
            "risks": _extract_points(
                risk_text,
                ['could adversely', 'may adversely', 'uncertainty',
                 'challenge', 'decline', 'volatility', 'material adverse',
                 'disruption', 'failure', 'negatively impact'],
                limit=5
            ),
            "tariff_impact": _extract_points(
                full_text,
                ['tariff', 'trade policy', 'import duty',
                 'trade restriction', 'trade war', 'customs duty'],
                limit=5
            ),
            "customers": _extract_points(
                combined,
                ['customer', 'client', 'contract', 'backlog',
                 'order', 'airline', 'defense', 'government'],
                limit=5
            ),
            "one_time": _extract_points(
                combined + '\n' + risk_text,
                ['one-time', 'non-recurring', 'impairment',
                 'restructuring', 'settlement', 'divestiture',
                 'write-off', 'gain on sale', 'separation'],
                limit=5
            )
        }
        return insights
    except Exception as e:
        print(f"Error extracting SEC insights for {symbol}: {e}")
        return None

@app.route('/api/stock_details/<symbol>')
def get_stock_details(symbol):
    """Fetch comprehensive details for a specific stock."""
    symbol = symbol.upper()
    try:
        ticker = yf.Ticker(symbol)
        
        # 1. Financials
        info = ticker.info
        financials = ticker.financials
        quarterly_financials = ticker.quarterly_financials
        
        # Calculate margins and growth
        revenue = info.get('totalRevenue')
        net_income = info.get('netIncomeToCommon')
        profit_margin = info.get('profitMargins')
        
        # 2. News
        formatted_news = []
        try:
            news_raw = ticker.news
            if news_raw:
                formatted_news = parse_yf_news(news_raw[:5])
        except Exception as ne:
            print(f"Error parsing news for {symbol}: {ne}")
        
        # 3. SEC 'Insights' (Dynamic extraction from actual filings)
        business_summary = info.get('longBusinessSummary', "N/A")
        dividend_yield = info.get('dividendYield')
        forward_pe = info.get('forwardPE')
        trailing_pe = info.get('trailingPE')
        
        sec_insights = extract_comprehensive_insights(symbol)
        
        # Calculate valuation score
        valuation_score = "Fair Value"
        if forward_pe and trailing_pe:
            if forward_pe < trailing_pe * 0.8:
                valuation_score = "Undervalued"
            elif forward_pe > trailing_pe * 1.2:
                valuation_score = "Overvalued"

        # Fallback to defaults or simulated logic if scraping fails
        if not sec_insights:
            insights = {
                "opportunities": [f"Expansion in {info.get('sector', 'its sector')} and recovery in margins."],
                "threats": ["Increasing competition and potential macro headwinds."],
                "revenue_drivers": ["Growth in core services and volume increases."],
                "tariff_impact": ["Subject to global trade policies but manageable through regional supply chains."],
                "customers": ["Diverse customer base across multiple geographic regions."],
                "one_time": ["N/A - No major one-time events identified in summary."],
                "valuation": valuation_score
            }
        else:
            sec_insights["valuation"] = valuation_score
            # Map labels for consistency
            insights = {
                "opportunities": sec_insights["opportunities"] if sec_insights["opportunities"] else ["No specific growth opportunities identified in latest filing."],
                "threats": sec_insights["risks"] if sec_insights["risks"] else ["No highlighted risks found in primary text blocks."],
                "revenue_drivers": sec_insights["customers"][:2] + sec_insights["opportunities"][:1], # Blend some drivers
                "tariff_impact": sec_insights["tariff_impact"] if sec_insights["tariff_impact"] else ["No significant mention of tariff impact in the latest filing."],
                "customers": sec_insights["customers"] if sec_insights["customers"] else ["Publicly traded and serving global markets."],
                "one_time": sec_insights["one_time"] if sec_insights["one_time"] else ["No major one-time events mentioned in recent results."],
                "valuation": valuation_score
            }

        # Format financials for the chart
        fin_summary = []
        if financials is not None and not financials.empty:
            try:
                dates = financials.columns[:4] # Last 4 years
                for d in dates:
                    # Check if requested indices exist in financials
                    rev = financials.loc['Total Revenue', d] if 'Total Revenue' in financials.index and d in financials.columns else 0
                    ni = financials.loc['Net Income', d] if 'Net Income' in financials.index and d in financials.columns else 0
                    
                    fin_summary.append({
                        "date": d.strftime('%Y') if hasattr(d, 'strftime') else str(d),
                        "revenue": float(rev) if pd.notnull(rev) else 0,
                        "net_income": float(ni) if pd.notnull(ni) else 0
                    })
            except Exception as fe:
                print(f"Error formatting financials for {symbol}: {fe}")

        return jsonify({
            "ticker": symbol,
            "company_name": info.get('longName', symbol),
            "summary": business_summary,
            "metrics": {
                "market_cap": info.get('marketCap'),
                "pe_ratio": info.get('trailingPE'),
                "forward_pe": forward_pe,
                "profit_margin": profit_margin,
                "revenue": revenue,
                "net_income": net_income,
                "dividend_yield": dividend_yield
            },
            "financials": fin_summary,
            "news": formatted_news,
            "insights": insights
        })
    except Exception as e:
        print(f"Error fetching stock details for {symbol}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/global_news')
@app.route('/api/news/market')
def get_global_news():
    """Fetch global market news."""
    try:
        # Use SPY news as a proxy for global market news (^GSPC often returns empty)
        mkt = yf.Ticker("SPY")
        news_raw = mkt.news
        if not news_raw:
            return jsonify([])
        return jsonify(parse_yf_news(news_raw[:10]))
    except Exception as e:
        print(f"Error fetching global news: {e}")
        return jsonify([]), 200 # Return empty list rather than 500 for better UI stability

@app.route('/api/earnings_calendar')
def api_earnings_calendar():
    """Return stocks sorted by their upcoming earnings date."""
    data = load_sp500_data()
    if not data:
        return jsonify([])
    
    # Filter for stocks with earnings date and sort them
    earnings_stocks = []
    for s in data:
        # We might need to normalize the date format
        edate = s.get('EarningsDate')
        if edate:
            earnings_stocks.append(s)
    
    # Simple sort by date string (YYYY-MM-DD or similar)
    # If EarningsDate is an epoch or different format, we'd handle it here
    def get_date_val(x):
        d = x.get('EarningsDate')
        if isinstance(d, (int, float)):
            return d
        return str(d)

    earnings_stocks.sort(key=get_date_val)
    
    return jsonify(sanitize_data(earnings_stocks))

if __name__ == '__main__':
    app.run(debug=True)
