from flask import Flask, render_template, jsonify
import pandas as pd
import datetime
import json
import os

app = Flask(__name__)

# Path to JSON data
DATA_FILE = 'sp500_data.json'

def load_sp500_data():
    """Load S&P 500 stock data from JSON file."""
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                data = json.load(f)
            return data
        except Exception as e:
            print(f"Error loading {DATA_FILE}: {e}")
            return []
    return []

def calculate_sector_data(data):
    """Aggregate data by sector."""
    if not data:
        return []
    
    df = pd.DataFrame(data)
    if df.empty or 'Sector' not in df.columns:
        return []

    sectors = []
    for sector_name, group in df.groupby("Sector"):
        if sector_name == "N/A":
            continue

        total_mcap = sum([x for x in group['Market Cap'] if x is not None])

        # Weighted P/E
        pe_group = group.dropna(subset=['P/E Ratio', 'Market Cap'])
        if not pe_group.empty and total_mcap > 0:
            weighted_pe = (pe_group['P/E Ratio'] * pe_group['Market Cap']).sum() / pe_group['Market Cap'].sum()
        else:
            weighted_pe = None

        avg_50d = group['50D MA'].mean()
        avg_200d = group['200D MA'].mean()

        sectors.append({
            "Sector": sector_name,
            "Market Cap": total_mcap,
            "Weighted P/E": round(weighted_pe, 2) if weighted_pe else None,
            "Avg 50D MA": round(avg_50d, 2) if avg_50d else None,
            "Avg 200D MA": round(avg_200d, 2) if avg_200d else None,
            "Stock Count": len(group)
        })
    return sectors

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/sector')
def sector_page():
    return render_template('sector.html')

@app.route('/api/data')
def api_data():
    data = load_sp500_data()
    return jsonify(data)

@app.route('/api/sector_data')
def api_sector_data():
    data = load_sp500_data()
    sector_data = calculate_sector_data(data)
    return jsonify(sector_data)

@app.route('/api/history/<symbol>')
def api_history(symbol):
    """Return historical prices from cached JSON (optional: could extend to CSV if stored)."""
    data = load_sp500_data()
    stock = next((x for x in data if x['Symbol'] == symbol), None)
    if stock:
        return jsonify({
            'symbol': symbol,
            'Price': stock.get('Price'),
            '50D MA': stock.get('50D MA'),
            '200D MA': stock.get('200D MA'),
            'Trend Strength': stock.get('Trend Strength')
        })
    return jsonify({'error': 'Symbol not found'}), 404

@app.route('/market-news')
def market_news_page():
    return render_template('market_news.html')

@app.route('/news/<symbol>')
def stock_news_page(symbol):
    return render_template('stock_news.html', symbol=symbol)

if __name__ == '__main__':
    app.run(debug=True)
