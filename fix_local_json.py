import json
import datetime
import pandas as pd

def calculate_score_v3_1(row, sector_pe_medians, sector_vol_medians):
    try:
        score = 0
        
        # A) TREND (0–35 points)
        price = row.get('Price') or 0
        ma50 = row.get('50D MA') or 0
        ma200 = row.get('200D MA') or 0
        trend_strength = row.get('Trend Strength') or 0
        
        if price >= ma200:
            if price > ma50: score += 8
            if ma50 > ma200: score += 8
            if price > ma200: score += 8
            if trend_strength > 0: score += 11
        else:
            score += 0 # Trend points = 0 if below 200D MA
        
        # B) MOMENTUM (0–25 points)
        ret5d = row.get('5D Return') or 0
        ret1m = row.get('1M Return') or 0
        ret6m = row.get('6M Return') or 0
        
        if ret6m > 0: score += 5
        if ret1m > 0: score += 5
        if ret5d > 0: score += 5
        if ret6m > ret1m > ret5d: score += 10
        if ret5d < 0 and ret1m > 0: score -= 5
        
        # C) VOLUME (0–20 points)
        ret1d = row.get('1D Return') or 0
        vol1d_ratio = 1 + (row.get('Vol Change 1D') or 0) / 100
        vol5d_ratio = 1 + (row.get('Vol Change 5D') or 0) / 100
        
        if vol5d_ratio >= 1.2: score += 5
        if vol1d_ratio >= 1.5: score += 10
        
        # Rising volume on up days
        if ret1d > 0 and (row.get('Vol Change 1D') or 0) > 0:
            score += 5
        # Down-day volume >= 2x avg
        if ret1d < 0 and vol1d_ratio >= 2.0:
            score -= 10
        
        # D) RISK & VALUATION (0–20 points)
        mcap = row.get('Market Cap') or 0
        if mcap > 10e9: score += 5
        
        pe = row.get('P/E Ratio')
        sector = row.get('Sector')
        median_pe = sector_pe_medians.get(sector)
        
        if pe and median_pe:
            if pe < median_pe:
                score += 5
            elif pe > median_pe and (ret1m > 0 or ret6m > 0):
                score += 3
        
        # NEW: Volatility (6M) < sector median
        vol6m = row.get('6M Volatility')
        median_vol = sector_vol_medians.get(sector)
        if vol6m and median_vol and vol6m < median_vol:
            score += 7
        
        final_points = round(max(0, score))

        # --- DECISION STEPS (1-7) ---
        
        # STEP 1: UNIVERSAL FILTER (HARD REJECTION)
        avg_vol_20d = row.get('Volume') / (1 + (row.get('Vol Change 1D') or 0) / 100) if row.get('Volume') and row.get('Vol Change 1D') is not None else 0
        if mcap < 2e9 or avg_vol_20d < 500000 or price < 10:
            return final_points, "Rejected – Universal Filter"

        # STEP 2: HARD SELL (CAPITAL PROTECTION)
        # Rules: Price < 200D MA, Down-day vol >= 2x, Trend strength < 0, Score < 45
        if price < ma200 or (ret1d < 0 and vol1d_ratio >= 2.0) or trend_strength < 0 or final_points < 45:
            return final_points, "Sell"

        # STEP 3: EARNINGS RISK FREEZE
        edate_str = row.get('EarningsDate')
        if edate_str:
            try:
                edate = datetime.datetime.strptime(edate_str, '%Y-%m-%d').date()
                today = datetime.date.today()
                days_diff = (edate - today).days
                if -1 <= days_diff <= 7:
                    return final_points, "Hold"
            except: pass

        # STEP 4: REDUCE (RISK CONTROL)
        # Rules: Score dropped (proxy < 65) OR BOTH 5D < 0 AND 1M < 0
        if (final_points < 65) or (ret5d < 0 and ret1m < 0):
            return final_points, "Reduce"

        # STEP 5: STRONG BUY (ADD AGGRESSIVELY)
        if (final_points >= 80 and price > ma50 and price > ma200 and vol5d_ratio >= 1.2 and ret6m > 0):
            edate_dist = 999
            if edate_str:
                try:
                    edate_dist = (datetime.datetime.strptime(edate_str, '%Y-%m-%d').date() - datetime.date.today()).days
                except: pass
            if edate_dist > 7:
                return final_points, "Strong Buy"

        # STEP 6: BUY (SMALL / ADD CAUTIOUSLY)
        if (70 <= final_points <= 79 and price > ma50 and price > ma200 and ret6m > 0):
            edate_dist = 999
            if edate_str:
                try:
                    edate_dist = (datetime.datetime.strptime(edate_str, '%Y-%m-%d').date() - datetime.date.today()).days
                except: pass
            if edate_dist > 7:
                return final_points, "Buy (Small)"

        # STEP 7: HOLD (DEFAULT)
        return final_points, "Hold"

    except Exception as e:
        print(f"Error scoring {row.get('Symbol')}: {e}")
        return 0, "ERROR"

def main():
    path = "sp500_data.json"
    print(f"Loading {path}...")
    with open(path, "r") as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    
    # Calculate medians from current data
    print("Calculating sector medians...")
    sector_pe_medians = df.groupby('Sector')['P/E Ratio'].median().to_dict()
    sector_vol_medians = df.groupby('Sector')['6M Volatility'].median().to_dict()
    
    print("Applying new v3.1 scoring logic locally...")
    results = df.apply(lambda r: calculate_score_v3_1(r, sector_pe_medians, sector_vol_medians), axis=1)
    
    df['Score'] = [r[0] for r in results]
    df['Trade Decision'] = [r[1] for r in results]
    df['LastUpdated'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " (Local Refix)"
    
    output = df.to_dict(orient="records")
    
    print(f"Saving updated data back to {path}...")
    with open(path, "w") as f:
        json.dump(output, f, indent=2)
    print("Done! Local fix applied.")

if __name__ == "__main__":
    main()
