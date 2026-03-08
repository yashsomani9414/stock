import json
import pandas as pd
import os
import datetime
from zoneinfo import ZoneInfo
from fetch_sp500 import calculate_score, DATA_FILE

def recalculate():
    if not os.path.exists(DATA_FILE):
        print("Data file not found.")
        return

    with open(DATA_FILE, 'r') as f:
        data = json.load(f)

    if not data:
        print("No data in file.")
        return

    df = pd.DataFrame(data)
    
    # Calculate medians from the existing data
    pe_med = df.groupby('Sector')['P/E Ratio'].median().to_dict()
    vol_med = df.groupby('Sector')['6M Volatility'].median().to_dict()
    
    # Create a history map (the data itself acts as history for consecutive low days etc)
    # But since we are recalculating the *current* state, we can pass None or the row itself
    # Actually, calculate_score takes (row, sector_pe_medians, sector_vol_medians, history=None)
    # history is used for prev_score and ConsecutiveLowDays
    
    print("Recalculating scores and decisions...")
    
    new_results = []
    for _, row in df.iterrows():
        # Pass the row itself as history to preserve ConsecutiveLowDays if needed, 
        # but the request is specifically to fix the logic jump.
        score, decision, low_days = calculate_score(row, pe_med, vol_med, history=row.to_dict())
        row_dict = row.to_dict()
        row_dict['Score'] = score
        row_dict['Trade Decision'] = decision
        row_dict['ConsecutiveLowDays'] = low_days
        new_results.append(row_dict)

    # Convert back to clean JSON (NaN -> None)
    output_df = pd.DataFrame(new_results)
    
    # Update LastUpdated timestamp in PT
    pacific_time = datetime.datetime.now(ZoneInfo("America/Los_Angeles"))
    output_df['LastUpdated'] = pacific_time.strftime("%Y-%m-%d %H:%M:%S")

    output = output_df.where(pd.notnull(output_df), None).to_dict(orient='records')
    
    with open(DATA_FILE, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"Recalculation complete. Updated {len(output)} records.")
    
    # Verify UPS specifically
    ups = next((s for s in output if s['Symbol'] == 'UPS'), None)
    if ups:
        print(f"UPS Update: Score={ups['Score']}, Decision={ups['Trade Decision']}")
    
    rost = next((s for s in output if s['Symbol'] == 'ROST'), None)
    if rost:
        print(f"ROST Update: Score={rost['Score']}, Decision={rost['Trade Decision']}")

if __name__ == "__main__":
    recalculate()
