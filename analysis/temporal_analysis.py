import pandas as pd
import matplotlib.pyplot as plt
import os

import pandas as pd
from datetime import datetime

def monthly_duration(data):
    # 1. Load data and initial time cleaning
    df = pd.DataFrame(data) 
    
    # ------------------ Existing Time String Code --------------------
    # Ensure time fields are strings and handle potential 'null' or None values 
    df['time_start'] = df['time_start'].astype(str).str.replace(r'[:.]', '', regex=True).str.zfill(4)
    df['time_end'] = df['time_end'].astype(str).str.replace(r'[:.]', '', regex=True).str.zfill(4)
    
    # ⚠️ Filtering out 'Unknown' to prevent errors in date parsing
    # We will use .dropna later to safely handle missing data.
    df = df[df['time_end'] != 'Unknown'].copy() # Use .copy() to avoid SettingWithCopyWarning
    
    # 🟢 STEP 2: Create Datetime Columns for Calculation (RE-INSERTED)
    df['start_dt'] = pd.to_datetime(
        df['year'].astype(str) + '-' + df['month'].astype(str) + '-' + df['day'].astype(str) + ' ' + 
        df['time_start'].str[:2] + ':' + df['time_start'].str[2:], 
        errors='coerce' # Set invalid dates/times to NaT
    )

    df['end_dt'] = pd.to_datetime(
        df['year'].astype(str) + '-' + df['month'].astype(str) + '-' + df['day'].astype(str) + ' ' + 
        df['time_end'].str[:2] + ':' + df['time_end'].str[2:],
        errors='coerce'
    )
    
    # 🟢 STEP 3: Calculate Duration (RE-INSERTED)
    # This creates the 'duration' column needed for aggregation.
    df['duration'] = (df['end_dt'] - df['start_dt']).dt.total_seconds() / 3600 # Convert to hours

    # 4. Filter Invalid Duration Rows
    # Safely remove rows where time parsing failed, resulting in NaN duration.
    df = df.dropna(subset=['duration'])
    
    # 5. Aggregate by Year and Month
    # This line now runs successfully because 'duration' exists on the filtered df.
    monthly_summary = df.groupby(['year', 'month'])['duration'].sum().reset_index()
    monthly_summary = monthly_summary.rename(columns={'duration': 'Total Duration (Hours)'})
    
    # 6. Optional: Sort and Create SortableDate
    monthly_summary['month'] = monthly_summary['month'].astype(int)
    monthly_summary = monthly_summary.sort_values(by=['year', 'month']).reset_index(drop=True)
    
    # 🟢 NEW STEP: Create a simple, clean datetime object for the x-axis index
    monthly_summary['SortableDate'] = pd.to_datetime(
        monthly_summary['year'].astype(str).str.split(' ').str[0] + '-' + 
        monthly_summary['month'].astype(str).str.zfill(2)
    )
    
    return monthly_summary



def plot_monthly_duration_line(monthly_summary_df):
    
    # 1. Prepare X-axis Labels (Keep this as is, using the imputed labels)
    monthly_summary_df['MonthLabel'] = (
        monthly_summary_df['year'].astype(str) + '-' + 
        monthly_summary_df['month'].astype(str).str.zfill(2)
    )
    
    # 2. Sort the DataFrame by the new column to ensure chronological order
    # (This is redundant if sorted in monthly_duration, but safer here)
    monthly_summary_df = monthly_summary_df.sort_values('SortableDate').reset_index(drop=True)
    
    # 3. Create Plot
    plt.figure(figsize=(14, 7))
    
    # 🟢 KEY CHANGE: Use 'SortableDate' for the plot data, but 'MonthLabel' for tick labels
    plt.plot(
        monthly_summary_df['SortableDate'], # Use the clean datetime object for plotting
        monthly_summary_df['Total Duration (Hours)'],
        marker='o',
        linestyle='-',
        color='darkorange',
        linewidth=2
    )
    
    # 4. Customize and Label
    plt.title('Keskeytysten kokonaiskesto kuukausittain', fontsize=16)
    plt.xlabel('Vuosi-Kuukausi', fontsize=12)
    plt.ylabel('Kokonaiskesto (tunteina)', fontsize=12)

    
    # 🟢 Set the tick *locations* based on the 'SortableDate' and the *labels* based on 'MonthLabel'
    plt.xticks(
        ticks=monthly_summary_df['SortableDate'], # Where the ticks should appear
        labels=monthly_summary_df['MonthLabel'], # What the labels should say
        rotation=45, 
        ha='right', 
        fontsize=10
    )
    
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    # 5. Save the figure
    output_dir = 'reports/charts'
    os.makedirs(output_dir, exist_ok=True)
    chart_path = os.path.join(output_dir, 'monthly_duration_line_chart_imputed.png')
    plt.savefig(chart_path)
    plt.close()
    
    print(f"\nLine chart saved to: {chart_path}")
