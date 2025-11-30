import pandas as pd
import matplotlib.pyplot as plt
import os
import json

def location_frequency(data):
    """
    Analyzes the frequency of outages per location and returns the summary.
    
    Args:
        data (list): List of dictionaries (your clean outage data).
        
    Returns:
        pd.DataFrame: A DataFrame showing the count of outages per location.
    """
    
    df = pd.DataFrame(data)
    
    # 1. Count the frequency of each unique location
    # Use .value_counts() on the 'location' column
    location_summary = df['location'].value_counts().reset_index()
    
    # 2. Rename columns for clarity
    location_summary.columns = ['Location', 'Outage Count']
    
    # 3. Sort by count, descending
    location_summary = location_summary.sort_values(by='Outage Count', ascending=False)
    
    return location_summary


def plot_location_bar_chart(location_summary_df):
    """
    Generates and saves a bar chart showing the frequency of outages by location.
    """
    
    plt.figure(figsize=(10, 6))
    
    # Create the horizontal bar chart
    plt.barh(
        location_summary_df['Location'], # Y-axis (cities)
        location_summary_df['Outage Count'], # X-axis (counts)
        color='teal'
    )
    
    # Customize and Label
    plt.title('Keskeytysten lukumäärä paikkakunnittain', fontsize=16)
    plt.xlabel('Keskeytysten kokonaismäärä', fontsize=12)
    plt.ylabel('Paikkakunta', fontsize=12)

    
    plt.gca().invert_yaxis() # Display the highest count at the top
    plt.tight_layout()
    
    # Save the figure
    output_dir = 'reports/charts'
    os.makedirs(output_dir, exist_ok=True)
    
    chart_path = os.path.join(output_dir, 'location_frequency_bar_chart.png')
    plt.savefig(chart_path)
    plt.close()
    
    print(f"\nLocation frequency bar chart saved to: {chart_path}")
