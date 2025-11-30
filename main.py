import argparse
import json
import numpy as np
import pandas as pd
from api.weather import weatherApi
from generators.spider import scrape_outage_data
from utils.file_utils import save_to_json
from processors.json_processor import raw_processor, save_to_interim_json
from processors.json_day_processor import find_weekday, extract_all, rejected_entries
from analysis.word_frequency import get_field_word_frequency
from processors.raw_json_processor import raw_processor1
from processors.json_interim_processor import filter_data
from analysis.temporal_analysis import monthly_duration , plot_monthly_duration_line
from  analysis.geograpgical_analysis import location_frequency, plot_location_bar_chart
from analysis.cause_location import plot_cause_by_location, analyze_cause_by_location
from generators.realtime_generator import outage_stream



def argparse_extract_all():
    # Open and load outage data from JSON file
    with open('data/raw/outages/outage_data.json', 'r', encoding='utf-8') as file:
        outage_data = json.load(file)  # This should be a list of strings
    
    updated_data = []  # List to store updated data with extracted fields
    
    # Loop through each entry in the outage data
    for entry in outage_data:
        result = extract_all(entry)  # Pass the string directly to extract_all
        if result:
            # If extraction is successful, create a new dictionary with the message and extracted data
            updated_entry = {
                "weekday": result["weekday"],  # Add extracted weekday
                "date": result["date"]  # Add extracted date
            }
            updated_data.append(updated_entry)  # Add the updated dictionary to the list
    
    # After processing all entries, print the updated list
    print(f"Processed outage data: {updated_data}")
    
    # Save the updated data to a new JSON file
    path = "data/interim/outage_all.json"
    save_to_json(updated_data, path)  # Save the new data structure

def argparse_day_processor():
    with open('data/raw/outages/outage_data.json', 'r', encoding='utf-8') as file:
        outage_data = json.load(file)
    
    days = raw_processor1(outage_data)
    print(f"{days}, length: {len(days)}")
    path="data/interim/outage_days.json"
    save_to_json(days, path)

    
    """Save the rejected entries to a JSON file."""
    path = "data/interim/rejected_days.json"  # Path to save the rejected entries
    with open(path, 'w', encoding='utf-8') as file:
        json.dump(rejected_entries, file, ensure_ascii=False, indent=4)
    print(f"Rejected entries saved to {path}")

def argparse_word_frequency():
    file_path = 'data/interim/outage_data.json'
    target_fields = [
        "weekday", 
        "day", 
        "month", 
        "year", 
        "time_start", 
        "time_end", 
        "tags", 
        "location"
        ]
    get_field_word_frequency(file_path, target_fields)

def argparse_realtime_data():
    # Load JSON data
    with open('data/processed/outage_data.json', 'r', encoding='utf-8') as f:
        data = json.load(f)  # data is a list of dicts

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Stream outages
    for outage in outage_stream(df, delay=0.1):
        print(outage)

def argparse_data_analysis():
    with open('data/processed/outage_data.json', 'r', encoding='utf-8') as file:
        outage_data = json.load(file)

        analyzed_data_df = monthly_duration(outage_data)

        plot_monthly_duration_line(analyzed_data_df)
        path = "reports/monthly_duration_outage_data.csv"
        analyzed_data_df.to_csv(path, index=False)

        location_summary_df = location_frequency(outage_data)
    
        # Generate the Location Bar Chart
        plot_location_bar_chart(location_summary_df)
    
        # Save the location summary to CSV
        location_summary_df.to_csv('reports/location_outage_summary.csv', index=False)

        cause_matrix_df = analyze_cause_by_location(outage_data) 
        plot_cause_by_location(cause_matrix_df) 
        cause_matrix_df.to_csv('reports/cause_by_location_matrix.csv')

def argparse_interim_processor():
    with open('data/interim/outage_data.json', 'r', encoding='utf-8') as file:
        outage_data = json.load(file)

        processed_data = filter_data(outage_data)
        path = "data/processed/outage_data.json"
        save_to_json(processed_data, path)

# Function to process raw data (load, process, and save)
def argparse_raw_processor():
    # Load the raw outage data from file
    with open('data/raw/outages/outage_data.json', 'r', encoding='utf-8') as file:
        outage_data = json.load(file)
    
    # Process the data
    # List of canonical cities
    
    canonical_cities = [
    "Iisalmi", "Joensuu", "Joroinen", "Juankoski", "Karttula", "Keitele", 
    "Kiuruvesi", "Lapinlahti", "Leppävirta", "Maaninka", "Nilsiä", "Pieksämäki", 
    "Pielavesi", "Rautalampi", "Siilinjärvi", "Suonenjoki", "Tahkovuori", 
    "Varpaisjärvi", "Vuorela", "Toivala",
    ]
    interim_data = raw_processor(outage_data, canonical_cities)

    # Save processed data as interim
    save_to_interim_json(interim_data, 'data/interim/outage_data.json')

# Main function for full data processing
def main():
    # Set up argparse to handle command-line arguments
    parser = argparse.ArgumentParser(description="Prosessoi häiriödatan")
    
    # Add --process argument to run only the raw processing
    parser.add_argument('--process', action='store_true', help='Processes the raw data')
    parser.add_argument('--filter', action='store_true', help='Filters missing interim data')
    parser.add_argument('--analyze', action='store_true', help='Analyze processes data')
    parser.add_argument('--generate', action='store_true', help='Generate realtime data from processed data')
    parser.add_argument('--display', action='store_true', help='Display analytics from processed data')

    # Parse command-line arguments
    args = parser.parse_args()

    if args.process:
        # Run the raw data processing (load, process, save)
        print("Prosessoidaan raakadataa...")
        argparse_raw_processor()

    elif args.filter:
        print("Suodatetaan interim dataa...")
        argparse_interim_processor()

    elif args.analyze:
        print("Analysoidaan dataa...")
        argparse_data_analysis()

    elif args.generate:
        print("Generoidaan dataa...")
        argparse_realtime_data()

    elif args.display:
        print("Avaa localhost portti:8050")
        argparse_realtime_data()

    elif args.all:
        print("Suoritetaan kaikki prosessit...")
        argparse_extract_all()

    else:
        # Scrape outage data and save it
        outage_data = scrape_outage_data()
        save_to_json(outage_data, 'data/raw/outages/outage_data.json')

        # Fetch weather data
        data = weatherApi()

        if data:
            for station, weather_data in data.items():
                print(f"Weather data for station {station}:")

                # Process weather data for readability in terminal
                for parameter, param_data in weather_data.items():
                    value = param_data['value']
                    units = param_data['units']
                    
                    # Convert np.float64 to native Python float, and handle np.nan
                    if isinstance(value, np.float64):
                        if np.isnan(value):
                            value = "N/A"
                        else:
                            value = float(value)  
                    
                    print(f"{parameter}: {value} {units}")
                
                print()

if __name__ == "__main__":
    main()
