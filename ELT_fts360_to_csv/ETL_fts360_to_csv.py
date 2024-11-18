import requests
import pandas as pd
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(filename='fts_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# User entered constants
agency_id = 874
api_key = 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJEYXRhIFRva2VuIGZvciBBZ2VuY3kgODc0Iiwib3JpZ2luIjoiZGV2aWNlIiwiaWF0IjoxNjg5MzYwMjU3fQ.sMP9W1dZl8kI257pOU16FU5N8lCxTqFwwcaHlXpY7HX3yKrv_r-LH3Op-SM6J5my2FCp7np1fVqSEiWSU8RnXA'
station_ids = ['6477ac8079ced27eb18b878b', '64766d1c19a78e32cec13b09']
look_back_in_minutes = 120
# End user entered constants

# Setting up base URL and headers for the API request
base_url = f"https://fts360devapi.com/data/v1/agencies/{agency_id}/records?"
headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + api_key
}

def look_back(minutes):
    return datetime.now() - timedelta(minutes=minutes)

def download_data():
    end_date = datetime.now()
    start_date = look_back(look_back_in_minutes)

    # Dictionary to hold DataFrames for each station
    station_dataframes = {}
    
    for station_id in station_ids:
        # Convert dates to ISO format for the query
        params = {
            'startDate': start_date.isoformat(),
            'endDate': end_date.isoformat(),
            'stationIds[]': station_id
        }

        # Sending the GET request to the API
        response = requests.get(base_url, headers=headers, params=params)

        # Handling the response
        if response.status_code == 200:
            data = response.json()
            logging.info(f"Data downloaded successfully for station {station_id}: {data}")
            
            # Convert the JSON response to a pandas DataFrame for the station
            station_df = pd.DataFrame(data)
            station_dataframes[station_id] = station_df
            logging.info(f"Data as DataFrame for station {station_id}:\n{station_df}")
        else:
            logging.error(f"Error for station {station_id}: {response.status_code} - {response.text}")
    
    # Log all DataFrames or indicate if none were downloaded
    if station_dataframes:
        for station_id, df in station_dataframes.items():
            logging.info(f"DataFrame for station {station_id}:\n{df}")
    else:
        logging.info("No data available to display.")

# Execute the function
download_data()
