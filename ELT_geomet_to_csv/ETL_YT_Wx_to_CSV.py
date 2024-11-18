import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime

# Clear workspace equivalent
for name in dir():
    if not name.startswith('_'):
        del globals()[name]

# Quick Setup: Import necessary libraries (pandas, numpy, matplotlib, etc)
# Python automatically imports required libraries as needed.

# Helper function to sum while ignoring NA values
def sum_na(df):
    if df.isna().all():
        return np.nan
    else:
        return df.sum(skipna=True)

# Data Import
path = "C:/Users/User/OneDrive - Avalanche Services/Documents/03 NW Operations/Weather/"
files = [file for file in os.listdir(path) if file.endswith('.csv')]

# Read all CSV files into dataframes and assign them dynamically
csv_dataframes = {}
for file in files:
    file_name = os.path.splitext(file)[0]
    csv_dataframes[file_name] = pd.read_csv(os.path.join(path, file))

# Data Merge
wdata = pd.concat(csv_dataframes.values(), ignore_index=True)

# Data Sort: Selecting specific columns
columns_to_keep = ['X.Station.Number.', 'X.Station.Name.', 'X.Date.', 'X.Time.',
                   'X.Max.Air.Temp.', 'X.Present.Air.Temp.', 'X.Min.Air.Temp.',
                   'X.Wind.Speed.', 'X.Maximum.Wind.Speed.', 'X.Wind.Direction.',
                   'X.SD.Wind.Direction.', 'X.Snowpack.Height.', 'X.New.Snow.',
                   'X.Hourly.Precip.', 'X.New.Precip.', 'X.Dew.Point.Temp.',
                   'X.Relative.Humidity.', 'X.Precip.Detector.Ratio.', 'X.Precip.Gauge.Total.',
                   'X.Atm..Pressure.', 'X.Pavement.Temperature.1.', 'X.Pavement.Temperature.2.',
                   'X.Alternate.Pavement.Temp.1.', 'X.Alternate.Pavement.Temp.2.',
                   'X.Sub.Temperature.', 'X.Freezing.Point.Temperature.1.',
                   'X.Freezing.Point.Temperature.2.', 'X.Road.Status.']

wdata = wdata[columns_to_keep]

# Remove duplicated rows based on specific columns
wdata.drop_duplicates(subset=['X.Station.Number.', 'X.Date.', 'X.Time.'], inplace=True)

# Data Export and Cleanup
wdata.to_csv(os.path.join(path, 'allwxdata.csv'), index=False)
for file in files:
    if file != 'allwxdata.csv':
        os.remove(os.path.join(path, file))

# Data De-bug and Program Specific Info
wdata['DATE'] = pd.to_datetime(wdata['X.Date.'] + ' ' + wdata['X.Time.'], format="%m/%d/%Y %H:%M", errors='coerce')
wdata['TIME'] = wdata['DATE'].dt.strftime('%H%M')

wdata.dropna(subset=['DATE'], inplace=True)

wdata = wdata[['X.Station.Number.', 'X.Station.Name.', 'X.Date.', 'X.Time.',
               'X.Max.Air.Temp.', 'X.Present.Air.Temp.', 'X.Min.Air.Temp.',
               'X.Wind.Speed.', 'X.Maximum.Wind.Speed.', 'X.Wind.Direction.',
               'X.SD.Wind.Direction.', 'X.Snowpack.Height.', 'X.New.Snow.',
               'X.Hourly.Precip.', 'X.New.Precip.', 'DATE', 'TIME']]

# Replacing negative or unrealistic values with NaN
replace_conditions = {
    'X.New.Precip.': (wdata['X.New.Precip.'] < 0, np.nan),
    'X.New.Snow.': (wdata['X.New.Snow.'] < 0, np.nan),
    'X.Max.Air.Temp.': (wdata['X.Max.Air.Temp.'] < -30, np.nan),
    'X.Present.Air.Temp.': (wdata['X.Present.Air.Temp.'] < -30, np.nan),
    'X.Min.Air.Temp.': (wdata['X.Min.Air.Temp.'] < -30, np.nan),
    'X.Hourly.Precip.': (wdata['X.Hourly.Precip.'] < 0, 0),
    'X.Wind.Speed.': (wdata['X.Wind.Speed.'] < 0, np.nan),
    'X.Wind.Direction.': (wdata['X.Wind.Direction.'] < 0, np.nan),
    'X.Snowpack.Height.': (wdata['X.Snowpack.Height.'] < 0, np.nan),
    'X.SD.Wind.Direction.': (wdata['X.SD.Wind.Direction.'] < 0, np.nan),
    'X.Maximum.Wind.Speed.': ((wdata['X.Maximum.Wind.Speed.'] > 150) | (wdata['X.Maximum.Wind.Speed.'] < 0), np.nan)
}

for column, (condition, replacement) in replace_conditions.items():
    wdata.loc[condition, column] = replacement

# Assigning labels based on station numbers
wdata['NAME'] = None
wdata['TEMP'] = None
wdata['PRECIP'] = None
wdata['WINDS'] = None
wdata['HS'] = None

station_map = {
    "52401": ("Shames (740m)", None, None, None, 'Yes'),
    "52093": ("Onion Lake (220m)", None, None, None, None),
    "52391": ("Rainbow East (12m)", None, 'Yes', None, None),
    "52091": ("Legate Creek (130m)", None, 'Yes', None, None),
    "52325": ("Kasiks Low (732m)", 'Yes', None, None, 'Yes'),
    "52322": ("Kasiks High (1435m)", 'Yes', None, 'Yes', None),
    "52326": ("Salvus (10m)", 'Yes', 'Yes', None, 'Yes')
}

for station_num, (name, temp, precip, winds, hs) in station_map.items():
    idx = wdata['X.Station.Number.'] == station_num
    wdata.loc[idx, ['NAME', 'TEMP', 'PRECIP', 'WINDS', 'HS']] = name, temp, precip, winds, hs

# Plotting examples (similar to ggplot2 in R, using matplotlib in Python)
# For simplicity, only a few basic plots are presented, but more customization can be added as needed.

# Example plot: Wind Plot
ld = wdata[wdata['WINDS'] == 'Yes']
plt.figure(figsize=(8, 6))
plt.polar(ld['X.Wind.Direction.'] * np.pi / 180, ld['X.Wind.Speed.'], 'o', alpha=0.1, color='blue')
plt.title(f"{ld['NAME'].unique()[0]} 24hr Wind Summary")
plt.show()

# Daily Weather Summary Table
stations = wdata['NAME'].unique()
wtable = pd.DataFrame(columns=["Station", "Tmax", "Tpres", "Tmin", "HN24SWE", "HN24", "Rain24", "HS", "HSdelta", "WS", "WD", "STD"])

for station in stations:
    station_data = wdata[wdata['NAME'] == station]
    if station_data.empty:
        continue

    recent_data = station_data[station_data['DATE'] == station_data['DATE'].max()]
    hs_delta = recent_data['X.Snowpack.Height.'].iloc[0] - station_data['X.Snowpack.Height.'].iloc[0]
    hn24_swe = sum_na(recent_data['X.Hourly.Precip.'])

    wtable = wtable.append({
        "Station": station,
        "Tmax": recent_data['X.Max.Air.Temp.'].max(skipna=True),
        "Tpres": recent_data['X.Present.Air.Temp.'].iloc[0],
        "Tmin": recent_data['X.Min.Air.Temp.'].min(skipna=True),
        "HN24SWE": hn24_swe,
        "HSdelta": hs_delta,
        "HS": recent_data['X.Snowpack.Height.'].iloc[0]
    }, ignore_index=True)

print(wtable)

# Save as PDF (using matplotlib)
plt.savefig("daily_weather_summary.pdf")

# Note: This script provides a high-level translation of the logic from R to Python, but full equivalence might need more adjustments.
# The equivalent logic was followed where possible, with the code aiming for functionality similar to the R script.
# Some manual intervention might be required for file paths, column names, and visualization adjustments.
