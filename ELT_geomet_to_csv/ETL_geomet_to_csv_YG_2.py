# Importation of Python modules
from datetime import time 
from datetime import datetime, timedelta
import tzlocal
import re
import warnings
import numpy as np
from owslib.wms import WebMapService
import pandas as pd
from openpyxl import Workbook
import logging

# Ignore warnings from the OWSLib module
warnings.filterwarnings('ignore', module='owslib', category=UserWarning)

logging.basicConfig(level=logging.INFO, filename='run_log.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logger=logging.getLogger() 
logger.info('Start time: ' +  str(datetime.today()))

#output_path = "C:\\Users\\Eirik Sharp\\Avalanche Services\\Operations - Remote Programs\\ZArchive\\LSCLP_Wx\\01ImmediateForecast"
output_path = "D:\\ETL_output\\wx\\"

# Set up forecast parameters
# Layers:

layers = [
    {'name': 'GDPS.ETA_TT', 'description': 'Air temperature [°C]'},
    {'name': 'GDPS.ETA_PN-SLP', 'description': 'Sea level pressure [Pa]'},
    {'name': 'GDPS.ETA_HR', 'description': 'Relative humidity [%]'},
    {'name': 'GDPS.ETA_RN', 'description': 'Rain accumulation [kg/(m^2)]'},
    {'name': 'GDPS.ETA_SN', 'description': 'Snow accumulation [kg/(m^2)]'},
    {'name': 'GDPS.PRES_WD.800.3h', 'description': 'Wind direction at 850.0 mb (3 hourly forecast) [°]'},
    {'name': 'GDPS.PRES_WSPD.800.3h', 'description': 'Winds at 800.0 mb (3 hourly forecast) [m/s]'},
    {'name': 'GEPS.DIAG.3_TT.ERMEAN', 'description': 'Air temperature at 2 m above ground [°C] (mean)'},
    {'name': 'GEPS.DIAG.3_TT.ERC25', 'description': 'Air temperature at 2 m above ground [°C] (25th percentile)'},
    {'name': 'GEPS.DIAG.3_TT.ERC75', 'description': 'Air temperature at 2 m above ground [°C] (75th percentile)'},
    {'name': 'GEPS.DIAG.3_WCF.ERMEAN', 'description': 'Wind chill factor at 2 m above ground [°C] (mean)'},
    {'name': 'GEPS.DIAG.24_PRMM.ERGE1', 'description': 'Quantity of precipitation >= 1 mm [probability %]'},
    {'name': 'GEPS.DIAG.24_RNMM.ERGE1', 'description': 'Rain >= 1 mm [probability %]'},
    {'name': 'GEPS.DIAG.24_RNMM.ERGE10', 'description': 'Rain >= 10 mm [probability %]'},
    {'name': 'GEPS.DIAG.24_RNMM.ERGE25', 'description': 'Rain >= 25 mm [probability %]'},
    {'name': 'GEPS.DIAG.24_RNMM.ERMEAN', 'description': 'Rain (mean)'},
    {'name': 'GEPS.DIAG.24_RNMM.ERC25', 'description': 'Rain (25th percentile)'},
    {'name': 'GEPS.DIAG.24_RNMM.ERC75', 'description': 'Rain (75th percentile)'},
    {'name': 'GEPS.DIAG.24_SNMM.ERGE1', 'description': 'Snow >= 1 mm [probability %]'},
    {'name': 'GEPS.DIAG.24_SNMM.ERGE10', 'description': 'Snow >= 10 mm [probability %]'},
    {'name': 'GEPS.DIAG.24_SNMM.ERGE25', 'description': 'Snow >= 25 mm [probability %]'},
    {'name': 'GEPS.DIAG.24_SNMM.ERMEAN', 'description': 'Snow (mean)'},
    {'name': 'GEPS.DIAG.24_SNMM.ERC25', 'description': 'Snow (25th percentile)'},
    {'name': 'GEPS.DIAG.24_SNMM.ERC75', 'description': 'Snow (75th percentile)'}
]

# Determine local timezone and offset
local_timezone = tzlocal.get_localzone()
current_time = datetime.now(local_timezone)
time_zone = current_time.utcoffset().total_seconds() / 3600
logger.info(f"Calculated time zone offset: {time_zone} hours")

# set model run time
current_date = datetime.today().strftime('%Y-%m-%d')
geps_run = current_date+'T00:00:00Z'

#define forecast range
fx_range = pd.date_range(geps_run,periods=16)
#fx_range = [date_time.strftime('%Y-%m-%d') for date_time in fx_range]
fx_range = [date_time.date() for date_time in fx_range]

# Station details:

# Station details from CSV file
stations_df = pd.read_csv('stations.csv')
stations = stations_df.to_dict(orient='records')


# WMS service connection
wms = WebMapService('https://geo.weather.gc.ca/geomet?SERVICE=WMS' + '&REQUEST=GetCapabilities', version='1.3.0', timeout=300)

def correct_wind(station,d):
    dirs = ["~"]
    cwind = 0
    dirs = [station['wind_in'], station['wind_out']]
    cwind = int((d + 45) / 180)
    return dirs[cwind % 2]
    

def ms_to_windspeed(m):
    if (m<=1):
        return 'C'
    if (m>1 and m<=7):
        return 'L'
    if (m>7 and m<=11):
        return 'M'
    if (m>11 and m<=17):
        return 'S'
    if (m>17):
        return 'X'

def time_parameters(layer):
    start_time, end_time, interval = (wms[layer].dimensions['time']['values'][0].split('/'))
    iso_format = '%Y-%m-%dT%H:%M:%SZ'
    start_time = datetime.strptime(start_time, iso_format)
    end_time = datetime.strptime(end_time, iso_format)
    interval = int(re.sub(r'\D', '', interval))
    return start_time, end_time, interval

def request(layer):
    info = []
    pixel_value = []
    try:
        for timestep in time:
            # WMS GetFeatureInfo query
            info.append(wms.getfeatureinfo(layers=[layer],
                                        srs='EPSG:4326',
                                        bbox=(min_x, min_y, max_x, max_y),
                                        size=(100, 100),
                                        format='image/jpeg',
                                        query_layers=[layer],
                                        info_format='text/plain',
                                        xy=(50, 50),
                                        feature_count=1,
                                        time=str(timestep.isoformat()) + 'Z'
                                        ))
            # Probability extraction from the request's results
            text = info[-1].read().decode('utf-8')
            pixel_value.append(str(re.findall(r'value_0\s+\d*.*\d+', text)))
            try:
                pixel_value[-1] = float(re.sub('value_0 = \'', '', pixel_value[-1]).strip('[""]'))
            except:
                pixel_value[-1] = 0.0
    except:
        logger.exception('')
        pixel_value = "~"
    return pixel_value

for station in stations:
    logger.info('Station: ' +  station['name'] + "...")
    fx_table=pd.DataFrame(index=fx_range)

    x=float(station['x'])
    y=float(station['y'])

    # bbox parameter
    min_x, min_y, max_x, max_y = x - 0.25, y - 0.25, x + 0.25, y + 0.25

    # dictonary to save GEPS forecast data
    fx_data = {}
    # dataframe for agregated forecast data
    fx_table=pd.DataFrame(index=fx_range)
        
    # agregated forecast data
    for layer in layers:
        logger.info('Layer: ' +  layer['name'] + "...")
        logger.info('Commence wmw call: ' +  str(datetime.today()))
        start_time, end_time, interval = time_parameters(layer['name'])
        time = [start_time]
        while time[-1] < end_time:
            time.append(time[-1] + timedelta(hours=interval))
        fx = pd.DataFrame()
        fx['time']=time
        fx['value']=request(layer['name'])
        logger.info('Complete wmw call: ' +  str(datetime.today()))
        if layer['name'] in  ['GDPS.ETA_TT', 'GEPS.DIAG.3_TT.ERMEAN', 'GEPS.DIAG.3_TT.ERC25', 'GEPS.DIAG.3_TT.ERC75', 'GEPS.DIAG.3_WCF.ERMEAN']:      
            fx['date'] = [date_time.date() for date_time in fx['time']]
            fx_table[f"{layer['description']} Min"] = fx.groupby([fx.date])['value'].min().round(0)
            fx_table[f"{layer['description']} Max"] = fx.groupby([fx.date])['value'].max().round(0)
        if layer['name'] in ['GDPS.ETA_PN-SLP', 'GDPS.ETA_HR']:
            fx['date'] = [date_time.date() for date_time in fx['time']]
            fx_table[f"{layer['description']} Mean"] =fx.groupby([fx.date])['value'].mean().round(0)
        if layer['name'] in ['GDPS.ETA_RN']:
            fx['date'] = [date_time.date() for date_time in fx['time']]
            fx_table[f"{layer['description']} Total"]  = fx.groupby([fx.date])['value'].sum().round(0)
        if layer['name'] in [ 'GDPS.ETA_SN']:
            fx['date'] = [date_time.date() for date_time in fx['time']]
            fx_table[f"{layer['description']} 5%"]  = (fx.groupby([fx.date])['value'].sum()/50).round(0)
            fx_table[f"{layer['description']} 10%"]  = (fx.groupby([fx.date])['value'].sum()/100).round(0)        
        if layer['name'] in ['GDPS.PRES_WD.800.3h']:
            fx['date'] = [date_time.date() for date_time in fx['time']]
            fx['value'] = fx['value'].apply(lambda x: correct_wind(station, x))
            fx_table[layer['description']] = fx.groupby([fx.date])['value'].agg(pd.Series.mode).to_frame()
        if layer['name'] in ['GDPS.PRES_WSPD.800.3h']:
            fx['date']=[date_time.date() for date_time in fx['time']]
            fx['value']=fx['value'].apply(lambda x: ms_to_windspeed(x))
            fx_table[layer['description']]=fx.groupby([fx.date])['value'].agg(pd.Series.mode).to_frame()
        if layer['name'] in ['GEPS.DIAG.24_PRMM.ERGE1', 'GEPS.DIAG.24_RNMM.ERGE1', 'GEPS.DIAG.24_RNMM.ERGE10', 'GEPS.DIAG.24_RNMM.ERGE25',
                           'GEPS.DIAG.24_RNMM.ERC25', 'GEPS.DIAG.24_RNMM.ERC75', 'GEPS.DIAG.24_RNMM.ERMEAN', 'GEPS.DIAG.24_SNMM.ERGE1',
                           'GEPS.DIAG.24_SNMM.ERGE10', 'GEPS.DIAG.24_SNMM.ERGE25', 'GEPS.DIAG.24_SNMM.ERC25', 'GEPS.DIAG.24_SNMM.ERC75',
                           'GEPS.DIAG.24_SNMM.ERMEAN']:
            fx['time'] = [date_time + timedelta(days=-1) for date_time in fx['time']]
            fx = fx.set_index('time')
            fx_table[layer['description']] = fx.at_time('08:00').round(0)
            
    # saving the DataFrame as a CSV file

    try:
        csv_data = fx_table.T.to_csv(output_path + station['name']+'.csv', index = True, mode='w+')
        logger.info(csv_data)
    except:
        logger.exception('')

   
    logging.info('End Time: ' +  str(datetime.today()))