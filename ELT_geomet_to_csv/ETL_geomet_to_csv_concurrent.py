import concurrent.futures
from datetime import datetime, timedelta
import re
import warnings
import numpy as np
from owslib.wms import WebMapService
import pandas as pd
import logging
import time

# Ignore warnings from the OWSLib module
warnings.filterwarnings('ignore', module='owslib', category=UserWarning)

logging.basicConfig(level=logging.INFO, filename='run_log.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.info('Start time: ' + str(datetime.today()))

output_path = "D://ETL_output//wx//"

# Set up forecast parameters
layers = [
    'GDPS.ETA_TT',
    'GDPS.ETA_PN-SLP',
    'GDPS.ETA_HR',
    'GDPS.ETA_RN',
    'GDPS.ETA_SN',
    'GDPS.PRES_WD.800.3h',
    'GDPS.PRES_WSPD.800.3h',
    'GEPS.DIAG.3_TT.ERMEAN',
    'GEPS.DIAG.3_TT.ERC25',
    'GEPS.DIAG.3_TT.ERC75',
    'GEPS.DIAG.3_WCF.ERMEAN',
    'GEPS.DIAG.24_PRMM.ERGE1',
    'GEPS.DIAG.24_RNMM.ERGE1',
    'GEPS.DIAG.24_RNMM.ERGE10',
    'GEPS.DIAG.24_RNMM.ERGE25',
    'GEPS.DIAG.24_RNMM.ERMEAN',
    'GEPS.DIAG.24_RNMM.ERC25',
    'GEPS.DIAG.24_RNMM.ERC75',
    'GEPS.DIAG.24_SNMM.ERGE1',
    'GEPS.DIAG.24_SNMM.ERGE10',
    'GEPS.DIAG.24_SNMM.ERGE25',
    'GEPS.DIAG.24_SNMM.ERMEAN',
    'GEPS.DIAG.24_SNMM.ERC25',
    'GEPS.DIAG.24_SNMM.ERC75'
]

# WMS service connection
wms = WebMapService('https://geo.weather.gc.ca/geomet?SERVICE=WMS' + '&REQUEST=GetCapabilities', version='1.3.0', timeout=30)

# Station details from CSV file
stations_df = pd.read_csv('stations.csv')
stations = stations_df.to_dict(orient='records')

# Define forecast range
current_date = datetime.today().strftime('%Y-%m-%d')
geps_run = current_date + 'T00:00:00Z'
fx_range = pd.date_range(geps_run, periods=16)
fx_range = [date_time.date() for date_time in fx_range]

def time_parameters(layer):
    start_time, end_time, interval = wms[layer].dimensions['time']['values'][0].split('/')
    iso_format = '%Y-%m-%dT%H:%M:%SZ'
    start_time = datetime.strptime(start_time, iso_format)
    end_time = datetime.strptime(end_time, iso_format)
    interval = int(re.sub(r'\D', '', interval))
    return start_time, end_time, interval

def request(layer, min_x, min_y, max_x, max_y, time):
    info = []
    pixel_value = []
    retries = 3
    for attempt in range(retries):
        try:
            for timestep in time:
                info.append(wms.getfeatureinfo(
                    layers=[layer],
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
                text = info[-1].read().decode('utf-8')
                pixel_value.append(str(re.findall(r'value_0\s+\d*.*\d+', text)))
                try:
                    pixel_value[-1] = float(re.sub('value_0 = \"', '', pixel_value[-1]).strip('[""]'))
                except:
                    pixel_value[-1] = 0.0
            break
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed for layer {layer}: {e}")
            time.sleep(5)  # Wait before retrying
            if attempt == retries - 1:
                logger.exception(f"All retry attempts failed for layer {layer}")
                pixel_value = "~"
    return pixel_value

def process_layer(layer, station, min_x, min_y, max_x, max_y):
    logger.info('Layer: ' + layer + "...")
    logger.info('Commence WMS call: ' + str(datetime.today()))
    start_time, end_time, interval = time_parameters(layer)
    time = [start_time]
    while time[-1] < end_time:
        time.append(time[-1] + timedelta(hours=interval))
    
    fx = pd.DataFrame()
    fx['time'] = time
    fx['value'] = request(layer, min_x, min_y, max_x, max_y, time)
    logger.info('Complete WMS call: ' + str(datetime.today()))
    
    return fx

for station in stations:
    logger.info('Station: ' + station['name'] + "...")
    fx_table = pd.DataFrame(index=fx_range)

    x, y = float(station['x']), float(station['y'])
    min_x, min_y, max_x, max_y = x - 0.25, y - 0.25, x + 0.25, y + 0.25

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_layer = {executor.submit(process_layer, layer, station, min_x, min_y, max_x, max_y): layer for layer in layers}
        for future in concurrent.futures.as_completed(future_to_layer):
            layer = future_to_layer[future]
            try:
                fx = future.result()
                if not fx.empty:
                    if layer in ['GDPS.ETA_TT', 'GEPS.DIAG.3_TT.ERMEAN', 'GEPS.DIAG.3_TT.ERC25', 'GEPS.DIAG.3_TT.ERC75', 'GEPS.DIAG.3_WCF.ERMEAN']:
                        fx['date'] = [date_time.date() for date_time in fx['time']]
                        fx_table[layer + '_min'] = fx.groupby(['date'])['value'].min().round(0)
                        fx_table[layer + '_max'] = fx.groupby(['date'])['value'].max().round(0)
                    elif layer in ['GDPS.ETA_PN-SLP', 'GDPS.ETA_HR']:
                        fx['date'] = [date_time.date() for date_time in fx['time']]
                        fx_table[layer + '_mean'] = fx.groupby(['date'])['value'].mean().round(0)
                    elif layer in ['GDPS.ETA_RN', 'GDPS.ETA_SN']:
                        fx['date'] = [date_time.date() for date_time in fx['time']]
                        fx_table[layer + '_total'] = fx.groupby(['date'])['value'].sum().round(0)
                    elif layer in ['GDPS.PRES_WD.800.3h']:
                        fx['date'] = [date_time.date() for date_time in fx['time']]
                        fx_table[layer] = fx.groupby(['date'])['value'].agg(pd.Series.mode).to_frame()
                    elif layer in ['GDPS.PRES_WSPD.800.3h']:
                        fx['date'] = [date_time.date() for date_time in fx['time']]
                        fx_table[layer] = fx.groupby(['date'])['value'].agg(pd.Series.mode)
                    elif layer in ['GEPS.DIAG.24_PRMM.ERGE1', 'GEPS.DIAG.24_RNMM.ERGE1', 'GEPS.DIAG.24_RNMM.ERGE10', 'GEPS.DIAG.24_RNMM.ERGE25',
                                   'GEPS.DIAG.24_RNMM.ERC25', 'GEPS.DIAG.24_RNMM.ERC75', 'GEPS.DIAG.24_RNMM.ERMEAN', 'GEPS.DIAG.24_SNMM.ERGE1',
                                   'GEPS.DIAG.24_SNMM.ERGE10', 'GEPS.DIAG.24_SNMM.ERGE25', 'GEPS.DIAG.24_SNMM.ERC25', 'GEPS.DIAG.24_SNMM.ERC75',
                                   'GEPS.DIAG.24_SNMM.ERMEAN']:
                        fx['time'] = [date_time + timedelta(days=-1) for date_time in fx['time']]
                        fx = fx.set_index('time')
                        fx_table[layer] = fx.at_time('00:00').round(0)
            except Exception as e:
                logger.error(f"Error processing layer {layer}: {e}")

    try:
        fx_table.to_csv(output_path + station['name'] + '.csv', index=True, mode='w+')
        logger.info(f'Successfully saved CSV for station: {station["name"]}')
    except Exception as e:
        logger.exception(f"Failed to save CSV for station {station['name']}: {e}")

logging.info('End Time: ' + str(datetime.today()))
