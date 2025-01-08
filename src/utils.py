import pandas as pd
from typing import Dict
from tqdm import tqdm
import requests
from dotenv import load_dotenv
import os
import yaml
from huggingface_hub import upload_file, login
from typing import List
load_dotenv()


def timestamp_int_to_datetime(timestamp):
    return pd.to_datetime(timestamp, unit='ms')

def assign_country(df, country_flag_map: Dict):
    df['country'] = df['mmsi'].apply(lambda x: country_flag_map.get(int(str(x)[:3]), None))
    return df


def get_country_flag_map():
    root_path = os.path.dirname(os.path.dirname(__file__))
    config_path = os.path.join(root_path, 'conf/config.yaml')   
    return yaml.load(open(config_path, 'r'), Loader=yaml.FullLoader)['MMSI_COUNTRY_MAP']

def get_country_list():
    mmsi_map = get_country_flag_map()
    country_list =  list(set(mmsi_map.values()))
    return country_list
    


def get_config():
    return yaml.load(open('conf/config.yaml', 'r'), Loader=yaml.FullLoader)

def parse_data(data: pd.DataFrame):

    vessels_dataframe = pd.DataFrame(columns=['mmsi', 'latitude', 'longitude', 'timestamp'])

    for index, row in tqdm(data.iterrows(), total=len(data)):
        mmsi = row['features']['properties']['mmsi']
        latitude = row['features']['geometry']['coordinates'][1]
        longitude = row['features']['geometry']['coordinates'][0]
        timestamp = row['features']['properties']['timestampExternal']
        timestamp = timestamp_int_to_datetime(timestamp)

        vessels_dataframe.loc[len(vessels_dataframe)] = [mmsi, latitude, longitude, timestamp]

    country_flag_map = get_country_flag_map()
    vessels_dataframe = assign_country(vessels_dataframe, country_flag_map=country_flag_map)
    vessels_dataframe['timestamp_hourly'] = vessels_dataframe['timestamp'].dt.floor('h')
    return vessels_dataframe

def get_vessels_locations_data(api_url: str = 'https://meri.digitraffic.fi/api/ais/v1/locations') -> pd.DataFrame:
    response = requests.get(api_url)
    res = response.json()
    df = pd.DataFrame(res)
    return df





def upload_dataframe_hf(df: pd.DataFrame, filename: str) -> None:
    '''
    Upload dataframe to huggingface dataset as raw file.

    df: datafrae to upload
    filename: path where the file will be stored in hf dataset space
    concat: If True concat to existing dataframe and drop duplicates
    '''
    import io
    buffer = io.BytesIO()

    df.to_parquet(buffer, engine='pyarrow')
    upload_file(path_or_fileobj=buffer, 
                repo_type='dataset',
                path_in_repo=filename,
                repo_id='clarkmaio/Ooops_dataset')


if __name__ == '__main__':

    data = get_vessels_locations_data()
    data_parsed = parse_data(data)

    last_timestamp = data_parsed.timestamp_hourly.max()
    last_data = data_parsed[data_parsed['timestamp_hourly'] == last_timestamp]
