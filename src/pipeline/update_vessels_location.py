
from huggingface_hub import login
from src.utils import get_vessels_locations_data, parse_data, upload_dataframe_hf
from huggingface_hub import HfApi
import os
import polars as pl
import pandas as pd
import io



def update_vessels_location_history(update_data: pd.DataFrame):
    '''
    Update vessels_location_history.pq in huggingface.

    1. Load current vessels_location_history.pq
    2. remove all entries with timestamp_hourly in update_data
    3. Concatenate current and update_data
    4. Upload to huggingface
    '''

    # Load current vessels_location_history.pq
    try:
        vessels_history = pl.scan_parquet('hf://datasets/clarkmaio/Ooops_dataset/vessels_location_history.pq').collect()
    except:
        vessels_history = pl.DataFrame(columns=['mmsi', 'latitude', 'longitude', 'timestamp', 'country', 'timestamp_hourly'])
    update_data = pl.DataFrame(update_data)

    # Remove all entries with timestamp_hourly in update_data
    vessels_history = vessels_history.filter(~pl.col('timestamp_hourly').is_in(update_data['timestamp_hourly']))
    
    # Concatenate current and update_data
    vessels_history = pl.concat([vessels_history, update_data])

    # Upload to huggingface
    buffer = io.BytesIO()
    vessels_history.to_pandas().to_parquet(buffer, engine='auto')

    api = HfApi()
    api.upload_file(
        path_or_fileobj=buffer,
        path_in_repo="vessels_location_history2.pq",
        repo_id="clarkmaio/Ooops_dataset",
        repo_type="dataset",
    )
    return


def UpdateVesselsLocation():
    """
    Update the data pipeline
    """
    # Load raw data and process
    data = get_vessels_locations_data()
    data_parsed = parse_data(data)

    # Filter data by last timestamp
    last_timestamp = data_parsed.timestamp_hourly.max()
    last_data = data_parsed[data_parsed['timestamp_hourly'] == last_timestamp]

    # Store on huggingface
    login(token=os.getenv('HF_TOKEN'), write_permission=True)
    tag = last_timestamp.strftime('%Y%m%d%H')
    upload_dataframe_hf(df=last_data,
                        filename=f'vessels_location/{tag}_vessels_location.pq')
    update_vessels_location_history(last_data)

if __name__ == '__main__':
    UpdateVesselsLocation()
