
from huggingface_hub import login
from src.utils import get_vessels_locations_data, parse_data, upload_dataframe_hf
import os





def UpdateDataPipeline():
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



if __name__ == '__main__':
    UpdateDataPipeline()
