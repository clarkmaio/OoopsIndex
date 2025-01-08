
import plotly
import plotly.graph_objects as go
import pandas as pd


CENTER_MAP = (59.831852, 25.118242)

def map_animation(data: pd.DataFrame):
    '''
    Create scatter map animation that shows vessels moving on the map.

    data must have columns:
    - latitude: float
    - longitude: float
    - timestamp_hourly: datetime
    - tag: bool
    '''


    fig_dict = {
    "data": [],
    "layout": {},
    "frames": []
    }

    fig_dict["layout"]["mapbox"] = {
        "center": {
            "lat": CENTER_MAP[0],
            "lon": CENTER_MAP[1]
        },
        "pitch": 0,
        "zoom": 5
    }

    fig_dict["layout"]["height"] = 600
    fig_dict["layout"]["legend"] = {
        "yanchor": "top",
        "y": 0.99,
        "xanchor": "right",
        "x": 0.99,
        "orientation": "v"
    }

    fig_dict["layout"]["mapbox"]["style"] = 'carto-positron'

    # Create single frame to debug
    frame_tmp = go.Scattermapbox(
        lat=data['latitude'],
        lon=data['longitude'],
        mode='markers',
        marker=dict(
            size=5,
            color='red',
            opacity=0.5
        ),
        showlegend=False,
        hoverinfo='skip'
    )
    fig_dict["data"].append(frame_tmp)
    fig_dict["frames"].append(
        frame_tmp
    )
    

    fig = go.Figure(fig_dict)
    return fig