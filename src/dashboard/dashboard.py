
import streamlit as st
import polars as pl
import pandas as pd
from typing import List
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
import numpy as np

from src import utils

st.set_page_config(initial_sidebar_state='auto', page_icon=':ship:', page_title='Ooops!', layout="centered")


LAT_BOX = [59.2, 60.5]
LON_BOX = [24, 27.5]


ESTLINK1 = {  
    'lon': [24.551667, 24.560278],
    'lat': [60.203889, 59.384722]
}

ESTLINK2 = {
    'lon': [25.528056, 27.0497],
    'lat': [60.292778, 59.3511]
}


def to_excel(dataframe):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        dataframe.to_excel(writer, index=False, sheet_name="Foglio1")
    processed_data = output.getvalue()
    return processed_data

def to_csv(dataframe):
    return dataframe.to_csv().encode('utf-8')


def filter_data(data, countries: List[str] = None, mmsi: List[int] = None):
    if countries is not None:
        data = data[data['country'].isin(countries)]
    if mmsi is not None:
        data = data[data['mmsi'].isin(mmsi)]
    return data


def tag_data(data, countries: List[str] = None, mmsi: List[int] = None):
    # Assign true to the rows that match the filter
    data['tag'] = False
    if countries is not None:
        data.loc[data['country'].isin(countries), 'tag'] = True
    if mmsi is not None:
        data.loc[data['mmsi'].isin(mmsi), 'tag'] = True

    # Define style
    data['color'] = 'green'
    data.loc[data['tag'] == True, 'color'] = 'red'

    data['size'] = 1
    data.loc[data['tag'] == True, 'size'] = 2
    return data


def return_filter_data(data, countries: List[str] = None, mmsi: List[int] = None):
    data = filter_data(data, countries=countries, mmsi=mmsi)
    data = tag_data(data, countries=countries, mmsi=mmsi)
    return data

def postprocess_data(data, countries: List[str] = None, mmsi: List[int] = None):
    data = tag_data(data, countries=countries, mmsi=mmsi)
    last_data = data.loc[data['timestamp_hourly'] == data['timestamp_hourly'].max()]
    return data, last_data 

def load_data():
    data = pl.read_parquet('hf://datasets/clarkmaio/Ooops_dataset/vessels_location_history.pq').to_pandas()
    data.drop('__index_level_0__', axis=1, inplace=True, errors='ignore')    
    return data

st.markdown("<h1 style='text-align: center; color: black;'>Ooops Index &#128674;</h1>", unsafe_allow_html=True)
dashboard_tab, info_tab = st.tabs(['Dashboard', 'Info'])


with info_tab:
    st.markdown("""
    ## Ooops Index

    During Christmas 2024, the Estlink submarine power cable was damaged by a "careless" vessel. 
    The Ooops Index is a metric that count how many (probable) careless vessels are close to Estlinks.

    ### Data
    The data comes from Finntraffic API.
    Since the API provide only a snapshot of vessels positions, data are periodically collected and stored in an Huggingface dataset.
                
    * [Finntraffic API](https://www.digitraffic.fi/en/marine-traffic/)  
    * [Huggingface Ooops dataset](https://huggingface.co/datasets/clarkmaio/Ooops_dataset)     


    ### Trigger area

    The trigger area is defined by a rectangle that covers the area around Estlink 1 and 2. The trigger area is defined by the following coordinates:

    - Latitude: 59.2, 60.5
    - Longitude: 24, 27.5

    """)


with dashboard_tab:
    with st.sidebar:
        country_selection = st.multiselect(label='Countries', options=utils.get_country_list(), default=['Russia'])
        mmsi_selection = st.text_input(label='MMSI', value='518998865')

        st.markdown('<br><br>', unsafe_allow_html=True)
        update_data = st.button(label='Update data', key='update_data', type='primary', use_container_width=True, icon=':material/sync:')

        st.markdown('<br>', unsafe_allow_html=True)
        st.download_button(
            label="Download data",
            data=to_csv(pd.DataFrame()) if 'data' not in st.session_state else to_csv(st.session_state.data),
            file_name="vessels.csv",
            #mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True, icon=':material/download:'
        )

    #------------------------------- PREPARE DATA
    # First time load
    if 'data' not in st.session_state:
        st.session_state['data'] = load_data()
        mmsi_selection_integers = None if mmsi_selection == '' else [int(x) for x in mmsi_selection.split(',')]
        st.session_state.data, st.session_state.last_data = postprocess_data(data=st.session_state['data'], countries=country_selection, mmsi=mmsi_selection_integers)

    # Update index
    if update_data:
        mmsi_selection_integers = None if mmsi_selection == '' else [int(x) for x in mmsi_selection.split(',')]
        st.session_state.data = load_data()
        st.session_state.data, st.session_state.last_data = postprocess_data(data=st.session_state['data'], countries=country_selection, mmsi=mmsi_selection_integers)

    c1_metric, c2_metric = st.columns(2)
    #  ------------------------------------------ PLOT
    fig = go.Figure()


    # Scatter
    no_tag_trace = go.Scattermap(
        lat=st.session_state.last_data .query('tag == False')['latitude'],
        lon=st.session_state.last_data .query('tag == False')['longitude'],
        mode='markers',
        name='Vessels',
        marker=dict(
            size=10,
            color='green',
            opacity=0.5),
    )
    fig.add_trace(no_tag_trace)

    # Scatter highlight
    tag_trace = go.Scattermap(
        lat=st.session_state.last_data .query('tag == True')['latitude'],
        lon=st.session_state.last_data .query('tag == True')['longitude'],
        mode='markers',
        name='Careless vessels',
        marker=dict(
            size=20,
            color='red',
            opacity=0.5,
        )
    )
    fig.add_trace(tag_trace)

    # Perimeter
    rectangle = go.Scattermap(
        lon=[LON_BOX[0], LON_BOX[1], LON_BOX[1], LON_BOX[0], LON_BOX[0]],
        lat=[LAT_BOX[0], LAT_BOX[0], LAT_BOX[1], LAT_BOX[1], LAT_BOX[0]],
        mode='lines',
        name='Trigger area',
        line=dict(width=2, color='black')
    )
    fig.add_trace(rectangle)

    # Draw Estlink
    estlink1 = go.Scattermap(
        lon=ESTLINK1['lon'],
        lat=ESTLINK1['lat'],
        mode='lines+markers',
        name='Estlink 1',
        showlegend=False,
        line=dict(width=2, color='blue')
    )
    fig.add_trace(estlink1)

    estlink2 = go.Scattermap(
        lon=ESTLINK2['lon'],
        lat=ESTLINK2['lat'],
        mode='lines+markers',
        name='Estlink 2',
        showlegend=False,
        line=dict(width=2, color='blue')
    )
    fig.add_trace(estlink2)



    fig.update_layout(
        hovermode='closest',
        title=f'Snapshot {st.session_state.last_data .timestamp.max().strftime("%Y-%m-%d %H:%M")} UTC',
        map=dict(
            bearing=0,
            center=go.layout.map.Center(
                lat=59.831852,
                lon=25.118242
            ),
            pitch=0,
            zoom=5
        ),
        height=600,  
        
        # Legend vertical position
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99,
            orientation="v"
        )   

    )


    st.plotly_chart(fig)


    #------------ Timeseries plot ------------

    ts_data = tag_data(st.session_state.data, countries=country_selection)
    # Keep only vesells in box
    ts_data = ts_data.query(f'latitude > {LAT_BOX[0]} & latitude < {LAT_BOX[1]} & longitude > {LON_BOX[0]} & longitude < {LON_BOX[1]}')
    ts_data = ts_data.query('tag == True').groupby('timestamp_hourly').count()['tag'].resample('h').mean().fillna(0).reset_index()
    ts_data.sort_values('timestamp_hourly', inplace=True)

    # Plot area
    fig = px.line(ts_data, x='timestamp_hourly', y='tag',
                    markers=True,
                    )
    fig.update_xaxes(title_text='')
    fig.update_yaxes(title_text='# careless vessels')
    fig.update_layout(title='Ooops Index')


    # plot first time
    if 'ts_plot' not in st.session_state:
        st.session_state.ts_plot = fig

    # update plot
    if update_data:
        st.session_state.ts_plot = fig

    st.plotly_chart(fig)


    with c2_metric:
        window = 12
        value = round(ts_data['tag'].iloc[-window:].mean(), 1)
        delta = np.round(value - ts_data['tag'].iloc[-2*window:-window].mean(), 1)
        st.metric(label=f'Average Index last {window}h', value=value, border= True, delta=delta) 


    with c1_metric:
        value = ts_data['tag'].iloc[-1]
        delta = np.round(value - ts_data['tag'].iloc[-window:-1].mean(), 1)
        st.metric(label='Current value', value=value, border= True, delta=delta)
