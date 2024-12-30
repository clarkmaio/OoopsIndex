import streamlit as st

st.set_page_config(initial_sidebar_state='collapsed', page_icon=':ship:', page_title='Oops!')
st.markdown("<h1 style='text-align: center; color: black;'>Oops Index</h1>", unsafe_allow_html=True)




with st.sidebar:
    st.multiselect(label='Countries', options=['RU', 'CN'], default=['RU'])


#------------ Row 1 ------------
# Plot
import plotly.express as px
import pandas as pd

df = pd.DataFrame({
    'x': [1, 2, 3, 4],
    'y': [10, 11, 12, 13]
})

fig = px.line(df, x='x', y='y')
st.plotly_chart(fig)



#------------ Row 2 ------------
c1, c2 = st.columns([3, 1])
with c1:
    st.write('Hello, world!')

with c2:
    st.metric(label='Oops! index', value=1, delta=0.1, delta_color='normal', border=True)