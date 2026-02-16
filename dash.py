import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
import numpy as np

#define engine
MYSQL_USER = "root"
MYSQL_PASSWORD = "Gj10077436"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
bls_db = "bls"
search_db = "search_trend_data"

bls_url = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{bls_db}"
)

search_url = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{search_db}"
)

bls_data = create_engine(bls_url)
search_trend_data = create_engine(search_url)

#read data into pandas
bls_data_query = """
SELECT *
FROM bls_timeseries
ORDER BY date
"""

google_search_data_query = """
SELECT *
FROM google_trends_timeseries
ORDER BY date
"""



google_search_data = pd.DataFrame(pd.read_sql(google_search_data_query, search_trend_data))

bls_data = pd.DataFrame(pd.read_sql(bls_data_query, bls_data))

#analysis

search_pivot = google_search_data.pivot(index="date", columns="query", values="value").sort_index()
search_index_z = (search_pivot - search_pivot.mean())/search_pivot.std()
SAI = search_index_z.mean(axis=1).to_frame("SAI")
current_SAI = np.round(SAI.iloc[-1].values, decimals = 2)
delta_2W_SAI = float(np.round(((current_SAI - SAI.iloc[-3].values) / current_SAI), decimals = 2))

SRI = 5
LMSI = -12

SRI_delta = 5
LMSI_delta = -3

#display
st.set_page_config(layout="wide")

st.markdown("""
        <style>
               /* Remove blank space at top and bottom */ 
               .block-container {
                   padding-top: 1%;
                   padding-bottom: 1%;
                }
                /* Adjust label font size */
                [data-testid="stMetricLabel"]:nth-of-type(1,2) p {
                    font-size: 12px;
                }

                /* Adjust value font size */
                [data-testid="stMetricValue"] {
                    font-size: 60px;
                }
        </style>
        """, unsafe_allow_html=True)

st.title("Real-Time Labor Sentiment Index")
st.markdown("Using real time Google search trends and BLS data to forecast labor market downturns.")

col1, col2, col3, col4 = st.columns([1,1,1,4])

with col1:
    st.metric("Sentiment-Market Gap", SRI, delta=f"{SRI_delta}% last 2W", delta_color="inverse")

with col2:
    st.metric("Search Anxiety Index", current_SAI, delta=f"{delta_2W_SAI}% last 2W", delta_color="inverse")

with col3:
    st.metric("Labor Market Stress Index", LMSI, delta=f"{LMSI_delta}% last 2W", delta_color="inverse")