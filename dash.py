import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
import numpy as np
import requests
import json

#page layout
st.set_page_config(layout="wide")

#define SQL engine
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
FROM bls_labor_data
ORDER BY date
"""

google_search_data_query = """
SELECT *
FROM google_trends_timeseries
ORDER BY date
"""

google_search_data = pd.DataFrame(pd.read_sql(google_search_data_query, search_trend_data))

bls_data = pd.DataFrame(pd.read_sql(bls_data_query, bls_data))

#mlvoca LLM API
mlvoca_url = "https://mlvoca.com/api/generate"

#analysis
#search sentiment index
search_pivot = google_search_data.pivot(index="date", columns="query", values="value").sort_index()
search_index_z = (search_pivot - search_pivot.mean())/search_pivot.std()
SAI = search_index_z.mean(axis=1).to_frame("SAI")
current_SAI = np.round(SAI.iloc[-1].values, decimals = 2)
delta_2W_SAI = float(np.round(((current_SAI - SAI.iloc[-3].values) / current_SAI), decimals = 2))
#line chart
SAI_line_chart = px.line(SAI, x=SAI.index, y = "SAI")

#labor market stress index
bls_pivot = bls_data.pivot(index="date", columns="series_id", values = "value")
bls_index_z = (bls_pivot - bls_pivot.mean()) / bls_pivot.std()
LMSI = bls_index_z.mean(axis=1).to_frame("LMSI")
current_LMSI = np.round(LMSI.iloc[-1].values, decimals = 2)
delta_2W_LMSI = float(np.round(((current_LMSI - LMSI.iloc[-3].values) / current_LMSI), decimals = 2))
#line chart
LMSI_line_chart = px.line(LMSI, x=LMSI.index, y="LMSI")

#gap index (sentiment - stress)
SAI_LMSI_combined = LMSI.join(SAI, how="outer").sort_index()

#GAP area chart
#function to add fill between lines in chart
def add_fill_between(
    fig: go.Figure,
    df: pd.DataFrame,
    col_a: str,
    col_b: str,
    fillcolor="rgba(0,120,255,0.25)",
    max_gap=None,   # e.g. "45D" to NOT fill across gaps larger than 45 days; None = fill across all gaps
):
    # ensure datetime index
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("df.index must be a DatetimeIndex for time interpolation.")

    # build "connectgaps-equivalent" series for filling
    tmp = df[[col_a, col_b]].copy()

    # optional: prevent filling across very large missing spans
    if max_gap is not None:
        # mark points where time step is too large -> split the series into segments
        dt = tmp.index.to_series().diff()
        split = (dt > pd.Timedelta(max_gap)).cumsum()
    else:
        split = pd.Series(0, index=tmp.index)

    for _, grp_idx in tmp.groupby(split).groups.items():
        g = tmp.loc[grp_idx]
        # time interpolation mimics straight segments between known points
        tmp.loc[grp_idx, [col_a, col_b]] = (
            g[[col_a, col_b]]
            .astype(float)
            .interpolate(method="time", limit_area="inside")
        )

    # after interpolation, drop any remaining rows where either is NaN (edges with no bracketing points)
    g = tmp.dropna(subset=[col_a, col_b])
    if len(g) < 2:
        return

    x = g.index
    y1 = g[col_a].to_numpy(float)
    y2 = g[col_b].to_numpy(float)

    # Fill polygon between max/min so crossings are handled automatically
    ytop = np.maximum(y1, y2)
    ybot = np.minimum(y1, y2)

    poly_x = np.concatenate([x, x[::-1]])
    poly_y = np.concatenate([ytop, ybot[::-1]])

    fig.add_trace(go.Scatter(
        x=poly_x,
        y=poly_y,
        fill="toself",
        fillcolor=fillcolor,
        line=dict(color="rgba(0,0,0,0)"),
        hoverinfo="skip",
        showlegend=False
    ))

GAP_area_chart = go.Figure()

add_fill_between(GAP_area_chart, SAI_LMSI_combined, "SAI", "LMSI", fillcolor="rgba(255, 171, 150, 0.2)")

GAP_area_chart.add_trace(go.Scatter(
    x=SAI_LMSI_combined.index,
    y=SAI_LMSI_combined["SAI"],
    mode="lines",
    name="Search Anxiety Index",
    line=dict(color='rgba(255, 65, 17, 0.8)'), # Line color
    connectgaps=True
))
GAP_area_chart.add_trace(go.Scatter(
    x=SAI_LMSI_combined.index,
    y=SAI_LMSI_combined["LMSI"],
    name="Labor Market Stress Index",
    mode="lines",
    line=dict(color='rgba(255, 170, 114, 1)', shape="linear"), # Line color (can be transparent)
    connectgaps=True
))
GAP_area_chart.update_xaxes(range=["2021-01-01", SAI_LMSI_combined.index.max()])
GAP_area_chart.update_layout(margin=dict(t=0, b=0, l=0, r=0), width = 500)

#use mlvoca for gen AI data analysis
so_what_mlvoca_prompt = f"In 1 paragraph, answer the question, 'so what?' for data shows current real time sentiment of google searches is rated at {current_SAI} while BLS data that is a couple months behind rates the labor market stress at {current_LMSI}. Mention these two figures and reason what their difference means for the labor market and shareholders. Do not repeat any of what i just said in your answer, just answer the question."

mlvoca_request = {
    "model": "tinyllama",
    "prompt": so_what_mlvoca_prompt
}

mlvoca_response = ""

with requests.post(mlvoca_url, json=mlvoca_request, stream=True, timeout=60) as r:
    r.raise_for_status()

    buffer = ""

    for chunk in r.iter_content(chunk_size=None):
        if not chunk:
            continue

        buffer += chunk.decode("utf-8", errors="ignore")

        normalized = buffer.replace("}\n{", "}|{") \
                           .replace("} {", "}|{") \
                           .replace("}{", "}|{")

        parts = normalized.split("|")
        buffer = parts.pop()

        for part in parts:
            part = part.strip()
            if not part:
                continue

            obj = json.loads(part)
            mlvoca_response += obj.get("response", "")

            if obj.get("done") is True:
                break

#display
st.markdown("""
        <style>
                
                [data-testid="stTextInput"] p {
                    font-size: 1.5rem;
                    font-weight: bold;
                }
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

st.markdown("<h1>Real-Time Labor Sentiment Index</h1>", unsafe_allow_html=True)
left, right = st.columns([3,2], gap="large")

with left:
    left_subcolumn1, left_subcolumn2 = st.columns([1,4])
    left_subcolumn1.metric("Search Anxiety Index", current_SAI, delta=f"{delta_2W_SAI}% last 2W", delta_color="inverse")
    left_subcolumn2.metric("Labor Market Stress Index", current_LMSI, delta=f"{delta_2W_LMSI}% last 2W", delta_color="inverse")

    st.plotly_chart(GAP_area_chart, use_container_width=True, config={'displaylogo': False})
with right:
    st.markdown("""
                <h2>So what? <span style="font-size: 1rem; font-weight: bold">(AI Generated)</span)</h2>
                """, unsafe_allow_html=True)
    st.markdown(mlvoca_response)
    mlvoca_followup = st.text_input("Ask a followup (coming soon):")
