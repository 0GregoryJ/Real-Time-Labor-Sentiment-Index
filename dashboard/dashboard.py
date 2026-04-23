import streamlit as st
from components.charts import generate_main_chart, generate_mini_chart
from utils.db import run_query
from datetime import datetime, timedelta
from pathlib import Path

#page styling
sec_color = st.get_option("theme.secondaryBackgroundColor") or "#f0f2f6"
text_color = st.get_option("theme.textColor")
st.set_page_config(layout="wide", page_title="Sentiment-Reporting Gap Dashboard")
st.markdown("""
    <head>
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Alfa+Slab+One&display=swap" rel="stylesheet">
        <style>
        /* Remove blank space at top and bottom */ 
        .block-container {
            padding-top: 2%;
            padding-bottom: 1%;
        }
        /*center sidebar header*/
        .logo-container {
            margin-top: 0;
            text-align: center;
            display: grid;
        }
        /*metric size*/
        [data-testid="stMetricValue"] {
            font-size: 3.5em;
        }
        /*remove anchors*/
        [data-testid='stHeaderActionElements'] {
            display: none;
        }
        </style>
    </head>
    """, unsafe_allow_html=True)

#sidebar
with st.sidebar:
    #logo
    st.markdown("""
                <div class="logo-container"
                    <span style="font-size: 2rem; font-family: 'Alfa Slab One'; font-style: normal; font-weight: 400">Gregory</br>Joshua</span> </br>
                    <span style="font-size: 1rem; font-weight: 100; font-family: Sans-Serif">Portfolio Project</span>
                </div>
                </br>
                """, unsafe_allow_html=True)
    #code/github/linkedin links
    st.markdown("""
                <div style="text-align: center;margin-bottom: 5%"><a href="https://github.com/0GregoryJ/Real-Time-Labor-Sentiment-Index/tree/main">Source code</a> | <a href="https://linkedin.com/in/gregjoshua">LinkedIn</a> | <a href="https://github.com/0GregoryJ">GitHub</a></div>
                """, unsafe_allow_html=True)
    #dropdown selections
    st.markdown("<h2>Get Started</h2>", unsafe_allow_html=True)
    data_selection = st.selectbox(
        "Data Category:",
        ("Labor Market Stress", "Consumer Spending"),
        index=None,
        placeholder="Select a data category"
    )
    range_selection = st.selectbox(
        "Time Range:",
        ("6 months", "1 year", "5 years", "Max"),
        index=None,
        placeholder="Select a time range"
    )
    #about this project
    st.markdown("""
                <div style="margin-top: 5%;">
                    <h2>About This Project</h2>
                    <p>This real-time dashboard indexes the "Sentiment-Reporting Gap," which shows the gap between what official data/reporting says about a topic and what Google search sentiment says. Read details <a href="https://github.com/0GregoryJ/Real-Time-Labor-Sentiment-Index/blob/main/README.md">here</a> or get started above.</p>
                </div>
                """, unsafe_allow_html=True)
    
#body
st.title("The Sentiment-Reporting Gap")

#title
st.markdown("""
            <h5 style="font-weight: 100">Visualizing the gap between offical reported data and Google search sentiment.</h5>
            <hr style="margin-top:5px; margin-bottom:15px">
            """, unsafe_allow_html=True)

#content
if data_selection == None and range_selection == None:
    st.markdown("<h1 style='text-align: center; margin-top: 20%; color: #A9A9A9'>Select a data category and time range to get started!</h1>", unsafe_allow_html=True)
elif data_selection == None and range_selection != None:
    st.markdown("<h1 style='text-align: center; margin-top: 20%; color: #A9A9A9'>Select a data category to get started!</h1>", unsafe_allow_html=True)
elif range_selection == None and data_selection != None:
    st.markdown("<h1 style='text-align: center; margin-top: 20%; color: #A9A9A9'>Select a time range to get started!</h1>", unsafe_allow_html=True)
else:
    #selection dictionary
    selection_dict = {
            "Labor Market Stress": ["labor", ["LNS14000000", "CES0500000002", "CES0500000003", "unemployment_benefits", "second_job", "layoffs"]],
            "Consumer Spending": ["spending", ["spending_reported_sentiment", "kitchen_remodel", "credit_card_application", "flight_deals"]],
            "6 months": datetime.today() - timedelta(days=180),
            "1 year": datetime.today() - timedelta(days=365),
            "5 years": datetime.today() - timedelta(days=1825),
            "Max": datetime(1900, 1, 1)
    }
    #get selected data and range
    filtered_data_query = Path(f"dashboard/queries/get_{selection_dict[data_selection][0]}_features.sql").read_text()
    filtered_data = run_query(filtered_data_query, [selection_dict[range_selection]])
    #current metrics
    current_metrics = (
        filtered_data[f"{selection_dict[data_selection][0]}_search_sentiment"].iloc[-2],
        filtered_data[f"{selection_dict[data_selection][0]}_reported_sentiment"].iloc[-2]
        )
    metric_deltas = (
        filtered_data[f"{selection_dict[data_selection][0]}_search_sentiment"].iloc[-2] - filtered_data[f"{selection_dict[data_selection][0]}_search_sentiment"].iloc[-4],
        filtered_data[f"{selection_dict[data_selection][0]}_reported_sentiment"].iloc[-2] - filtered_data[f"{selection_dict[data_selection][0]}_reported_sentiment"].iloc[-4]
        )
    with st.container():
        mcol1, mcol2, mcol3 = st.columns([1,1,3]) 
        with mcol1:
            st.metric(
                "Google Search Sentiment",
                int(current_metrics[0]),
                delta = f"{int(metric_deltas[0])} since last month",
                delta_color="inverse" if data_selection == "Labor Market Stress" else "normal",
                )
        with mcol2:
            st.metric(
                "Reported Sentiment",
                int(current_metrics[1]),
                delta = f"{int(metric_deltas[1])} since last month",
                delta_color="inverse" if data_selection == "Labor Market Stress" else "normal"
                )
        with mcol3:
            #chart explainer
            st.markdown(f"""
                        <div style="
                            background-color: {sec_color};
                            padding: 20px;
                            border-radius: 10px;
                            color: #000;
                            border: 1px solid rgba(0,0,0,0.05);
                        ">
                            Below, the y-axis represents <strong>Google search trend frequency -  official reporting</strong>, so when sentiment about a topic is more intense than what's being reported, expect high values.
                        </div>
                        """, unsafe_allow_html=True)

    #main chart
    generate_main_chart(filtered_data, selection_dict[data_selection][0], range_selection)
    
    #individual frequency charts
    st.subheader("Individual Search Frequencies")
    st.markdown('<h6 style="font-weight: 100">Visualize some of the individual search frequencies that compose the search sentiment data in the chart above.</h6><hr style="margin-top:5px; margin-bottom:15px">', unsafe_allow_html=True)
    IScol1,IScol2,IScol3 = st.columns([1,1,1])
    if data_selection == "Labor Market Stress":
        with IScol1:
                generate_mini_chart(filtered_data, selection_dict[data_selection][1][3])
        with IScol2:
                generate_mini_chart(filtered_data, selection_dict[data_selection][1][4])
        with IScol3:
                generate_mini_chart(filtered_data, selection_dict[data_selection][1][5])
    elif data_selection == "Consumer Spending":
        with IScol1:
                generate_mini_chart(filtered_data, selection_dict[data_selection][1][1])
        with IScol2:
                generate_mini_chart(filtered_data, selection_dict[data_selection][1][2])
        with IScol3:
                generate_mini_chart(filtered_data, selection_dict[data_selection][1][3])
    
    #individual reported data points
    st.subheader("Individual Reported Data Series")
    if data_selection == "Labor Market Stress":
        st.markdown('<h6 style="font-weight: 100">Visualize some of the individual reported data series that constitute the reported data composite in the chart above.</h6><hr style="margin-top:5px; margin-bottom:15px">', unsafe_allow_html=True)
        IRcol1,IRcol2,IRcol3 = st.columns([1,1,1])
        IRcol1,IRcol2,IRcol3 = st.columns([1,1,1])
        with IRcol1:
                generate_mini_chart(filtered_data, selection_dict[data_selection][1][0])
        with IRcol2:
                generate_mini_chart(filtered_data, selection_dict[data_selection][1][1])
        with IRcol3:
                generate_mini_chart(filtered_data, selection_dict[data_selection][1][2])
    elif data_selection == "Consumer Spending":
        st.markdown('<h6 style="font-weight: 100">Visualize the standardized reported consumer spending data..</h6><hr style="margin-top:5px; margin-bottom:15px">', unsafe_allow_html=True)
        generate_mini_chart(filtered_data, selection_dict[data_selection][1][0])
    #insights
    st.subheader("Insights")
    st.markdown("""<h6 style="font-weight: 100">Insights about what today's gap means, generated per session, powered by Llama 3. (In construction!)</h6><hr style="margin-top:5px; margin-bottom:15px">""", unsafe_allow_html=True)
