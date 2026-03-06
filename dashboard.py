import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st

#-----read data into pandas------
FRED_data = pd.read_parquet("/Users/gregoryjoshua/Desktop/Projects/SearchSentimentDashboard/data_parquets/FRED_data.parquet")
bls_data = pd.read_parquet("/Users/gregoryjoshua/Desktop/Projects/SearchSentimentDashboard/data_parquets/bls_data.parquet")
serpAPI_data = pd.read_parquet("/Users/gregoryjoshua/Desktop/Projects/SearchSentimentDashboard/data_parquets/search_data.parquet")
bea_data = pd.read_parquet("/Users/gregoryjoshua/Desktop/Projects/SearchSentimentDashboard/data_parquets/BEA_data.parquet")
#-----data analysis------
#transform BLS labor market data before adding to master df
bls_data_transformed = bls_data.copy()
bls_data_transformed["value"] = pd.to_numeric(bls_data_transformed["value"], errors="coerce")
#transform CES0000000001 (payroll employment) into pct change
payroll_emp_mask = bls_data_transformed["query"] == "CES0000000001"
bls_data_transformed.loc[payroll_emp_mask, "value"] = (
    bls_data_transformed.loc[payroll_emp_mask]
    .groupby("query")["value"]
    .transform(lambda x: x.pct_change())
)
#transform CES0500000003 (average hourly earnings) into pct change yearly
avg_hourly_earn_mask = bls_data_transformed["query"] == "CES0500000003"
bls_data_transformed.loc[avg_hourly_earn_mask, "value"] = (
    bls_data_transformed.loc[avg_hourly_earn_mask]
    .groupby("query")["value"]
    .transform(lambda x: x.pct_change(12))
)
#transform CES0500000002 (average weekly hours) into monthly change
avg_weekly_hrs_mask = bls_data_transformed["query"] == "CES0500000002"
bls_data_transformed.loc[avg_weekly_hrs_mask, "value"] = (
    bls_data_transformed.loc[avg_weekly_hrs_mask]
    .groupby("query")["value"]    
    .transform(lambda x: x.diff())
)
#transform LNS14000000 (unemployment rate) by inverting level
unmp_rate_mask = bls_data_transformed["query"] == "LNS14000000"
bls_data_transformed.loc[unmp_rate_mask, "value"] = (
    bls_data_transformed.loc[unmp_rate_mask]
    .groupby("query")["value"]
    .transform(lambda x: -x)
)
#transform LNS13000000 (unemployment level) into pct change and invert
unmp_level_mask = bls_data_transformed["query"] == "LNS13000000"
bls_data_transformed.loc[unmp_level_mask, "value"] = (
    bls_data_transformed.loc[unmp_level_mask]
    .groupby("query")["value"]
    .transform(lambda x: -x.pct_change())
)
#combine parquets into single dataframe
master_data = pd.concat([bls_data_transformed, serpAPI_data, bea_data, FRED_data], ignore_index=True)   
#composite search frequency for labor market ((x-mean)/sd) (z-score)
labor_market_queries = [
    "job cuts", 
    "layoffs", 
    "second job", 
    "unemployment benefits", 
    "credit card minimum payment", 
    "side hustle"
]
master_data["labor_market_search_zscores"] = master_data[master_data["query"].isin(labor_market_queries)].groupby("query")["value"].transform(lambda x: (x - x.mean()) / x.std() if x.std() != 0 else 0)
master_data["labor_market_search_stress_composite"] = master_data[master_data["query"].isin(labor_market_queries)].groupby("date")["labor_market_search_zscores"].transform("mean")
#composite BLS labor market data ((x-mean)/sd) (z-score) - first transform vars then create composite
bls_queries = [
    "CES0000000001", #payroll employment
    "CES0500000003", #average hourly earnings
    "CES0500000002", #average weekly hours
    "LNS14000000", #unemployment rate
    "LNS13000000", #unemployment level
]
master_data["bls_zscores"] = master_data[master_data["query"].isin(bls_queries)].groupby("query")["value"].transform(lambda x: (x - x.mean()) / x.std() if x.std() != 0 else 0)
master_data["bls_composite"] = master_data[master_data["query"].isin(bls_queries)].groupby("date")["bls_zscores"].transform("mean")

#composite search frequency for consumer spending ((x-mean)/sd) (z-score)
consumer_spending_queries = [
    "mortgage preapproval", 
    "new car deals", 
    "flight deals", 
    "credit card application", 
    "kitchen remodel"
]
master_data["consumer_spending_search_zscores"] = master_data[master_data["query"].isin(consumer_spending_queries)].groupby("query")["value"].transform(lambda x: (x - x.mean()) / x.std() if x.std() != 0 else 0)
master_data["consumer_spending_search_composite"] = master_data[master_data["query"].isin(consumer_spending_queries)].groupby("date")["consumer_spending_search_zscores"].transform("mean")

#composite BEA consumer spending data ((x-mean)/sd) (z-score)
master_data["bea_zscores"] = master_data[master_data["query"] == "Personal consumption expenditures (PCE)"].groupby("query")["value"].transform(lambda x: (x - x.mean()) / x.std() if x.std() != 0 else 0)
master_data["bea_composite"] = -1* master_data[master_data["query"] == "Personal consumption expenditures (PCE)"].groupby("date")["bea_zscores"].transform("mean")

#composite FRED consumer spending data ((x-mean)/sd) (z-score)
master_data["fred_zscores"] = master_data[master_data["query"] == "FRED Personal Consumption Expenditures (PCE)"].groupby("query")["value"].transform(lambda x: (x - x.mean()) / x.std() if x.std() != 0 else 0)
master_data["fred_composite"] = master_data[master_data["query"] == "FRED Personal Consumption Expenditures (PCE)"].groupby("date")["fred_zscores"].transform("mean")
#function to generate main chart based on dropdown selections
def generate_main_chart(data_selection, time_selection):
    #data selection
    data_selection_dict = {
        "Labor Market: Search Sentiment vs. BLS Data": ["labor_market_search_stress_composite", "bls_composite"],
        "Consumer Spending: Search Sentiment vs. BEA Data": ["consumer_spending_search_composite", "fred_composite"],
        "Cost of Living: Search Sentiment vs. CPI Data": "cost_of_living"
    }
    cols = data_selection_dict[data_selection]
    cols = cols if isinstance(cols, list) else [cols]

    #range selection
    end_date = master_data["date"].max()
    if time_selection == "2 weeks":
        start_date = end_date - pd.DateOffset(weeks=2)

    elif time_selection == "1 month":
        start_date = end_date - pd.DateOffset(months=1)

    elif time_selection == "1 year":
        start_date = end_date - pd.DateOffset(years=1)

    elif time_selection == "5 years (if available)":
        start_date = end_date - pd.DateOffset(years=5)
    filtered_data = (
        master_data.loc[master_data["date"] >= start_date, ["date"] + cols]
        .groupby("date", as_index=False)[cols]
        .mean()
        .sort_values("date")
    )
    #create chart
    main_line_chart = px.line(
        filtered_data.sort_values("date"),
        x="date",
        y=cols,
        labels= {
            "date": "Date",
            "value": "Deviations from historical average",
        }
    )
    #connect gaps
    main_line_chart.update_traces(connectgaps=True)
    #legend names and title + chart title font and margin + legend position
    legend_remapping = {
        "labor_market_search_stress_composite": "Labor Market Stress Search Sentiment",
        "consumer_spending_search_composite": "Consumer Spending Search Sentiment",
        "bea_composite": "BEA Consumer Spending Data",
        "bls_composite": "BLS Labor Market Stress Composite",
        "fred_composite": "FRED Consumer Spending Data",
    }
    main_line_chart.for_each_trace(lambda t: t.update(name=legend_remapping.get(t.name, t.name)))
    main_line_chart.update_layout(
        legend_title_text="Series",
        title ={
            "text": f"{data_selection} over the last {time_selection}",
            "font": {"size": 18},
        },
        margin=dict(l=40, r=20, t=30, b=40),
        legend=dict(
        x=0.01,          # left side
        y=0.95,          # top
        xanchor="left",
        yanchor="top",
        )
    )

    return main_line_chart
#------display------
#page layout
st.set_page_config(layout="wide", page_title="Sentiment-Reporting Gap Dashboard")#page styling
st.markdown("""
    <head>
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Alfa+Slab+One&display=swap" rel="stylesheet">
        <style>
        /* Remove blank space at top and bottom */ 
        .block-container {
            padding-top: 1%;
            padding-bottom: 1%;
        }
        /*center sidebar header*/
        .logo-container {
            margin-top: 0;
            text-align: center;
            display: grid;
        }
            
        /* Disable sidebar scrollbar */
        section[data-testid="stSidebar"] div:first-child {
            overflow: hidden !important;
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
        ("Labor Market: Search Sentiment vs. BLS Data", "Consumer Spending: Search Sentiment vs. BEA Data", "Cost of Living: Search Sentiment vs. CPI Data"),
        index=None,
        placeholder="Select a data category"
    )
    time_selection = st.selectbox(
        "Time Range:",
        ("2 weeks", "1 month", "1 year", "5 years (if available)"),
        index=None,
        placeholder="Select a time range"
    )
    #about this project
    st.markdown("""
                <div style="margin-top: 5%;">
                    <h2>About This Project</h2>
                    <p>I created this real-time dashboard to index the "Sentiment-Reporting Gap," which points to the gap between what official data/reporting says about a topic and what Google search sentiment says. Read details <a href="https://github.com/0GregoryJ/Real-Time-Labor-Sentiment-Index/blob/main/README.md">here</a> or get started above.</p>
                </div>
                """, unsafe_allow_html=True)
    
#body
st.title("The Sentiment-Reporting Gap")
if data_selection == None and time_selection == None:
    st.markdown("<h1 style='text-align: center; margin-top: 20%; color: #A9A9A9'>Select a data category and time range to get started!</h1>", unsafe_allow_html=True)
elif data_selection == None and time_selection != None:
    st.markdown("<h1 style='text-align: center; margin-top: 20%; color: #A9A9A9'>Select a data category to get started!</h1>", unsafe_allow_html=True)
elif time_selection == None and data_selection != None:
    st.markdown("<h1 style='text-align: center; margin-top: 20%; color: #A9A9A9'>Select a time range to get started!</h1>", unsafe_allow_html=True)
else:
    #line chart of filtered dataframe
    st.markdown("<div style='margin-top: 30px;'></div>",unsafe_allow_html=True)
    st.plotly_chart(generate_main_chart(data_selection, time_selection), use_container_width=True)
    st.markdown("""
        <div style="margin-top: 1%;">
            <h2>Interpreting the Chart</h2>
            <p>The chart above shows the selected search sentiment composite and official data composite over the selected time range. The y-axis represents deviations from the historical average (z-scores) for each series, allowing for easier comparison of trends. A rising line indicates increasing search sentiment or official data values relative to the historical average. 
    """, unsafe_allow_html=True)
