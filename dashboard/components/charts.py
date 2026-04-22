import plotly.express as px
import streamlit as st

def generate_main_chart(filtered_data, data_selection, range_selection):
    main_chart = px.line(
        filtered_data,
        x="date",
        y=f"{data_selection}_gap",
    )
    #chart title
    title_dict = {
        "labor": f"Labor Market Stress: Sentiment-Reporting Gap Over the Last {range_selection}",
        "spending": f"Consumer Spending: Sentiment-Reporting Gap Over the Last {range_selection}",
    }
    main_chart.update_layout(title=title_dict[data_selection])
    
    #remove gridlines
    main_chart.update_yaxes(showgrid=False)

    #remove axis labels
    main_chart.update_layout(xaxis_title=None, yaxis_title=None)

    #y-axis scale
    main_chart.update_yaxes(range=[-100,100])

    st.plotly_chart(main_chart, use_container_width=True)

def generate_mini_chart(filtered_data, keyword):
    if keyword not in filtered_data.columns:
        st.warning(f"Missing column '{keyword}' in dataset; skipping this chart.")
        return

    mini_chart = px.line(
        filtered_data,
        x="date",
        y=keyword,
    )
    #chart title
    title_and_label_dict = {
        "LNS14000000": ["Unemployment Rate", "Percent"],
        "CES0500000002": ["Average weekly hours", "Hours"],
        "CES0000000001": ["Payroll Employment", "Thousands of Paid Workers"],
        "unemployment_benefits": ['"Unemployment Benefits"', "Google Trends Units"],
        "second_job": ['"Second Job"', "Google Trends Units"],
        "layoffs": ['"Layoffs"', "Google Trends Units"],
        "credit_card_application": ['"Credit Card Application"', "Google Trends Units"],
        "kitchen_remodel": ['"Kitchen Remodel"', "Google Trends Units"],
        "flight_deals": ['"Flights Deals"', "Google Trends Units"],
        "spending_reported_sentiment": ["FRED Personal Comsumption (Standardized)", ""]
    }
    mini_chart.update_layout(title=title_and_label_dict[keyword][0])
    
    #remove gridlines
    mini_chart.update_yaxes(showgrid=False)

    #remove axis labels
    mini_chart.update_layout(xaxis_title=None, yaxis_title=title_and_label_dict[keyword][1])

    #connectgaps
    mini_chart.update_traces(connectgaps = True)

    #y-axis scale
    mini_chart.update_yaxes(range=[0,100])

    st.plotly_chart(mini_chart, use_container_width=True)