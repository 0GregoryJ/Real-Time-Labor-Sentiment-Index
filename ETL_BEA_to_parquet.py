import requests
import pandas as pd

#-----BEA API request------
params = {
    "UserID": "6EC25985-5ED6-4FEB-BE7E-CFAB9E02E38D",
    "method": "GetData",
    "datasetname": "NIPA",
    "TableName": "T20305",   # Table 2.3.5 PCE
    "Frequency": "Q",        # quarterly
    "Year": "2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025,2026",           # Or "ALL" for all years
    "ResultFormat": "JSON"
}

response = requests.get("https://apps.bea.gov/api/data", params=params)
BEA_response = response.json()

# Extract the actual data rows
rows = BEA_response["BEAAPI"]["Results"]["Data"]

BEA_spending_data = pd.DataFrame(rows)

#-----clean, write to parquet------
#date column
BEA_spending_data["TimePeriod"] = pd.to_datetime(BEA_spending_data["TimePeriod"])
BEA_spending_data["DataValue"] = (
    BEA_spending_data["DataValue"]
    .str.replace(",", "", regex=False)
    .astype(float)
)
BEA_spending_data = BEA_spending_data.rename(columns={
    "TimePeriod": "date",
    "LineDescription": "query",
    "DataValue": "value"
})
#add cateogory column
BEA_spending_data["category"] = "consumer_spending"
#drop columns
BEA_spending_data.drop(columns = ["TableName", "SeriesCode", "LineNumber", "UNIT_MULT", "METRIC_NAME", "CL_UNIT", "NoteRef"], inplace=True)
#write to parquet
BEA_spending_data.to_parquet("/Users/gregoryjoshua/Desktop/Projects/SearchSentimentDashboard/data_parquets/BEA_data.parquet", index=False)