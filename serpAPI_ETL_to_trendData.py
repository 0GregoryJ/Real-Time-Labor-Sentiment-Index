from serpapi import GoogleSearch
import pandas as pd
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.mysql import insert

#request data
layoffs_request = {
    "engine": "google_trends",
    "q": "layoffs",
    "date": "today 5-y",
    "data_type": "TIMESERIES",
    "api_key": "5c597ac025dfa95818dfafe4feb46f8028f80bca3e5ed9777ed7e311d95eea67",
}

umplymt_benefits_request = {
    "engine": "google_trends",
    "q": "unemployment benefits",
    "date": "today 5-y",
    "data_type": "TIMESERIES",
    "api_key": "5c597ac025dfa95818dfafe4feb46f8028f80bca3e5ed9777ed7e311d95eea67",
}

job_cuts_request = {
    "engine": "google_trends",
    "q": "job cuts",
    "date": "today 5-y",
    "data_type": "TIMESERIES",
    "api_key": "5c597ac025dfa95818dfafe4feb46f8028f80bca3e5ed9777ed7e311d95eea67",
}

second_job_request = {
    "engine": "google_trends",
    "q": "second job",
    "date": "today 5-y",
    "data_type": "TIMESERIES",
    "api_key": "5c597ac025dfa95818dfafe4feb46f8028f80bca3e5ed9777ed7e311d95eea67",
}

side_hustle_request = {
    "engine": "google_trends",
    "q": "side hustle",
    "date": "today 5-y",
    "data_type": "TIMESERIES",
    "api_key": "5c597ac025dfa95818dfafe4feb46f8028f80bca3e5ed9777ed7e311d95eea67",
}

CCMP_request = {
    "engine": "google_trends",
    "q": "credit card minimum payment",
    "date": "today 5-y",
    "data_type": "TIMESERIES",
    "api_key": "5c597ac025dfa95818dfafe4feb46f8028f80bca3e5ed9777ed7e311d95eea67",
}

#search and store results
layoffs_search = GoogleSearch(layoffs_request)

layoffs_results = layoffs_search.get_dict()


umplymt_benefits_search = GoogleSearch(umplymt_benefits_request)

umplymt_benefits_results = umplymt_benefits_search.get_dict()


job_cuts_search = GoogleSearch(job_cuts_request)

job_cuts_results = job_cuts_search.get_dict()


second_job_search = GoogleSearch(second_job_request)

second_job_results = second_job_search.get_dict()


side_hustle_search = GoogleSearch(side_hustle_request)

side_hustle_results = side_hustle_search.get_dict()


CCMP_search = GoogleSearch(CCMP_request)

CCMP_results = CCMP_search.get_dict()

#function gets df from serpapi results
def serpapi_to_df(results):
    IOT = results["interest_over_time"]
    timeline = IOT["timeline_data"]

    rows = []
    
    for point in timeline:
        #skip parial data
        if point.get("partial_data", False):
            continue
        #convert serpapi unix timestamp to datetime
        date = pd.to_datetime(
            int(point["timestamp"]),
            unit="s",
            utc = True
        ).tz_convert(None)
        #get info
        for v in point["values"]:
            rows.append({
                "date": date,
                "query": v["query"],
                "value": int(v["extracted_value"]),
            })

    return (
        pd.DataFrame(rows)
          .sort_values("date")
          .reset_index(drop=True)
    )


#connect to SQL database - "search_trend_data"
DATABASE_URL = (
    "mysql+pymysql://root:Gj10077436@localhost:3306/search_trend_data"
)

engine = create_engine(DATABASE_URL, future=True)

Base = declarative_base()

#define table
class GoogleTrendsTimeSeries(Base):
    __tablename__ = "google_trends_timeseries"

    id = Column(Integer, primary_key=True)
    query = Column(String(200), nullable=False)
    date = Column(DateTime, nullable=False)   # weekly bucket start
    value = Column(Integer, nullable=False)   # 0–100 Google Trends index
    source = Column(String(50), nullable=False, default="serpapi")

    __table_args__ = (
        UniqueConstraint("query", "date", name="uq_query_date"),
    )

#create table if doesn't exist
Base.metadata.create_all(engine)

#function upserts df with columns: date, query, value to SQL table
def df_upsert_to_SQL(engine, df):

    records = (
        df.assign(source="serpapi")
          .to_dict(orient="records")
    )

    stmt = insert(GoogleTrendsTimeSeries).values(records)

    stmt = stmt.on_duplicate_key_update(
        value=stmt.inserted.value,
        source=stmt.inserted.source,
    )

    with engine.begin() as conn:
        conn.execute(stmt)

#upsert df to sql
df_upsert_to_SQL(engine, serpapi_to_df(layoffs_results))
df_upsert_to_SQL(engine, serpapi_to_df(job_cuts_results))
df_upsert_to_SQL(engine, serpapi_to_df(umplymt_benefits_results))
df_upsert_to_SQL(engine, serpapi_to_df(second_job_results))
df_upsert_to_SQL(engine, serpapi_to_df(CCMP_results))
df_upsert_to_SQL(engine, serpapi_to_df(side_hustle_results))