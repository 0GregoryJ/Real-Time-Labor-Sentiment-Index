# Real-Time-Labor-Sentiment-Index
An ETL script extracts data from multiple APIs and uses SQLAlchemy to update a SQL database, and the dashboard, built in Streamlit, pulls data from the database, visualizes it, and analyzes it with an LLM interpretation. Processes are automated with Apache Airflow.

The dashboard details Google search sentiment about the current labor market and compares it with lagging statistics from the BLS.
