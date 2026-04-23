SELECT
    date,
    labor_search_sentiment,
    labor_reported_sentiment,
    labor_gap,
    LNS14000000,
    CES0500000002,
    CES0500000003,
    unemployment_benefits,
    second_job,
    layoffs
FROM main_features
WHERE date >= ?
ORDER BY date;