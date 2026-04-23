CREATE OR REPLACE TABLE main_features AS
WITH base AS (
    SELECT
        date,
        value,
        query,
        category,
        source,
        (
            (value - AVG(value) OVER (
                PARTITION BY source, query
            ))
            / NULLIF(
                STDDEV_SAMP(value) OVER (
                    PARTITION BY source, query
                ),
                0
            )
        ) AS zscore
    FROM fact_observations
),
scored AS (
    SELECT
        date,
        value,
        query,
        category,
        source,
        zscore,
        -- Normal CDF approximation using tanh, DuckDB-native, and smooth).
        100.0 * (0.5 * (1.0 + tanh(sqrt(2.0 / pi()) * (zscore + 0.044715 * pow(zscore, 3))))) AS cdf_scaled
    FROM base
),
aggregated AS (
    SELECT
        date,

        AVG(
            CASE
                WHEN category = 'labor_market' AND source = 'serp'
                THEN cdf_scaled
            END
        ) AS labor_search_sentiment,

        AVG(
            CASE
                WHEN category = 'consumer_spending' AND source = 'serp'
                THEN cdf_scaled
            END
        ) AS spending_search_sentiment,

        AVG(
            CASE
                WHEN category = 'labor_market' AND source IN ('bls')
                THEN cdf_scaled
            END
        ) AS labor_reported_sentiment,

        AVG(
            CASE
                WHEN category = 'consumer_spending' AND source IN ('fred')
                THEN cdf_scaled
            END
        ) AS spending_reported_sentiment,

        MAX(
            CASE
                WHEN query = 'LNS14000000' THEN value
            END
        ) AS LNS14000000,

        MAX(
            CASE
                WHEN query = 'CES0500000002' THEN value
            END
        ) AS CES0500000002,

        MAX(
            CASE
                WHEN query = 'CES0500000003' THEN value
            END
        ) AS CES0500000003,

        MAX(
            CASE
                WHEN query = 'unemployment benefits' THEN value
            END
        ) AS unemployment_benefits,

        MAX(
            CASE
                WHEN query = 'second job' THEN value
            END
        ) AS second_job,

        MAX(
            CASE
                WHEN query = 'layoffs' THEN value
            END
        ) AS layoffs,

        MAX(
            CASE
                WHEN query = 'credit card application' THEN value
            END
        ) AS credit_card_application,

        MAX(
            CASE
                WHEN query = 'kitchen remodel' THEN value
            END
        ) AS kitchen_remodel,

        MAX(
            CASE
                WHEN query = 'flight deals' THEN value
            END
        ) AS flight_deals,


    FROM scored
    GROUP BY date
)

SELECT
    date,
    labor_search_sentiment,
    spending_search_sentiment,
    labor_reported_sentiment,
    spending_reported_sentiment,
    labor_search_sentiment - labor_reported_sentiment AS labor_gap,
    spending_search_sentiment - spending_reported_sentiment AS spending_gap,
    LNS14000000,
    CES0500000002,
    CES0500000003,
    unemployment_benefits,
    second_job,
    layoffs,
    flight_deals,
    kitchen_remodel,
    credit_card_application,

FROM aggregated
ORDER BY date;