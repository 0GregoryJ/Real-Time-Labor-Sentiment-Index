# Economic Sentiment vs. Official Data Dashboard

## Overview

This dashboard visualizes the relationship between **public economic sentiment** (measured through Google search behavior) and **official macroeconomic data** (BLS, BEA, FRED).

Its core purpose is to highlight **gaps between how people feel about the economy and what official statistics report**.

Rather than focusing only on economic levels (e.g., unemployment rate), the dashboard measures:

* **Search-based stress signals**
* **Fundamental economic conditions**
* The **divergence between the two**

This allows users to identify periods where:

* Public anxiety exceeds economic fundamentals
* Official data weakens before public concern rises
* Sentiment and fundamentals move in opposite directions

---

## What the Dashboard Measures

### 1. Search-Based Stress Indices

Google search terms related to economic stress (e.g., layoffs, unemployment benefits, credit card minimum payments) are:

1. Standardized over time (z-score)
2. Averaged into thematic composites (e.g., Labor Stress)

These indices measure:

> Relative public anxiety compared to historical norms

---

### 2. Official Economic Condition Indices

BLS series are transformed into cyclical measures before aggregation:

| Series                  | Transformation        | Direction         |
| ----------------------- | --------------------- | ----------------- |
| Payroll Employment      | MoM % change          | Higher = stronger |
| Average Hourly Earnings | YoY % change          | Higher = stronger |
| Weekly Hours            | MoM change            | Higher = stronger |
| Unemployment Rate       | Inverted level        | Higher = stronger |
| Unemployment Level      | Inverted MoM % change | Higher = stronger |

After transformation:

1. Each series is standardized (z-score)
2. The standardized series are averaged
3. The result is inverted to create a **Labor Stress Index**

This produces a clean, comparable stress measure aligned with sentiment data.

---

### 3. Sentiment–Fundamentals Gap

The dashboard computes:

```
Stress Gap = Search Stress – Official Stress
```

Interpretation:

* **Positive Gap** → Public anxiety exceeds fundamentals
* **Negative Gap** → Fundamentals worse than public perception
* **Zero** → Alignment between sentiment and data

This gap is often more informative than either series alone.

---

## Technical Architecture

### Data Sources

* **FRED API** (BLS, BEA series)
* **Google search data**
* Monthly frequency

### Data Structure

Master dataset stored in long format:

```
date | query | value
```

Derived columns:

* Transformed cyclical values
* Z-scores (standardized over time)
* Composite indices
* Stress gap measures

### Transformation Pipeline

1. Convert values to numeric
2. Sort by series and date
3. Apply series-specific transformation
4. Standardize within series (z-score)
5. Aggregate across series by date
6. Align direction (stress-based interpretation)

### Visualization

* Built using Plotly
* Dynamic time-range filtering (2W, 1M, 1Y, 5Y)
* Legend and layout customized for macro-style presentation
* Rendered in Streamlit

---

## Why This Dashboard Is Valuable

Traditional dashboards show economic levels.

This dashboard shows:

* **Relative stress**
* **Perception vs reality**
* **Divergence signals**

These divergences can:

* Anticipate turning points
* Reveal fragility beneath strong headline data
* Highlight media amplification cycles
* Provide macroeconomic context beyond raw statistics

---

## Running the Project

1. Install dependencies:

   ```
   pip install streamlit pandas plotly requests
   ```

2. Set your API keys.

3. Run the app:

   ```
   streamlit run app.py
   ```

---

## Summary

This dashboard reframes economic monitoring from:

> “What is the unemployment rate?”

to:

> “How does economic reality compare to public perception?”

By standardizing and aligning both search behavior and official data, it creates a coherent framework for analyzing economic stress in real time.
