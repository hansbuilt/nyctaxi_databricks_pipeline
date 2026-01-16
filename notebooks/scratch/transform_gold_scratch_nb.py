# Databricks notebook source
# MAGIC %md
# MAGIC Given that you already have **bronze and silver layers** for **NYC green and yellow taxi data**, your **gold layer and visualizations** should demonstrate three things clearly:
# MAGIC
# MAGIC 1. **Analytical judgment** (you know what questions matter)
# MAGIC 2. **Data modeling skills** (you can design consumable fact/dimension tables)
# MAGIC 3. **PySpark competence** (aggregations, window functions, joins, performance awareness)
# MAGIC
# MAGIC Below is a structured set of **worthwhile investigations**, mapped to **gold tables** and **visual outputs**, optimized for a portfolio-style data engineering project.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Trip Economics & Revenue Efficiency (Core, High Signal)
# MAGIC
# MAGIC ### Gold Tables
# MAGIC
# MAGIC * **fact_trip_revenue_daily**
# MAGIC
# MAGIC   * date
# MAGIC   * taxi_type (yellow / green)
# MAGIC   * borough / zone
# MAGIC   * trip_count
# MAGIC   * total_fare_amount
# MAGIC   * total_tip_amount
# MAGIC   * avg_fare
# MAGIC   * avg_tip
# MAGIC   * tip_pct
# MAGIC
# MAGIC ### Skills Demonstrated
# MAGIC
# MAGIC * Aggregations at multiple grains
# MAGIC * Dimensional joins (taxi zones)
# MAGIC * Numeric stability (tip % handling)
# MAGIC * Partitioning by date
# MAGIC
# MAGIC ### Analyses / Visuals
# MAGIC
# MAGIC * Revenue by borough over time
# MAGIC * Tip percentage by taxi type and location
# MAGIC * Green vs yellow taxi revenue contribution
# MAGIC * Weekday vs weekend revenue patterns
# MAGIC
# MAGIC Why this matters:
# MAGIC This mirrors **real-world KPI tables** used by transportation or marketplace teams.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Demand Patterns & Temporal Behavior (Very Strong Signal)
# MAGIC
# MAGIC ### Gold Tables
# MAGIC
# MAGIC * **fact_trip_demand_hourly**
# MAGIC
# MAGIC   * date
# MAGIC   * hour_of_day
# MAGIC   * day_of_week
# MAGIC   * zone
# MAGIC   * trip_count
# MAGIC   * avg_trip_distance
# MAGIC   * avg_trip_duration
# MAGIC
# MAGIC ### Skills Demonstrated
# MAGIC
# MAGIC * Time-based feature engineering
# MAGIC * Window functions for rolling averages
# MAGIC * Calendar normalization
# MAGIC
# MAGIC ### Analyses / Visuals
# MAGIC
# MAGIC * Heatmap: trips by hour and weekday
# MAGIC * Peak demand hours by borough
# MAGIC * Seasonal comparisons (month-over-month)
# MAGIC * Green vs yellow demand overlap
# MAGIC
# MAGIC This shows you understand **time-series aggregation at scale**, not just static reporting.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Trip Efficiency & Anomaly Detection (Excellent Differentiator)
# MAGIC
# MAGIC ### Gold Tables
# MAGIC
# MAGIC * **fact_trip_efficiency**
# MAGIC
# MAGIC   * zone_pair (pickup → dropoff)
# MAGIC   * taxi_type
# MAGIC   * avg_duration
# MAGIC   * avg_distance
# MAGIC   * avg_speed_mph
# MAGIC   * trip_count
# MAGIC   * p95_duration
# MAGIC
# MAGIC ### Skills Demonstrated
# MAGIC
# MAGIC * Derived metrics (speed)
# MAGIC * Percentile aggregations
# MAGIC * Filtering low-signal noise
# MAGIC * Grouping on composite keys
# MAGIC
# MAGIC ### Analyses / Visuals
# MAGIC
# MAGIC * Slowest routes by time of day
# MAGIC * Distance vs duration scatter plots
# MAGIC * Outlier-heavy zones
# MAGIC * Congestion patterns
# MAGIC
# MAGIC This goes beyond dashboards and into **operational insight**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Green vs Yellow Taxi Market Segmentation (Strategic Insight)
# MAGIC
# MAGIC ### Gold Tables
# MAGIC
# MAGIC * **dim_zone_taxi_coverage**
# MAGIC
# MAGIC   * zone
# MAGIC   * borough
# MAGIC   * yellow_trip_share
# MAGIC   * green_trip_share
# MAGIC   * avg_fare_diff
# MAGIC   * avg_duration_diff
# MAGIC
# MAGIC ### Skills Demonstrated
# MAGIC
# MAGIC * Cross-dataset comparison
# MAGIC * Share-of-market calculations
# MAGIC * Null-safe joins
# MAGIC * Business logic
# MAGIC
# MAGIC ### Analyses / Visuals
# MAGIC
# MAGIC * Zones dominated by green taxis
# MAGIC * Fare and tip differences by zone
# MAGIC * Market coverage gaps
# MAGIC * Overlay map (if supported)
# MAGIC
# MAGIC This shows you can **frame analytical questions**, not just process data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. Driver Behavior Proxies (Advanced, Optional)
# MAGIC
# MAGIC You don’t have driver IDs, but you can still infer behavior.
# MAGIC
# MAGIC ### Gold Tables
# MAGIC
# MAGIC * **fact_trip_tip_behavior**
# MAGIC
# MAGIC   * zone
# MAGIC   * hour
# MAGIC   * trip_count
# MAGIC   * avg_tip_pct
# MAGIC   * p90_tip_pct
# MAGIC
# MAGIC ### Skills Demonstrated
# MAGIC
# MAGIC * Behavioral inference from incomplete data
# MAGIC * Quantiles
# MAGIC * Noise filtering
# MAGIC
# MAGIC ### Analyses / Visuals
# MAGIC
# MAGIC * High-tip zones by time
# MAGIC * Tip volatility patterns
# MAGIC * Green vs yellow tipping norms
# MAGIC
# MAGIC This demonstrates **analytical maturity**, not just engineering.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6. Data Engineering–Focused Enhancements (Very Important for Interviews)
# MAGIC
# MAGIC Beyond analytics, explicitly show **engineering decisions**:
# MAGIC
# MAGIC ### Gold Layer Practices
# MAGIC
# MAGIC * Clearly defined **grain** per table
# MAGIC * Partitioning strategy (date / taxi_type)
# MAGIC * Schema enforcement
# MAGIC * Incremental build logic
# MAGIC * Reusable transformations
# MAGIC
# MAGIC ### Supporting Artifacts
# MAGIC
# MAGIC * README explaining:
# MAGIC
# MAGIC   * Bronze → Silver → Gold contracts
# MAGIC   * Why certain aggregations exist
# MAGIC   * Expected consumers (BI, ML, ad hoc)
# MAGIC
# MAGIC This is often more impressive than the visuals themselves.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Recommended Visualization Stack (Given Your Constraints)
# MAGIC
# MAGIC Since you mentioned **free Power BI + Azure** earlier:
# MAGIC
# MAGIC * Power BI for:
# MAGIC
# MAGIC   * Time-series trends
# MAGIC   * Heatmaps
# MAGIC   * KPI cards
# MAGIC * Optional:
# MAGIC
# MAGIC   * Static PNG charts committed to GitHub
# MAGIC   * Simple maps using taxi zones shapefile
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## If You Want a “Narrative” Project Angle
# MAGIC
# MAGIC Frame the project as:
# MAGIC
# MAGIC > “How NYC taxi demand, efficiency, and revenue differ by geography and time — and how green and yellow taxis serve distinct market roles.”
# MAGIC
# MAGIC That narrative ties all gold tables together and feels intentional.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC If you want, I can:
# MAGIC
# MAGIC * Propose **exact PySpark transformations** for one gold table
# MAGIC * Suggest a **folder structure** for gold + viz assets
# MAGIC * Help you decide which **3–4 visuals** give maximum interview impact
# MAGIC
# MAGIC Tell me which direction you want to go next.
# MAGIC