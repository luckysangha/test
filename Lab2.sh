#!/bin/bash

# Step 1: Create a dataset if it doesn't exist
echo "Creating dataset bqml_lab..."
bq mk bqml_lab

# Step 2: Create the logistic regression model with optimizations
echo "Creating optimized logistic regression model..."
bq query --use_legacy_sql=false \
'CREATE OR REPLACE MODEL `bqml_lab.sample_model`
OPTIONS(model_type="logistic_reg") AS
WITH filtered_data AS (
  SELECT
    IF(totals.transactions IS NULL, 0, 1) AS label,
    IFNULL(device.operatingSystem, "") AS os,
    device.isMobile AS is_mobile,
    IFNULL(geoNetwork.country, "") AS country,
    IFNULL(totals.pageviews, 0) AS pageviews
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX BETWEEN "20160801" AND "20161231"  -- Smaller time range for faster processing
)
SELECT * FROM filtered_data
LIMIT 50000;  -- Reduced the row limit to 50,000 for faster model creation'

# Step 3: Evaluate the model
echo "Evaluating the optimized model..."
bq query --use_legacy_sql=false \
'SELECT
  *
FROM
  ML.EVALUATE(MODEL `bqml_lab.sample_model`, (
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  IFNULL(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  IFNULL(geoNetwork.country, "") AS country,
  IFNULL(totals.pageviews, 0) AS pageviews
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN "20170701" AND "20170801"));'

# Step 4: Predict purchases by country
echo "Predicting purchases by country..."
bq query --use_legacy_sql=false \
'SELECT
  country,
  SUM(predicted_label) AS total_predicted_purchases
FROM
  ML.PREDICT(MODEL `bqml_lab.sample_model`, (
SELECT
  IFNULL(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  IFNULL(totals.pageviews, 0) AS pageviews,
  IFNULL(geoNetwork.country, "") AS country
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN "20170701" AND "20170801"))
GROUP BY country
ORDER BY total_predicted_purchases DESC
LIMIT 10;'

# Step 5: Predict purchases by visitor
echo "Predicting purchases by visitor..."
bq query --use_legacy_sql=false \
'SELECT
  fullVisitorId,
  SUM(predicted_label) AS total_predicted_purchases
FROM
  ML.PREDICT(MODEL `bqml_lab.sample_model`, (
SELECT
  IFNULL(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  IFNULL(totals.pageviews, 0) AS pageviews,
  IFNULL(geoNetwork.country, "") AS country,
  fullVisitorId
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN "20170701" AND "20170801"))
GROUP BY fullVisitorId
ORDER BY total_predicted_purchases DESC
LIMIT 10;'

# Step 6: Completion message
echo "Lab completed successfully!"
