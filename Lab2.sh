#!/bin/bash

# Step 1: Create a dataset if it doesn't exist
echo "Creating dataset bqml_lab..."
bq mk bqml_lab

# Step 2: Create the logistic regression model
echo "Creating logistic regression model..."
bq query --use_legacy_sql=false \
'CREATE OR REPLACE MODEL `bqml_lab.sample_model`
OPTIONS(
  model_type="logistic_reg",
  max_iterations=5,
  learn_rate=0.1,
  l1_reg=0.1,
  l2_reg=0.1,
  enable_global_explain=false
) AS
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  COALESCE(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  COALESCE(geoNetwork.country, "") AS country,
  COALESCE(totals.pageviews, 0) AS pageviews
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN "20170101" AND "20170131"
LIMIT 50000;'



# Step 3: Evaluate the model
echo "Evaluating the model..."
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
