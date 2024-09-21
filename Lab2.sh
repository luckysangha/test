#!/bin/bash

# Step 1: Create a dataset if it doesn't exist
echo "Creating dataset bqml_lab..."
bq mk --dataset --description "BQML Lab Dataset" bqml_lab

# Step 2: Create the logistic regression model
echo "Creating logistic regression model..."
bq query --use_legacy_sql=false \
'CREATE OR REPLACE MODEL `bqml_lab.sample_model`
OPTIONS(
  model_type="logistic_reg",
  max_iterations=2,
  learn_rate_strategy="constant",
  learn_rate=0.2,
  l1_reg=0.0,
  l2_reg=0.0,
  enable_global_explain=false,
  early_stop=false,
  data_split_method="NO_SPLIT"
) AS
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  COALESCE(device.isMobile, FALSE) AS is_mobile,
  COALESCE(totals.pageviews, 0) AS pageviews
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_20170101`
LIMIT 10000;'

# Step 3: Evaluate the model
echo "Evaluating the model..."
bq query --use_legacy_sql=false \
'SELECT
  *
FROM
  ML.EVALUATE(MODEL `bqml_lab.sample_model`, (
    SELECT
      IF(totals.transactions IS NULL, 0, 1) AS label,
      COALESCE(device.isMobile, FALSE) AS is_mobile,
      COALESCE(totals.pageviews, 0) AS pageviews
    FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_20170701`
));'

# Step 4: Predict purchases by country (Adjusted to match new features)
echo "Predicting purchases by country..."
bq query --use_legacy_sql=false \
'SELECT
  country,
  SUM(predicted_label) AS total_predicted_purchases
FROM
  ML.PREDICT(MODEL `bqml_lab.sample_model`, (
    SELECT
      COALESCE(device.isMobile, FALSE) AS is_mobile,
      COALESCE(totals.pageviews, 0) AS pageviews,
      COALESCE(geoNetwork.country, "") AS country
    FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_20170701`
))
GROUP BY country
ORDER BY total_predicted_purchases DESC
LIMIT 10;'

# Step 5: Predict purchases by visitor (Adjusted to match new features)
echo "Predicting purchases by visitor..."
bq query --use_legacy_sql=false \
'SELECT
  fullVisitorId,
  SUM(predicted_label) AS total_predicted_purchases
FROM
  ML.PREDICT(MODEL `bqml_lab.sample_model`, (
    SELECT
      COALESCE(device.isMobile, FALSE) AS is_mobile,
      COALESCE(totals.pageviews, 0) AS pageviews,
      fullVisitorId
    FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_20170701`
))
GROUP BY fullVisitorId
ORDER BY total_predicted_purchases DESC
LIMIT 10;'

# Step 6: Completion message
echo "Lab completed successfully!"
