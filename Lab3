#!/bin/bash

# Variables
PROJECT_ID=$(gcloud config get-value project)
DATASET_ID="ecommerce"

# Step 1: Create BigQuery dataset
echo "Creating BigQuery dataset '$DATASET_ID'..."
bq --location=US mk -d --description "Dataset for ecommerce ML models" $PROJECT_ID:$DATASET_ID || echo "Dataset already exists."

# Step 2: Create the first classification model
echo "Creating the first classification model..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID "
CREATE OR REPLACE MODEL \`$DATASET_ID.classification_model\`
OPTIONS (
  model_type='logistic_reg',
  input_label_cols=['will_buy_on_return_visit']
) AS
SELECT
  * EXCEPT(fullVisitorId)
FROM
  (
    SELECT
      fullVisitorId,
      IFNULL(totals.bounces, 0) AS bounces,
      IFNULL(totals.timeOnSite, 0) AS time_on_site
    FROM
      \`data-to-insights.ecommerce.web_analytics\`
    WHERE
      totals.newVisits = 1
      AND date BETWEEN '20160801' AND '20170430'
  ) AS features
JOIN
  (
    SELECT
      fullVisitorId,
      IF(COUNTIF(totals.transactions > 0 AND totals.newVisits IS NULL) > 0, 1, 0) AS will_buy_on_return_visit
    FROM
      \`data-to-insights.ecommerce.web_analytics\`
    GROUP BY fullVisitorId
  ) AS labels
USING (fullVisitorId);
"

# Wait for the model to train (could take several minutes)
echo "Training the first model. This may take a few minutes..."
sleep 180  # Adjust the sleep time as needed

# Step 3: Evaluate the first model
echo "Evaluating the first classification model..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID "
SELECT
  roc_auc,
  CASE
    WHEN roc_auc > 0.9 THEN 'good'
    WHEN roc_auc > 0.8 THEN 'fair'
    WHEN roc_auc > 0.7 THEN 'decent'
    WHEN roc_auc > 0.6 THEN 'not great'
    ELSE 'poor'
  END AS model_quality
FROM
  ML.EVALUATE(MODEL \`$DATASET_ID.classification_model\`, (
    SELECT
      * EXCEPT(fullVisitorId)
    FROM
      (
        SELECT
          fullVisitorId,
          IFNULL(totals.bounces, 0) AS bounces,
          IFNULL(totals.timeOnSite, 0) AS time_on_site
        FROM
          \`data-to-insights.ecommerce.web_analytics\`
        WHERE
          totals.newVisits = 1
          AND date BETWEEN '20170501' AND '20170630'
      ) AS features
    JOIN
      (
        SELECT
          fullVisitorId,
          IF(COUNTIF(totals.transactions > 0 AND totals.newVisits IS NULL) > 0, 1, 0) AS will_buy_on_return_visit
        FROM
          \`data-to-insights.ecommerce.web_analytics\`
        GROUP BY fullVisitorId
      ) AS labels
    USING (fullVisitorId)
  ));
"

# Step 4: Create the second classification model with additional features
echo "Creating the second classification model with additional features..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID "
CREATE OR REPLACE MODEL \`$DATASET_ID.classification_model_2\`
OPTIONS (
  model_type='logistic_reg',
  input_label_cols=['will_buy_on_return_visit']
) AS
WITH all_visitor_stats AS (
  SELECT
    fullVisitorId,
    IF(COUNTIF(totals.transactions > 0 AND totals.newVisits IS NULL) > 0, 1, 0) AS will_buy_on_return_visit
  FROM
    \`data-to-insights.ecommerce.web_analytics\`
  GROUP BY fullVisitorId
)
SELECT
  * EXCEPT(unique_session_id)
FROM (
  SELECT
    CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS unique_session_id,
    -- Label
    will_buy_on_return_visit,
    MAX(CAST(h.eCommerceAction.action_type AS INT64)) AS latest_ecommerce_progress,
    -- Behavior on the site
    IFNULL(totals.bounces, 0) AS bounces,
    IFNULL(totals.timeOnSite, 0) AS time_on_site,
    IFNULL(totals.pageviews, 0) AS pageviews,
    -- Where the visitor came from
    trafficSource.source,
    trafficSource.medium,
    channelGrouping,
    -- Mobile or desktop
    device.deviceCategory,
    -- Geographic
    IFNULL(geoNetwork.country, '') AS country
  FROM
    \`data-to-insights.ecommerce.web_analytics\`,
    UNNEST(hits) AS h
  JOIN
    all_visitor_stats
  USING (fullVisitorId)
  WHERE
    totals.newVisits = 1
    AND date BETWEEN '20160801' AND '20170430'
  GROUP BY
    unique_session_id,
    will_buy_on_return_visit,
    bounces,
    time_on_site,
    pageviews,
    trafficSource.source,
    trafficSource.medium,
    channelGrouping,
    device.deviceCategory,
    country
);
"

# Wait for the model to train
echo "Training the second model. This may take a few minutes..."
sleep 180  # Adjust the sleep time as needed

# Step 5: Evaluate the second model
echo "Evaluating the second classification model..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID "
SELECT
  roc_auc,
  CASE
    WHEN roc_auc > 0.9 THEN 'good'
    WHEN roc_auc > 0.8 THEN 'fair'
    WHEN roc_auc > 0.7 THEN 'decent'
    WHEN roc_auc > 0.6 THEN 'not great'
    ELSE 'poor'
  END AS model_quality
FROM
  ML.EVALUATE(MODEL \`$DATASET_ID.classification_model_2\`, (
    WITH all_visitor_stats AS (
      SELECT
        fullVisitorId,
        IF(COUNTIF(totals.transactions > 0 AND totals.newVisits IS NULL) > 0, 1, 0) AS will_buy_on_return_visit
      FROM
        \`data-to-insights.ecommerce.web_analytics\`
      GROUP BY fullVisitorId
    )
    SELECT
      * EXCEPT(unique_session_id)
    FROM (
      SELECT
        CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS unique_session_id,
        -- Label
        will_buy_on_return_visit,
        MAX(CAST(h.eCommerceAction.action_type AS INT64)) AS latest_ecommerce_progress,
        -- Behavior on the site
        IFNULL(totals.bounces, 0) AS bounces,
        IFNULL(totals.timeOnSite, 0) AS time_on_site,
        IFNULL(totals.pageviews, 0) AS pageviews,
        -- Where the visitor came from
        trafficSource.source,
        trafficSource.medium,
        channelGrouping,
        -- Mobile or desktop
        device.deviceCategory,
        -- Geographic
        IFNULL(geoNetwork.country, '') AS country
      FROM
        \`data-to-insights.ecommerce.web_analytics\`,
        UNNEST(hits) AS h
      JOIN
        all_visitor_stats
      USING (fullVisitorId)
      WHERE
        totals.newVisits = 1
        AND date BETWEEN '20170501' AND '20170630'
      GROUP BY
        unique_session_id,
        will_buy_on_return_visit,
        bounces,
        time_on_site,
        pageviews,
        trafficSource.source,
        trafficSource.medium,
        channelGrouping,
        device.deviceCategory,
        country
    )
  ));
"

# Step 6: Predict which new visitors will come back and purchase
echo "Predicting which new visitors will come back and purchase..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID "
SELECT
  *
FROM
  ML.PREDICT(MODEL \`$DATASET_ID.classification_model_2\`, (
    WITH all_visitor_stats AS (
      SELECT
        fullVisitorId,
        IF(COUNTIF(totals.transactions > 0 AND totals.newVisits IS NULL) > 0, 1, 0) AS will_buy_on_return_visit
      FROM
        \`data-to-insights.ecommerce.web_analytics\`
      GROUP BY fullVisitorId
    )
    SELECT
      CONCAT(fullVisitorId, '-', CAST(visitId AS STRING)) AS unique_session_id,
      -- Label
      will_buy_on_return_visit,
      MAX(CAST(h.eCommerceAction.action_type AS INT64)) AS latest_ecommerce_progress,
      -- Behavior on the site
      IFNULL(totals.bounces, 0) AS bounces,
      IFNULL(totals.timeOnSite, 0) AS time_on_site,
      IFNULL(totals.pageviews, 0) AS pageviews,
      -- Where the visitor came from
      trafficSource.source,
      trafficSource.medium,
      channelGrouping,
      -- Mobile or desktop
      device.deviceCategory,
      -- Geographic
      IFNULL(geoNetwork.country, '') AS country
    FROM
      \`data-to-insights.ecommerce.web_analytics\`,
      UNNEST(hits) AS h
    JOIN
      all_visitor_stats
    USING (fullVisitorId)
    WHERE
      totals.newVisits = 1
      AND date BETWEEN '20170701' AND '20170801'
    GROUP BY
      unique_session_id,
      will_buy_on_return_visit,
      bounces,
      time_on_site,
      pageviews,
      trafficSource.source,
      trafficSource.medium,
      channelGrouping,
      device.deviceCategory,
      country
  ))
ORDER BY
  predicted_will_buy_on_return_visit DESC;
"

echo "Lab automation completed!"
