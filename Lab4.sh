#!/bin/bash

echo "Starting Lab"
bq mk ecommerce
bq query --nouse_legacy_sql '
CREATE OR REPLACE MODEL `ecommerce.classification_model`
OPTIONS
(
  model_type="logistic_reg",
  labels = ["will_buy_on_return_visit"]
)
AS
#standardSQL
SELECT
  * EXCEPT(fullVisitorId)
FROM
  -- features
  (
    SELECT
      fullVisitorId,
      IFNULL(totals.bounces, 0) AS bounces,
      IFNULL(totals.timeOnSite, 0) AS time_on_site
    FROM
      `data-to-insights.ecommerce.web_analytics`
    WHERE
      totals.newVisits = 1
      AND date BETWEEN "20160801" AND "20170430"  -- train on first 9 months
  )
  JOIN
  (
    SELECT
      fullVisitorId,
      IF(
        COUNTIF(totals.transactions > 0 AND totals.newVisits IS NULL) > 0,
        1,
        0
      ) AS will_buy_on_return_visit
    FROM
      `data-to-insights.ecommerce.web_analytics`
    GROUP BY
      fullVisitorId
  )
  USING (fullVisitorId);'
  
bq query --nouse_legacy_sql '
SELECT
  roc_auc,
  CASE
    WHEN roc_auc > .9 THEN "good"
    WHEN roc_auc > .8 THEN "fair"
    WHEN roc_auc > .7 THEN "decent"
    WHEN roc_auc > .6 THEN "not great"
    ELSE "poor"
  END AS model_quality
FROM
  ML.EVALUATE(
    MODEL ecommerce.classification_model,
    (
      SELECT
        * EXCEPT(fullVisitorId)
      FROM
        -- features
        (
          SELECT
            fullVisitorId,
            IFNULL(totals.bounces, 0) AS bounces,
            IFNULL(totals.timeOnSite, 0) AS time_on_site
          FROM
            `data-to-insights.ecommerce.web_analytics`
          WHERE
            totals.newVisits = 1
            AND date BETWEEN "20170501" AND "20170630"  -- eval on 2 months
        )
        JOIN
        (
          SELECT
            fullVisitorId,
            IF(
              COUNTIF(totals.transactions > 0 AND totals.newVisits IS NULL) > 0,
              1,
              0
            ) AS will_buy_on_return_visit
          FROM
            `data-to-insights.ecommerce.web_analytics`
          GROUP BY
            fullVisitorId
        )
        USING (fullVisitorId)
    )
  );'
  
bq query --nouse_legacy_sql '
CREATE OR REPLACE MODEL `ecommerce.classification_model_2`
OPTIONS
  (model_type="logistic_reg", labels = ["will_buy_on_return_visit"]) AS
WITH all_visitor_stats AS (
  SELECT
    fullVisitorId,
    IF(
      COUNTIF(totals.transactions > 0 AND totals.newVisits IS NULL) > 0,
      1,
      0
    ) AS will_buy_on_return_visit
  FROM
    `data-to-insights.ecommerce.web_analytics`
  GROUP BY
    fullVisitorId
)
-- add in new features
SELECT
  * EXCEPT(unique_session_id)
FROM (
  SELECT
    CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS unique_session_id,
    -- labels
    will_buy_on_return_visit,
    MAX(CAST(h.eCommerceAction.action_type AS INT64)) AS latest_ecommerce_progress,
    -- behavior on the site
    IFNULL(totals.bounces, 0) AS bounces,
    IFNULL(totals.timeOnSite, 0) AS time_on_site,
    IFNULL(totals.pageviews, 0) AS pageviews,
    -- where the visitor came from
    trafficSource.source,
    trafficSource.medium,
    channelGrouping,
    -- mobile or desktop
    device.deviceCategory,
    -- geographic
    IFNULL(geoNetwork.country, "") AS country
  FROM
    `data-to-insights.ecommerce.web_analytics`,
    UNNEST(hits) AS h
  JOIN
    all_visitor_stats
  USING (fullVisitorId)
  WHERE
    -- only predict for new visits
    totals.newVisits = 1
    AND date BETWEEN "20160801" AND "20170430"  -- train 9 months
  GROUP BY
    unique_session_id,
    will_buy_on_return_visit,
    bounces,
    time_on_site,
    totals.pageviews,
    trafficSource.source,
    trafficSource.medium,
    channelGrouping,
    device.deviceCategory,
    country
);'

bq query --nouse_legacy_sql '
SELECT
  roc_auc,
  CASE
    WHEN roc_auc > .9 THEN "good"
    WHEN roc_auc > .8 THEN "fair"
    WHEN roc_auc > .7 THEN "decent"
    WHEN roc_auc > .6 THEN "not great"
    ELSE "poor"
  END AS model_quality
FROM
  ML.EVALUATE(
    MODEL ecommerce.classification_model_2,
    (
      WITH all_visitor_stats AS (
        SELECT
          fullVisitorId,
          IF(
            COUNTIF(totals.transactions > 0 AND totals.newVisits IS NULL) > 0,
            1,
            0
          ) AS will_buy_on_return_visit
        FROM
          `data-to-insights.ecommerce.web_analytics`
        GROUP BY
          fullVisitorId
      )
      -- add in new features
      SELECT
        * EXCEPT(unique_session_id)
      FROM (
        SELECT
          CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS unique_session_id,
          -- labels
          will_buy_on_return_visit,
          MAX(CAST(h.eCommerceAction.action_type AS INT64)) AS latest_ecommerce_progress,
          -- behavior on the site
          IFNULL(totals.bounces, 0) AS bounces,
          IFNULL(totals.timeOnSite, 0) AS time_on_site,
          totals.pageviews,
          -- where the visitor came from
          trafficSource.source,
          trafficSource.medium,
          channelGrouping,
          -- mobile or desktop
          device.deviceCategory,
          -- geographic
          IFNULL(geoNetwork.country, "") AS country
        FROM
          `data-to-insights.ecommerce.web_analytics`,
          UNNEST(hits) AS h
        JOIN
          all_visitor_stats
        USING (fullVisitorId)
        WHERE
          -- only predict for new visits
          totals.newVisits = 1
          AND date BETWEEN "20170501" AND "20170630"  -- eval 2 months
        GROUP BY
          unique_session_id,
          will_buy_on_return_visit,
          bounces,
          time_on_site,
          totals.pageviews,
          trafficSource.source,
          trafficSource.medium,
          channelGrouping,
          device.deviceCategory,
          country
      )
    )
  );'
  
bq query --nouse_legacy_sql '
SELECT
  *
FROM
  ML.PREDICT(
    MODEL `ecommerce.classification_model_2`,
    (
      WITH all_visitor_stats AS (
        SELECT
          fullVisitorId,
          IF(
            COUNTIF(totals.transactions > 0 AND totals.newVisits IS NULL) > 0,
            1,
            0
          ) AS will_buy_on_return_visit
        FROM
          `data-to-insights.ecommerce.web_analytics`
        GROUP BY
          fullVisitorId
      )
      SELECT
        CONCAT(fullVisitorId, "-", CAST(visitId AS STRING)) AS unique_session_id,
        -- labels
        will_buy_on_return_visit,
        MAX(CAST(h.eCommerceAction.action_type AS INT64)) AS latest_ecommerce_progress,
        -- behavior on the site
        IFNULL(totals.bounces, 0) AS bounces,
        IFNULL(totals.timeOnSite, 0) AS time_on_site,
        totals.pageviews,
        -- where the visitor came from
        trafficSource.source,
        trafficSource.medium,
        channelGrouping,
        -- mobile or desktop
        device.deviceCategory,
        -- geographic
        IFNULL(geoNetwork.country, "") AS country
      FROM
        `data-to-insights.ecommerce.web_analytics`,
        UNNEST(hits) AS h
      JOIN
        all_visitor_stats
      USING (fullVisitorId)
      WHERE
        -- only predict for new visits
        totals.newVisits = 1
        AND date BETWEEN "20170701" AND "20170801"  -- test 1 month
      GROUP BY
        unique_session_id,
        will_buy_on_return_visit,
        bounces,
        time_on_site,
        totals.pageviews,
        trafficSource.source,
        trafficSource.medium,
        channelGrouping,
        device.deviceCategory,
        country
    )
  )
ORDER BY
  predicted_will_buy_on_return_visit DESC;'
