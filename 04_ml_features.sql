CREATE OR REPLACE TABLE `fraud_detection.ml_features` AS
WITH base AS (
  SELECT
    v.*,
    -- Time features
    EXTRACT(HOUR FROM transaction_timestamp) AS tx_hour,
    EXTRACT(DAYOFWEEK FROM transaction_timestamp) AS tx_day_of_week,
    EXTRACT(MONTH FROM transaction_timestamp) AS tx_month,
    CASE
      WHEN EXTRACT(HOUR FROM transaction_timestamp) BETWEEN 0 AND 5 THEN 1
      ELSE 0
    END AS is_late_night,

    -- Amount features
    LOG(TransactionAmt + 1) AS log_tx_amt,
    CASE
      WHEN TransactionAmt < 50 THEN 'low'
      WHEN TransactionAmt < 200 THEN 'medium'
      WHEN TransactionAmt < 1000 THEN 'high'
      ELSE 'very_high'
    END AS amt_bucket,
    ROUND(TransactionAmt) - TransactionAmt AS amt_decimal_part,

    -- Null flags (missingness is itself a signal)
    CASE WHEN dist1 IS NULL THEN 1 ELSE 0 END AS dist1_is_null,
    CASE WHEN dist2 IS NULL THEN 1 ELSE 0 END AS dist2_is_null,
    CASE WHEN P_emaildomain IS NULL THEN 1 ELSE 0 END AS email_is_null,
    CASE WHEN addr1 IS NULL THEN 1 ELSE 0 END AS addr_is_null,
    CASE WHEN DeviceType IS NULL THEN 1 ELSE 0 END AS device_is_null,

    -- Device type encoding
    CASE WHEN DeviceType = 'mobile' THEN 1 ELSE 0 END AS is_mobile,
    CASE WHEN DeviceType = 'desktop' THEN 1 ELSE 0 END AS is_desktop

  FROM `fraud_detection.v_transactions_with_identity` v
),
enriched AS (
  SELECT
    b.*,

    -- Card aggregate features
    ca.card1_tx_count,
    ca.card1_avg_amt,
    ca.card1_std_amt,
    ca.card1_max_amt,
    ca.card1_historical_fraud_rate,

    -- Amount deviation from card's history (how unusual is this transaction?)
    CASE
      WHEN ca.card1_std_amt > 0
      THEN (b.TransactionAmt - ca.card1_avg_amt) / ca.card1_std_amt
      ELSE 0
    END AS amt_zscore_vs_card,

    -- Email domain risk
    COALESCE(ed.domain_fraud_rate, 0.035) AS email_domain_fraud_rate,
    COALESCE(ed.domain_tx_count, 0) AS email_domain_tx_count

  FROM base b
  LEFT JOIN `fraud_detection.card_aggregates` ca ON b.card1 = ca.card1
  LEFT JOIN `fraud_detection.email_domain_risk` ed ON b.P_emaildomain = ed.P_emaildomain
)
SELECT * FROM enriched;
