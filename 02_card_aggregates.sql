CREATE OR REPLACE TABLE `fraud_detection.card_aggregates` AS
SELECT
  card1,
  COUNT(*) AS card1_tx_count,
  AVG(TransactionAmt) AS card1_avg_amt,
  STDDEV(TransactionAmt) AS card1_std_amt,
  MAX(TransactionAmt) AS card1_max_amt,
  MIN(TransactionAmt) AS card1_min_amt,
  AVG(CASE WHEN isFraud = 1 THEN 1.0 ELSE 0.0 END) AS card1_historical_fraud_rate
FROM `fraud_detection.raw_transactions`
WHERE card1 IS NOT NULL
GROUP BY card1;
