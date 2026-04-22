CREATE OR REPLACE TABLE `fraud_detection.email_domain_risk` AS
SELECT
  P_emaildomain,
  COUNT(*) AS domain_tx_count,
  SUM(isFraud) AS domain_fraud_count,
  AVG(CASE WHEN isFraud = 1 THEN 1.0 ELSE 0.0 END) AS domain_fraud_rate
FROM `fraud_detection.raw_transactions`
WHERE P_emaildomain IS NOT NULL
GROUP BY P_emaildomain;
