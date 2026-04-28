-- Batch predictions on all 590K transactions using XGBoost model
-- Results: HIGH bucket 53.44% fraud rate, SAFE bucket 0.27%
CREATE OR REPLACE TABLE `fraud_detection.fraud_scores` AS
SELECT
  t.TransactionID,
  t.isFraud AS actual_fraud,
  p.predicted_isFraud AS predicted_fraud,
  ROUND(p.predicted_isFraud_probs[OFFSET(0)].prob, 6) AS fraud_probability,
  t.TransactionAmt, t.ProductCD, t.card4, t.card6,
  t.P_emaildomain, t.DeviceType, t.tx_hour, t.is_late_night,
  t.amt_bucket, t.card1_historical_fraud_rate,
  t.email_domain_fraud_rate, t.amt_zscore_vs_card,
  CASE
    WHEN p.predicted_isFraud_probs[OFFSET(0)].prob >= 0.8 THEN 'HIGH'
    WHEN p.predicted_isFraud_probs[OFFSET(0)].prob >= 0.5 THEN 'MEDIUM'
    WHEN p.predicted_isFraud_probs[OFFSET(0)].prob >= 0.3 THEN 'LOW'
    ELSE 'SAFE'
  END AS risk_level
FROM ML.PREDICT(MODEL `fraud_detection.model_xgboost`,
  (SELECT * FROM `fraud_detection.ml_features` WHERE isFraud IS NOT NULL)) p
JOIN `fraud_detection.ml_features` t ON p.TransactionID = t.TransactionID;
