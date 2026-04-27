-- XGBoost model - WINNER - ROC-AUC: 0.936, Recall: 0.840
CREATE OR REPLACE MODEL `fraud_detection.model_xgboost`
OPTIONS(
  model_type         = 'BOOSTED_TREE_CLASSIFIER',
  input_label_cols   = ['isFraud'],
  num_parallel_tree  = 4,
  max_iterations     = 50,
  tree_method        = 'hist',
  auto_class_weights = TRUE,
  subsample          = 0.8,
  colsample_bytree   = 0.8
) AS
SELECT isFraud, TransactionAmt, log_tx_amt, amt_decimal_part,
  tx_hour, tx_day_of_week, is_late_night, card1, card4, card6,
  addr1, dist1_is_null, dist2_is_null, P_emaildomain, email_is_null,
  email_domain_fraud_rate, DeviceType, device_is_null, is_mobile,
  card1_tx_count, card1_avg_amt, card1_std_amt, card1_historical_fraud_rate,
  amt_zscore_vs_card, C1, C2, C5, C6, C13, C14, D1, D4, D10, D15, M4, M5, M6
FROM `fraud_detection.train_data` WHERE isFraud IS NOT NULL;
