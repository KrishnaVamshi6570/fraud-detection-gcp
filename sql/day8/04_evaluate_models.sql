-- Model comparison - XGBoost wins (AUC 0.936 vs 0.854)
SELECT 'Logistic Regression' AS model, roc_auc, precision, recall, f1_score
FROM ML.EVALUATE(MODEL `fraud_detection.model_logistic_regression`,
  (SELECT * FROM `fraud_detection.test_data` WHERE isFraud IS NOT NULL))
UNION ALL
SELECT 'XGBoost', roc_auc, precision, recall, f1_score
FROM ML.EVALUATE(MODEL `fraud_detection.model_xgboost`,
  (SELECT * FROM `fraud_detection.test_data` WHERE isFraud IS NOT NULL));
