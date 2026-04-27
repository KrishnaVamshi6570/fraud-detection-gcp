-- Top features - card1_historical_fraud_rate #1, custom features in top 12
SELECT feature, ROUND(importance_weight, 4) AS importance
FROM ML.FEATURE_IMPORTANCE(MODEL `fraud_detection.model_xgboost`)
ORDER BY importance_weight DESC
LIMIT 15;
