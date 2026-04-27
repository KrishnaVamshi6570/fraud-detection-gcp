-- 80/20 train test split
CREATE OR REPLACE TABLE `fraud_detection.train_data` AS
SELECT * FROM `fraud_detection.ml_features`
WHERE MOD(TransactionID, 10) < 8;

CREATE OR REPLACE TABLE `fraud_detection.test_data` AS
SELECT * FROM `fraud_detection.ml_features`
WHERE MOD(TransactionID, 10) >= 8;
