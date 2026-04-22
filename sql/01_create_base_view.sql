-- Creates a joined view of transactions + identity
CREATE OR REPLACE VIEW `fraud_detection.v_transactions_with_identity` AS
-- (CREATE OR REPLACE VIEW `fraud_detection.v_transactions_with_identity` AS
SELECT
  t.TransactionID,
  t.isFraud,
  t.TransactionDT,
  TIMESTAMP_ADD(TIMESTAMP '2017-11-30 00:00:00', INTERVAL t.TransactionDT SECOND) AS transaction_timestamp,
  t.TransactionAmt,
  t.ProductCD,
  t.card1,
  t.card2,
  t.card3,
  t.card4,
  t.card5,
  t.card6,
  t.addr1,
  t.addr2,
  t.dist1,
  t.dist2,
  t.P_emaildomain,
  t.R_emaildomain,
  t.C1, t.C2, t.C3, t.C4, t.C5,
  t.C6, t.C7, t.C8, t.C9, t.C10,
  t.C11, t.C12, t.C13, t.C14,
  t.D1, t.D2, t.D3, t.D4, t.D5,
  t.D10, t.D11, t.D15,
  t.M1, t.M2, t.M3, t.M4, t.M5,
  t.M6, t.M7, t.M8, t.M9,
  t.V1, t.V2, t.V3, t.V4, t.V5,
  t.V12, t.V13, t.V14, t.V15,
  t.V70, t.V76, t.V78, t.V82, t.V83,
  -- Identity columns (NULL if no identity record)
  i.id_01, i.id_02, i.id_03, i.id_05, i.id_06,
  i.id_09, i.id_10, i.id_11,
  i.id_12, i.id_15, i.id_16,
  i.id_23, i.id_27, i.id_28, i.id_29,
  i.id_31, i.id_35, i.id_36, i.id_37, i.id_38,
  i.DeviceType,
  i.DeviceInfo
FROM `fraud_detection.raw_transactions` t
LEFT JOIN `fraud_detection.raw_identity` i
  ON t.TransactionID = i.TransactionID;)
