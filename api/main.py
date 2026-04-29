import os
import math
import logging
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Fraud Detection API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

PROJECT_ID = os.environ.get("PROJECT_ID", "fraud-detection-gcp-kv")
MODEL      = f"{PROJECT_ID}.fraud_detection.model_xgboost"
bq_client  = bigquery.Client(project=PROJECT_ID)

class Transaction(BaseModel):
    TransactionAmt:              float
    ProductCD:                   Optional[str]   = "W"
    card1:                       Optional[int]   = 1000
    card4:                       Optional[str]   = None
    card6:                       Optional[str]   = None
    addr1:                       Optional[float] = None
    P_emaildomain:               Optional[str]   = None
    DeviceType:                  Optional[str]   = None
    C1:                          Optional[float] = 1.0
    C2:                          Optional[float] = 1.0
    C5:                          Optional[float] = 0.0
    C6:                          Optional[float] = 1.0
    C13:                         Optional[float] = 1.0
    C14:                         Optional[float] = 1.0
    D1:                          Optional[float] = None
    D4:                          Optional[float] = None
    D10:                         Optional[float] = None
    D15:                         Optional[float] = None
    M4:                          Optional[str]   = None
    M5:                          Optional[str]   = None
    M6:                          Optional[str]   = None
    card1_tx_count:              Optional[int]   = 1
    card1_avg_amt:               Optional[float] = None
    card1_std_amt:               Optional[float] = None
    card1_historical_fraud_rate: Optional[float] = 0.035
    email_domain_fraud_rate:     Optional[float] = 0.035

def sf(v):
    """Format string value for SQL - NULL or quoted string."""
    if v is None: return "NULL"
    return f"'{v}'"

def nf(v, default=0.0):
    """Format numeric value for SQL - NULL or number."""
    if v is None: return str(default)
    return str(v)

@app.get("/")
def root():
    return {"service": "Fraud Detection API", "status": "healthy", "model": "XGBoost BQML"}

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.post("/predict")
def predict(tx: Transaction):
    try:
        amount = tx.TransactionAmt
        avg    = tx.card1_avg_amt if tx.card1_avg_amt else amount
        std    = tx.card1_std_amt if tx.card1_std_amt else 1.0
        now    = datetime.now(timezone.utc)
        amt_bucket = (
            "low"      if amount < 50   else
            "medium"   if amount < 200  else
            "high"     if amount < 1000 else "very_high"
        )
        zscore = round((amount - avg) / std, 4)

        query = f"""
        SELECT
          predicted_isFraud,
          predicted_isFraud_probs[OFFSET(0)].prob AS fraud_probability
        FROM ML.PREDICT(MODEL `{MODEL}`, (SELECT
          CAST({amount} AS FLOAT64)                             AS TransactionAmt,
          CAST({round(math.log(amount+1),4)} AS FLOAT64)        AS log_tx_amt,
          CAST({round(amount - round(amount),4)} AS FLOAT64)    AS amt_decimal_part,
          CAST('{amt_bucket}' AS STRING)                        AS amt_bucket,
          CAST({now.hour} AS INT64)                             AS tx_hour,
          CAST({now.weekday()} AS INT64)                        AS tx_day_of_week,
          CAST({1 if now.hour < 6 else 0} AS INT64)            AS is_late_night,
          CAST({sf(tx.ProductCD)} AS STRING)                    AS ProductCD,
          CAST({tx.card1 or 1000} AS INT64)                    AS card1,
          CAST({sf(tx.card4)} AS STRING)                        AS card4,
          CAST({sf(tx.card6)} AS STRING)                        AS card6,
          CAST({nf(tx.addr1)} AS FLOAT64)                      AS addr1,
          CAST(1 AS INT64)                                      AS dist1_is_null,
          CAST(1 AS INT64)                                      AS dist2_is_null,
          CAST({sf(tx.P_emaildomain)} AS STRING)                AS P_emaildomain,
          CAST({0 if tx.P_emaildomain else 1} AS INT64)        AS email_is_null,
          CAST({tx.email_domain_fraud_rate} AS FLOAT64)        AS email_domain_fraud_rate,
          CAST({sf(tx.DeviceType)} AS STRING)                   AS DeviceType,
          CAST({0 if tx.DeviceType else 1} AS INT64)           AS device_is_null,
          CAST({1 if tx.DeviceType=="mobile" else 0} AS INT64) AS is_mobile,
          CAST({tx.card1_tx_count or 1} AS INT64)              AS card1_tx_count,
          CAST({avg} AS FLOAT64)                               AS card1_avg_amt,
          CAST({std} AS FLOAT64)                               AS card1_std_amt,
          CAST({tx.card1_historical_fraud_rate} AS FLOAT64)    AS card1_historical_fraud_rate,
          CAST({zscore} AS FLOAT64)                            AS amt_zscore_vs_card,
          CAST({tx.C1} AS FLOAT64)  AS C1,
          CAST({tx.C2} AS FLOAT64)  AS C2,
          CAST({tx.C5} AS FLOAT64)  AS C5,
          CAST({tx.C6} AS FLOAT64)  AS C6,
          CAST({tx.C13} AS FLOAT64) AS C13,
          CAST({tx.C14} AS FLOAT64) AS C14,
          CAST({nf(tx.D1)} AS FLOAT64)  AS D1,
          CAST({nf(tx.D4)} AS FLOAT64)  AS D4,
          CAST({nf(tx.D10)} AS FLOAT64) AS D10,
          CAST({nf(tx.D15)} AS FLOAT64) AS D15,
          CAST({sf(tx.M4)} AS STRING) AS M4,
          CAST({sf(tx.M5)} AS STRING) AS M5,
          CAST({sf(tx.M6)} AS STRING) AS M6
        ))
        """

        logger.info(f"Running prediction for amount={amount}")
        result    = bq_client.query(query).result()
        row       = next(iter(result))
        fraud_prob = round(float(row.fraud_probability), 6)
        risk = (
            "HIGH"   if fraud_prob >= 0.8 else
            "MEDIUM" if fraud_prob >= 0.5 else
            "LOW"    if fraud_prob >= 0.3 else "SAFE"
        )
        return {
            "fraud_probability" : fraud_prob,
            "predicted_fraud"   : int(row.predicted_isFraud),
            "risk_level"        : risk,
            "transaction_amount": amount,
            "model"             : "xgboost",
            "scored_at"         : datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
