import json
import time
import random
import argparse
from datetime import datetime
from google.cloud import pubsub_v1

# ── Configuration ──────────────────────────────────────────────────────────────
TOPIC_ID = "fraud-transactions"

# Realistic value pools derived from IEEE-CIS dataset analysis
PRODUCT_CODES   = ["C", "H", "R", "S", "W"]
PRODUCT_WEIGHTS = [0.40, 0.10, 0.35, 0.10, 0.05]   # C and R dominate

CARD_TYPES      = ["visa", "mastercard", "american express", "discover"]
CARD_WEIGHTS    = [0.55, 0.30, 0.10, 0.05]

CARD_CATEGORIES = ["credit", "debit"]
CARD_CAT_WEIGHTS= [0.60, 0.40]

EMAIL_DOMAINS   = [
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "icloud.com", "protonmail.com", "anonymous.com", "mail.ru"
]
EMAIL_WEIGHTS   = [0.40, 0.20, 0.15, 0.10, 0.05, 0.04, 0.03, 0.03]

DEVICE_TYPES    = ["desktop", "mobile", None]
DEVICE_WEIGHTS  = [0.55, 0.35, 0.10]

# High-risk email domains (higher fraud probability)
HIGH_RISK_DOMAINS = {"anonymous.com", "mail.ru", "protonmail.com"}


def generate_transaction(is_fraud: bool = False) -> dict:
    """Generate a single realistic transaction JSON."""

    # Amount generation — fraud tends toward mid-high amounts
    if is_fraud:
        # Fraud: bimodal — either suspiciously small (testing card) or large
        if random.random() < 0.3:
            amount = round(random.uniform(1.0, 20.0), 2)   # card testing
        else:
            amount = round(random.uniform(100.0, 2000.0), 2)
    else:
        # Legit: log-normal distribution (most txns are small)
        amount = round(min(random.lognormvariate(3.5, 1.2), 5000.0), 2)

    email_domain = random.choices(EMAIL_DOMAINS, EMAIL_WEIGHTS)[0]
    from datetime import timezone
    now = datetime.now(timezone.utc)
    hour         = now.hour
    is_late_night= 1 if hour < 6 else 0

    transaction = {
        # Identifiers
        "TransactionID"  : random.randint(3000000, 9999999),
        "isFraud"        : 1 if is_fraud else 0,

        # Core fields
        "TransactionDT"  : int(time.time()),
        "TransactionAmt" : amount,
        "ProductCD"      : random.choices(PRODUCT_CODES, PRODUCT_WEIGHTS)[0],

        # Card details
        "card1"          : random.randint(1000, 18000),
        "card2"          : round(random.uniform(100.0, 600.0), 1),
        "card3"          : random.choice([150.0, 185.0, None]),
        "card4"          : random.choices(CARD_TYPES, CARD_WEIGHTS)[0],
        "card5"          : round(random.uniform(100.0, 240.0), 1),
        "card6"          : random.choices(CARD_CATEGORIES, CARD_CAT_WEIGHTS)[0],

        # Address
        "addr1"          : random.choice([None, random.randint(100, 540)]),
        "addr2"          : random.choice([None, 87.0, 96.0, 60.0]),
        "dist1"          : None if random.random() < 0.6 else round(random.uniform(0, 10000), 1),
        "dist2"          : None if random.random() < 0.9 else round(random.uniform(0, 10000), 1),

        # Email
        "P_emaildomain"  : email_domain,
        "R_emaildomain"  : random.choice([email_domain, None, "gmail.com"]),

        # Count features (C1-C14) — kept simple
        "C1"  : float(random.randint(0, 4)),
        "C2"  : float(random.randint(0, 4)),
        "C3"  : 0.0,
        "C4"  : float(random.randint(0, 2)),
        "C5"  : float(random.randint(0, 3)),
        "C6"  : float(random.randint(0, 4)),
        "C13" : float(random.randint(0, 50)),
        "C14" : float(random.randint(0, 30)),

        # Device info
        "DeviceType"     : random.choices(DEVICE_TYPES, DEVICE_WEIGHTS)[0],
        "DeviceInfo"     : random.choice(["Windows", "iOS Device", "MacOS", "Android", None]),

        # Engineered features (pre-computed for streaming)
        "tx_hour"        : hour,
        "tx_day_of_week" : now.weekday(),
        "is_late_night"  : is_late_night,
        "log_tx_amt"     : round(__import__('math').log(amount + 1), 4),
        "email_is_null"  : 0,
        "device_is_null" : 0 if random.choices(DEVICE_TYPES, DEVICE_WEIGHTS)[0] else 1,

        # Metadata
        "ingested_at"    : now.strftime("%Y-%m-%d %H:%M:%S.%f")
    }

    return transaction


def publish_transactions(project_id: str, num_messages: int, fraud_rate: float, delay: float):
    """Publish transactions to Pub/Sub topic."""

    publisher   = pubsub_v1.PublisherClient()
    topic_path  = publisher.topic_path(project_id, TOPIC_ID)

    print(f"\n🚀 Starting transaction publisher")
    print(f"   Topic      : {topic_path}")
    print(f"   Messages   : {num_messages} ({'infinite' if num_messages == -1 else num_messages})")
    print(f"   Fraud rate : {fraud_rate * 100:.1f}%")
    print(f"   Delay      : {delay}s between messages")
    print(f"{'─' * 50}\n")

    sent        = 0
    fraud_sent  = 0
    errors      = 0

    try:
        while num_messages == -1 or sent < num_messages:
            is_fraud    = random.random() < fraud_rate
            transaction = generate_transaction(is_fraud=is_fraud)

            message_data = json.dumps(transaction).encode("utf-8")

            try:
                future = publisher.publish(
                    topic_path,
                    data=message_data,
                    # Pub/Sub message attributes for filtering
                    is_fraud=str(transaction["isFraud"]),
                    product_cd=transaction["ProductCD"]
                )
                future.result()  # Wait for publish confirmation

                sent += 1
                if is_fraud:
                    fraud_sent += 1

                # Progress log every 10 messages
                if sent % 10 == 0:
                    print(f"✅ Sent: {sent:>6} | Fraud: {fraud_sent:>5} ({fraud_sent/sent*100:.1f}%) | Errors: {errors}")

            except Exception as e:
                errors += 1
                print(f"❌ Publish error: {e}")

            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\n\n⛔ Stopped by user")

    finally:
        print(f"\n{'─' * 50}")
        print(f"📊 Final stats:")
        print(f"   Total sent  : {sent}")
        print(f"   Fraud sent  : {fraud_sent}")
        print(f"   Legit sent  : {sent - fraud_sent}")
        print(f"   Errors      : {errors}")
        print(f"{'─' * 50}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fraud transaction Pub/Sub publisher")
    parser.add_argument("--project-id",   required=True,        help="GCP project ID")
    parser.add_argument("--num-messages", type=int, default=100, help="Number of messages (-1 for infinite)")
    parser.add_argument("--fraud-rate",   type=float, default=0.10, help="Fraud rate 0.0-1.0 (default 0.10 = 10%)")
    parser.add_argument("--delay",        type=float, default=0.5,  help="Delay in seconds between messages")
    args = parser.parse_args()

    publish_transactions(
        project_id   = args.project_id,
        num_messages = args.num_messages,
        fraud_rate   = args.fraud_rate,
        delay        = args.delay
    )
