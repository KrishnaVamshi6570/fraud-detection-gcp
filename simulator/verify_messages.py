import json
from google.cloud import pubsub_v1

PROJECT_ID    = None   # Set via argument
SUBSCRIPTION  = "fraud-transactions-sub"

def pull_and_print(project_id: str, max_messages: int = 10):
    """Pull and display messages from subscription."""

    subscriber = pubsub_v1.SubscriberClient()
    sub_path   = subscriber.subscription_path(project_id, SUBSCRIPTION)

    print(f"\n📥 Pulling up to {max_messages} messages from {sub_path}\n")

    response = subscriber.pull(
        request={"subscription": sub_path, "max_messages": max_messages}
    )

    if not response.received_messages:
        print("⚠️  No messages found. Run the generator first.")
        return

    ack_ids = []
    for i, msg in enumerate(response.received_messages, 1):
        data = json.loads(msg.message.data.decode("utf-8"))
        print(f"Message {i}:")
        print(f"  TransactionID : {data['TransactionID']}")
        print(f"  isFraud       : {'🔴 FRAUD' if data['isFraud'] else '🟢 Legit'}")
        print(f"  Amount        : ${data['TransactionAmt']}")
        print(f"  ProductCD     : {data['ProductCD']}")
        print(f"  Card type     : {data['card4']}")
        print(f"  Email domain  : {data['P_emaildomain']}")
        print(f"  Device        : {data['DeviceType']}")
        print(f"  Hour          : {data['tx_hour']} ({'🌙 Late night' if data['is_late_night'] else '☀️ Daytime'})")
        print(f"  Ingested at   : {data['ingested_at']}")
        print()
        ack_ids.append(msg.ack_id)

    # Acknowledge messages so they're not redelivered
    subscriber.acknowledge(
        request={"subscription": sub_path, "ack_ids": ack_ids}
    )
    print(f"✅ Acknowledged {len(ack_ids)} messages")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--max-messages", type=int, default=10)
    args = parser.parse_args()
    pull_and_print(args.project_id, args.max_messages)
