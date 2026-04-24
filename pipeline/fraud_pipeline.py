import json
import logging
import argparse
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Required fields for validation ────────────────────────────────────────────
REQUIRED_FIELDS = ["TransactionID", "TransactionAmt", "ProductCD"]

# ── BigQuery table schema ──────────────────────────────────────────────────────
BQ_SCHEMA = {
    "fields": [
        {"name": "TransactionID",  "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "isFraud",        "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "TransactionDT",  "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "TransactionAmt", "type": "FLOAT",   "mode": "NULLABLE"},
        {"name": "ProductCD",      "type": "STRING",  "mode": "NULLABLE"},
        {"name": "card1",          "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "card4",          "type": "STRING",  "mode": "NULLABLE"},
        {"name": "card6",          "type": "STRING",  "mode": "NULLABLE"},
        {"name": "addr1",          "type": "FLOAT",   "mode": "NULLABLE"},
        {"name": "addr2",          "type": "FLOAT",   "mode": "NULLABLE"},
        {"name": "dist1",          "type": "FLOAT",   "mode": "NULLABLE"},
        {"name": "P_emaildomain",  "type": "STRING",  "mode": "NULLABLE"},
        {"name": "R_emaildomain",  "type": "STRING",  "mode": "NULLABLE"},
        {"name": "DeviceType",     "type": "STRING",  "mode": "NULLABLE"},
        {"name": "DeviceInfo",     "type": "STRING",  "mode": "NULLABLE"},
        {"name": "tx_hour",        "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "tx_day_of_week", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "is_late_night",  "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "log_tx_amt",     "type": "FLOAT",   "mode": "NULLABLE"},
        {"name": "email_is_null",  "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "device_is_null", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "ingested_at",    "type": "TIMESTAMP","mode": "NULLABLE"},
    ]
}


# ── Transform functions ────────────────────────────────────────────────────────

class ParseTransactionFn(beam.DoFn):
    """Parse raw Pub/Sub JSON message into a dict."""

    def process(self, element):
        try:
            # element is bytes from Pub/Sub
            record = json.loads(element.decode("utf-8"))
            yield record
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Failed to parse message: {e} | Raw: {element[:100]}")
            # Bad messages are silently dropped — don't crash the pipeline


class ValidateTransactionFn(beam.DoFn):
    """Validate required fields. Route invalid records to dead-letter tag."""

    INVALID_TAG = "invalid"

    def process(self, element):
        missing = [f for f in REQUIRED_FIELDS if not element.get(f)]
        if missing:
            logger.warning(f"Invalid record missing {missing}: TransactionID={element.get('TransactionID')}")
            yield beam.pvalue.TaggedOutput(self.INVALID_TAG, element)
        else:
            yield element

class EnrichTransactionFn(beam.DoFn):
    """Add/clean fields before writing to BigQuery."""

    def process(self, element):
        from datetime import datetime, timezone

        # Always overwrite ingested_at with a clean pipeline timestamp
        # This avoids any format issues from the generator side
        element["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

        # Ensure TransactionID is integer
        element["TransactionID"] = int(element["TransactionID"])

        # Ensure numeric fields are correct types
        if element.get("TransactionAmt"):
            element["TransactionAmt"] = float(element["TransactionAmt"])
        if element.get("card1"):
            element["card1"] = int(element["card1"])
        if element.get("addr1"):
            element["addr1"] = float(element["addr1"])
        if element.get("addr2"):
            element["addr2"] = float(element["addr2"])
        if element.get("dist1"):
            element["dist1"] = float(element["dist1"])

        # Keep only fields that exist in BQ schema
        schema_fields = {f["name"] for f in BQ_SCHEMA["fields"]}
        cleaned = {k: v for k, v in element.items() if k in schema_fields}

        yield cleaned

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-id",    required=True,  help="GCP project ID")
    parser.add_argument("--subscription",  required=True,  help="Pub/Sub subscription path")
    parser.add_argument("--bq-table",      required=True,  help="BigQuery table: project:dataset.table")
    parser.add_argument("--runner",        default="DirectRunner", help="DirectRunner or DataflowRunner")
    parser.add_argument("--temp-location", default=None,   help="GCS temp path (required for Dataflow)")
    parser.add_argument("--max-messages",  type=int, default=100, help="DirectRunner only: stop after N messages")
    known_args, pipeline_args = parser.parse_known_args(argv)

    # ── Pipeline options ───────────────────────────────────────────────────────
    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).runner   = known_args.runner
    options.view_as(StandardOptions).streaming = True

    if known_args.runner == "DataflowRunner":
        google_opts = options.view_as(GoogleCloudOptions)
        google_opts.project      = known_args.project_id
        google_opts.region       = "asia-south1"
        google_opts.temp_location= known_args.temp_location
        options.view_as(SetupOptions).save_main_session = True

    logger.info(f"Starting pipeline | Runner: {known_args.runner}")
    logger.info(f"Subscription: {known_args.subscription}")
    logger.info(f"BQ Table: {known_args.bq_table}")

    # ── Pipeline definition ────────────────────────────────────────────────────
    with beam.Pipeline(options=options) as p:

        # Step 1: Read from Pub/Sub
        raw_messages = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
                subscription=known_args.subscription
            )
        )

        # Step 2: Parse JSON
        parsed = (
            raw_messages
            | "ParseJSON" >> beam.ParDo(ParseTransactionFn())
        )

        # Step 3: Validate — split into valid and invalid
        validated = (
            parsed
            | "ValidateFields" >> beam.ParDo(
                ValidateTransactionFn()
            ).with_outputs(
                ValidateTransactionFn.INVALID_TAG,
                main="valid"
            )
        )

        # Step 4: Enrich valid records
        enriched = (
            validated.valid
            | "EnrichRecords" >> beam.ParDo(EnrichTransactionFn())
        )

        # Step 5: Write to BigQuery (streaming insert)
        enriched | "WriteToBigQuery" >> WriteToBigQuery(
            table=known_args.bq_table,
            schema=BQ_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            method="STREAMING_INSERTS"
        )

        # Step 6: Log invalid records (dead letter)
        validated.invalid | "LogInvalidRecords" >> beam.Map(
            lambda r: logger.warning(f"DEAD LETTER: {r.get('TransactionID')}")
        )

    logger.info("Pipeline finished.")


if __name__ == "__main__":
    run()
