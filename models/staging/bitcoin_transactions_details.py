import json
import pandas as pd


def parse_json_safe(data):
    if data is None:
        return []
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        return []


def model(dbt, session):
    # dbt configuration
    dbt.config(packages=["pandas"])

    # Load the data from the referenced model
    df = dbt.ref("bitcoin_transactions").to_pandas()

    if dbt.is_incremental:
        max_from_this = f"select max(ingestion_timestamp) from {dbt.this}"
        max_ingestion_timestamp = session.sql(max_from_this).collect()[0][0]

        if max_ingestion_timestamp is not None:
            df = df[df["INGESTION_TIMESTAMP"] > max_ingestion_timestamp]

    df["PARSED_TX_INPUTS_DATA"] = df["TX_INPUTS_DATA"].apply(parse_json_safe)

    df_exploded = df.explode("PARSED_TX_INPUTS_DATA")

    df_exploded[["ADDRESS_INPUT", "TX_HASH_2"]] = pd.json_normalize(
        df_exploded["PARSED_TX_INPUTS_DATA"]
    )[["address", "spent_transaction_hash"]]

    df = df_exploded[
        [
            "DATE",
            "TX_HASH",
            "TX_HASH_2",
            "BLOCK_HASH",
            "BLOCK_TIMESTAMP",
            "TX_INPUT_VALUE",
            "TX_OUTPUT_VALUE",
            "TX_FEE",
            "TX_IS_COINBASE",
            "ADDRESS_INPUT",
            "INGESTION_TIMESTAMP",
            "BATCH_ID",
        ]
    ]
    df = df.drop_duplicates()
    return df
