def model(dbt, session):
    # dbt configuration
    dbt.config(packages=["pandas"])

    # Load the data from the referenced model
    df = dbt.ref("bitcoin_transactions")

    if dbt.is_incremental:
        # only new rows compared to max in current table
        max_from_this = f"select max(ingestion_timestamp) from {dbt.this}"
        df = df.filter(
            df.ingestion_timestamp > session.sql(max_from_this).collect()[0][0]
        )
        
        
    
    return df


#        def parse_json_safe(data):
#            if data is None:
#                return []
#            try:
#                return json.loads(data)
#            except json.JSONDecodeError as e:
#                dbt.log(f"Error parsing JSON: {e} | Data: {data}")
#                return []
#
#        df["PARSED_TX_INPUTS_DATA"] = df["TX_INPUTS_DATA"].apply(parse_json_safe)
#
#        # Explode the parsed JSON data
#        try:
#            df_exploded = df.explode("PARSED_TX_INPUTS_DATA")
#        except ValueError as e:
#            dbt.log(f"Error exploding PARSED_TX_INPUTS_DATA: {e}")
#            return pd.DataFrame()  # Return an empty dataframe in case of error
#
#        # Normalize JSON data into columns
#        try:
#            df_exploded[["ADDRESS_INPUT", "TX_HASH_2"]] = pd.json_normalize(
#                df_exploded["PARSED_TX_INPUTS_DATA"]
#            )[["address", "spent_transaction_hash"]]
#        except KeyError as e:
#            dbt.log(f"Error normalizing JSON data: {e}")
#            return pd.DataFrame()  # Return an empty dataframe in case of error
#
#        # Select the relevant columns
#        df1 = df_exploded[
#            [
#                "DATE",
#                "TX_HASH",
#                "TX_HASH_2",
#                "BLOCK_HASH",
#                "BLOCK_TIMESTAMP",
#                "TX_INPUT_VALUE",
#                "TX_OUTPUT_VALUE",
#                "TX_FEE",
#                "TX_IS_COINBASE",
#                "ADDRESS_INPUT",
#            ]
#        ]
#
#    return df1
