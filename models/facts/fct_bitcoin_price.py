import pandas as pd  # Import pandas within the function

def model(dbt, session):
    
    # dbt configuration
    dbt.config(packages=["pandas"])

    # Reference the table and convert it to a pandas DataFrame
    df = dbt.ref("bitcoin_transactions").to_pandas()

    # Extract unique dates from the DataFrame
    dates = df['DATE'].unique()

    # Create a new DataFrame from the unique dates
    result_df = pd.DataFrame(dates, columns=['unique_dates'])

    return result_df
