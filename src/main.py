import pandas as pd
import yaml
from utils import run_query, download_financial_data, calculate_vwap, upload_to_sheets

if __name__ == "__main__":
    # Load query from YAML file
    with open('src/queries/queries.yaml', 'r') as f:
        QUERIES = yaml.safe_load(f)

    # Get the single query directly (no loop needed since there's only one query)
    query_sql = QUERIES['monthly_af_unrealized_payouts']['sql']
    rows, cols = run_query(query_sql)
    payouts_df = pd.DataFrame(rows, columns=cols)
    payouts_df['month'] = payouts_df['month'].astype(str)

    algousd = download_financial_data('ALGO-USD')
    vwap = calculate_vwap(algousd)
    upload_to_sheets(payouts_df, 'AF Unrealized Payouts')
    upload_to_sheets(vwap, 'VWAP')
