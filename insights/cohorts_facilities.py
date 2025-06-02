from glob import glob
import pandas as pd
import polars as pl
from sys import exit

# Get medical facilities
def get_cohort_facilities(eg_nid):

    files = glob(f"/home/azureuser/Operations/{eg_nid}/DB_TRANSFORMATIONS_PROD/Helper_Scripts/Prep_Data/omop_data/medical/{eg_nid}*medical_cost*.parquet")
    dataframes = []
    for file in files:
        try:
            df = pd.read_parquet(file)
            if df is not None and not df.empty:
                df = df.dropna(axis=1, how='all')
                if not df.empty and len(df.columns) > 0:
                    dataframes.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")
                
    if dataframes:
        med_cost_lf = pd.concat(dataframes, ignore_index=True)
    else:
        med_cost_lf = pd.DataFrame()

    # for col in ["claim_line_id", "benifit_amount", "claim_status", "service_period_start_date", "pos_desc", "place_of_service_code_source_value"]:
    for col in ["claim_line_id", "benifit_amount", "claim_status", "service_period_start_date", "pos_desc"]:
        if col in med_cost_lf.columns:
            med_cost_lf[col] = med_cost_lf[col].astype(str)

    # med_cost_lf = med_cost_lf[["claim_line_id", "benifit_amount", "claim_status", "service_period_start_date", "pos_desc", "place_of_service_code_source_value"]].drop_duplicates()
    med_cost_lf = med_cost_lf[["claim_line_id", "benifit_amount", "claim_status", "service_period_start_date", "pos_desc"]].drop_duplicates()

    med_cost_lf = pl.from_pandas(med_cost_lf).lazy().filter(pl.col("claim_status") != "Rejected").with_columns(pl.col("benifit_amount").cast(pl.Float64))

    # return med_cost_lf.rename({"service_period_start_date": "claim_date", "place_of_service_code_source_value": "cohort_id", "benifit_amount": "claim_amount", "pos_desc": "cohort_name"})
    return med_cost_lf.rename({"service_period_start_date": "claim_date", "benifit_amount": "claim_amount", "pos_desc": "cohort_name"})

if __name__ == "__main__":
    print(get_cohort_facilities("PS").collect())