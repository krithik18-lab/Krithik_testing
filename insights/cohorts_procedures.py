from glob import glob
import pandas as pd
import polars as pl
from sys import exit

# Get medical procedures
def get_cohort_procedures(eg_nid):

    files = glob(f"/home/azureuser/Operations/{eg_nid}/DB_TRANSFORMATIONS_PROD/Helper_Scripts/Prep_Data/omop_data/medical/{eg_nid}*medical_procedure*.parquet")
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
        med_proc_lf = pd.concat(dataframes, ignore_index=True)
    else:
        med_proc_lf = pd.DataFrame()

    # for col in ["claim_line_id", "claim_status", "service_period_start_date", "type_of_service_code", "type_of_service"]:
    for col in ["claim_line_id", "claim_status", "service_period_start_date", "type_of_service"]:
        if col in med_proc_lf.columns:
            med_proc_lf[col] = med_proc_lf[col].astype(str)

    # med_proc_lf = med_proc_lf[["claim_line_id", "claim_status", "service_period_start_date", "type_of_service_code","type_of_service"]].drop_duplicates()
    med_proc_lf = med_proc_lf[["claim_line_id", "claim_status", "service_period_start_date", "type_of_service"]].drop_duplicates()

    med_proc_lf = pl.from_pandas(med_proc_lf).lazy().filter(pl.col("claim_status") != "Rejected").with_columns(pl.col("service_period_start_date").cast(pl.Date)).lazy()

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

    for col in ["claim_line_id", "benifit_amount"]:
        if col in med_cost_lf.columns:
            med_cost_lf[col] = med_cost_lf[col].astype(str)

    med_cost_lf = med_cost_lf[["claim_line_id", "benifit_amount"]].drop_duplicates()

    med_cost_lf = pl.from_pandas(med_cost_lf).lazy().with_columns(pl.col("benifit_amount").cast(pl.Float64))

    # return med_proc_lf.join(other=med_cost_lf, on="claim_line_id", how="inner", coalesce=True).rename({"service_period_start_date": "claim_date", "type_of_service_code": "cohort_id", "benifit_amount": "claim_amount","type_of_service":"cohort_name"})
    return med_proc_lf.join(other=med_cost_lf, on="claim_line_id", how="inner", coalesce=True).rename({"service_period_start_date": "claim_date", "benifit_amount": "claim_amount","type_of_service":"cohort_name"})

if __name__ == "__main__":
    print(get_cohort_procedures("PS").collect().filter(pl.col("cohort_name").str.strip_chars() == "Procedure (Musculoskeletal)"))