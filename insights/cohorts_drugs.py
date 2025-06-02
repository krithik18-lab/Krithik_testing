from glob import glob
import pandas as pd
import polars as pl
import re

# Get ATC drug name from a group
def get_cohort_drugs_usage(eg_nid):

    files = glob(f"/home/azureuser/Operations/{eg_nid}/DB_TRANSFORMATIONS_PROD/Helper_Scripts/Prep_Data/omop_data/rx/{eg_nid}*rx_diagnosis*.parquet")
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
        rx_diag_lf = pd.concat(dataframes, ignore_index=True)
    else:
        rx_diag_lf = pd.DataFrame()

    # for col in ["claim_id", "claim_status", "drug_source_value", "atc_level2_name"]:
    for col in ["claim_id", "claim_status", "atc_level2_name"]:    
        if col in rx_diag_lf.columns:
            rx_diag_lf[col] = rx_diag_lf[col].astype(str)

    # rx_diag_lf = rx_diag_lf[["claim_id", "claim_status", "drug_source_value", "atc_level2_name"]].drop_duplicates()
    rx_diag_lf = rx_diag_lf[["claim_id", "claim_status", "atc_level2_name"]].drop_duplicates()

    # Function to properly parse chronic_disease__1 column values
    def parse_list_string(s):
        if not isinstance(s, str):
            return None
        
        # Remove brackets
        s = s.strip('[]')
        
        # Split by spaces but preserve quoted strings
        pattern = r'\'[^\']*\'|None'
        matches = re.findall(pattern, s)
        
        # Process each item
        result = []
        for item in matches:
            if item == 'None':
                result.append(None)
            else:
                # Remove quotes from string items
                result.append(item.strip("'"))
        
        return result
    
    rx_diag_lf["atc_level2_name"] = rx_diag_lf["atc_level2_name"].apply(parse_list_string)

    rx_diag_lf = pl.from_pandas(rx_diag_lf).lazy().filter(pl.col("claim_status") != "Rejected").explode("atc_level2_name").filter(pl.col("atc_level2_name").is_not_null()).unique()

    files = glob(f"/home/azureuser/Operations/{eg_nid}/DB_TRANSFORMATIONS_PROD/Helper_Scripts/Prep_Data/omop_data/rx/{eg_nid}*rx_drug_and_cost*.parquet")
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
        rx_drug_cost_lf = pd.concat(dataframes, ignore_index=True)
    else:
        rx_drug_cost_lf = pd.DataFrame()

    for col in ["claim_id", "paid_amount", "paid_date"]:
        if col in rx_drug_cost_lf.columns:
            rx_drug_cost_lf[col] = rx_drug_cost_lf[col].astype(str)

    rx_drug_cost_lf = rx_drug_cost_lf[["claim_id", "paid_amount", "paid_date"]].drop_duplicates()

    rx_drug_cost_lf = pl.from_pandas(rx_drug_cost_lf).lazy().with_columns(pl.col("paid_amount").cast(pl.Float64), pl.col("paid_date").cast(pl.Date))

    # return rx_diag_lf.join(other=rx_drug_cost_lf, on="claim_id", how="inner", coalesce=True).rename({"paid_date": "claim_date", "paid_amount": "claim_amount", "drug_source_value": "cohort_id", "atc_level2_name": "cohort_name"})
    return rx_diag_lf.join(other=rx_drug_cost_lf, on="claim_id", how="inner", coalesce=True).rename({"paid_date": "claim_date", "paid_amount": "claim_amount", "atc_level2_name": "cohort_name"})

if __name__ == "__main__":
    print(get_cohort_drugs_usage("PS").collect())