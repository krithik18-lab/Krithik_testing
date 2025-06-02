from glob import glob
import pandas as pd
import polars as pl
import re
from sys import exit

# Get ICD level 1 disease data from a group
def get_cohort_diseases_icd_level_1(eg_nid):

    files = glob(f"/home/azureuser/Operations/{eg_nid}/DB_TRANSFORMATIONS_PROD/Helper_Scripts/Prep_Data/omop_data/medical/{eg_nid}*medical_diagnosis*.parquet")
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
        med_diag_lf = pd.concat(dataframes, ignore_index=True)
    else:
        med_diag_lf = pd.DataFrame()

    for col in ["claim_line_id", "claim_status", "service_period_start_date", "condition_1_level1_desc"]:
        if col in med_diag_lf.columns:
            med_diag_lf[col] = med_diag_lf[col].astype(str)

    med_diag_lf = med_diag_lf[["claim_line_id", "claim_status", "service_period_start_date", "condition_1_level1_desc"]].drop_duplicates()

    med_diag_lf = pl.from_pandas(med_diag_lf).lazy().filter(pl.col("claim_status") != "Rejected").with_columns(pl.col("service_period_start_date").cast(pl.Date))

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

    return med_diag_lf.join(other=med_cost_lf, on="claim_line_id", how="inner", coalesce=True).filter(pl.col("condition_1_level1_desc") != "Factors influencing health status and contact with health services").rename({"service_period_start_date": "claim_date", "benifit_amount": "claim_amount", "condition_1_level1_desc": "cohort_name"})

# Get ICD level 2 disease data from a group
def get_cohort_diseases_icd_level_2(eg_nid):

    files = glob(f"/home/azureuser/Operations/{eg_nid}/DB_TRANSFORMATIONS_PROD/Helper_Scripts/Prep_Data/omop_data/medical/{eg_nid}*medical_diagnosis*.parquet")
    dataframes = []
    for file in files:
        try:
            df = pd.read_parquet(file)
            if df is not None and not df.empty:
                df = df.dropna(axis=1, how='all')
                if not df.empty and len(df.columns) > 0:
                    dataframes.append(df)
                else:
                    print(f"Skipping empty dataframe: {file}, df: {df}")
        except Exception as e:
            print(f"Error reading {file}: {e}")
                
    if dataframes:
        med_prod_lf = pd.concat(dataframes, ignore_index=True)
    else:
        med_prod_lf = pd.DataFrame()

    for col in ["claim_line_id", "5", "service_period_start_date", "condition_1_level2_desc"]:
        if col in med_prod_lf.columns:
            med_prod_lf[col] = med_prod_lf[col].astype(str)

    med_prod_lf = med_prod_lf[["claim_line_id", "claim_status", "service_period_start_date", "condition_1_level2_desc"]].drop_duplicates()

    med_prod_lf = pl.from_pandas(med_prod_lf).lazy().filter(pl.col("claim_status") != "Rejected").with_columns(pl.col("service_period_start_date").cast(pl.Date))

    files = glob(f"/home/azureuser/Operations/{eg_nid}/DB_TRANSFORMATIONS_PROD/Helper_Scripts/Prep_Data/omop_data/medical/{eg_nid}*medical_cost*.parquet")
    dataframes = []
    for file in files:
        try:
            df = pd.read_parquet(file)
            if df is not None and not df.empty:
                df = df.dropna(axis=1, how='all')
                if not df.empty and len(df.columns) > 0:
                    dataframes.append(df)
                else:
                    print(f"Skipping empty dataframe: {file}, df: {df}")
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

    return med_prod_lf.join(other=med_cost_lf, on="claim_line_id", how="inner", coalesce=True).filter(pl.col("condition_1_level2_desc").str.strip_chars().str.to_lowercase() != "persons encountering health services for examinations").rename({"service_period_start_date": "claim_date", "benifit_amount": "claim_amount", "condition_1_level2_desc": "cohort_name"})

# Get chronic disease data from a group
def get_cohort_diseases_chronic(eg_nid):
    
    files = glob(f"/home/azureuser/Operations/{eg_nid}/DB_TRANSFORMATIONS_PROD/Helper_Scripts/Prep_Data/omop_data/medical/{eg_nid}*medical_diagnosis*.parquet")
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
        med_prod_lf = pd.concat(dataframes, ignore_index=True)
    else:
        med_prod_lf = pd.DataFrame()

    for col in ["claim_line_id", "claim_status", "service_period_start_date", "chronic_disease__1"]:
        if col in med_prod_lf.columns:
            med_prod_lf[col] = med_prod_lf[col].astype(str)


    med_prod_lf = med_prod_lf[["claim_line_id", "claim_status", "service_period_start_date", "chronic_disease__1"]].drop_duplicates()

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
    
    med_prod_lf["chronic_disease__1"] = med_prod_lf["chronic_disease__1"].apply(parse_list_string)

    med_prod_lf = pl.from_pandas(med_prod_lf).lazy()\
    .filter(pl.col("claim_status") != "Rejected")\
    .with_columns(pl.col("service_period_start_date").cast(pl.Date))\
    .explode("chronic_disease__1").filter(pl.col("chronic_disease__1").is_not_null()).unique()

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

    return med_prod_lf.join(other=med_cost_lf, on="claim_line_id", how="inner", coalesce=True).rename({"service_period_start_date": "claim_date", "benifit_amount": "claim_amount", "chronic_disease__1": "cohort_name"})

# Get trigger level 2 disease data from a group
def get_cohort_diseases_trigger_level_2(eg_nid):
    
    files = glob(f"/home/azureuser/Operations/{eg_nid}/DB_TRANSFORMATIONS_PROD/Helper_Scripts/Prep_Data/omop_data/medical/{eg_nid}*medical_diagnosis*.parquet")
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
        med_diag_lf = pd.concat(dataframes, ignore_index=True)
    else:
        med_diag_lf = pd.DataFrame()

    for col in ["claim_line_id", "claim_status", "service_period_start_date", "trigger_level2_1"]:
        if col in med_diag_lf.columns:
            med_diag_lf[col] = med_diag_lf[col].astype(str)

    med_diag_lf = med_diag_lf[["claim_line_id", "claim_status", "service_period_start_date", "trigger_level2_1"]].drop_duplicates()

    med_diag_lf = pl.from_pandas(med_diag_lf).lazy().filter(pl.col("claim_status") != "Rejected").with_columns(pl.col("service_period_start_date").cast(pl.Date)).filter(pl.col("trigger_level2_1") != "None").unique()

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

    return med_diag_lf.join(other=med_cost_lf, on="claim_line_id", how="inner", coalesce=True).rename({"service_period_start_date": "claim_date", "benifit_amount": "claim_amount", "trigger_level2_1": "cohort_name"})

if __name__ == "__main__":
    print(get_cohort_diseases_icd_level_1("PS").collect())
    print(get_cohort_diseases_icd_level_2("FT").collect())
    print(get_cohort_diseases_chronic("PS").collect())
    print(get_cohort_diseases_trigger_level_2("PS").collect())