from glob import glob
import pandas as pd
import polars as pl
from sys import exit

# Get employees ages from a group
def get_cohort_demographics_ages(eg_nid):

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

    # for col in ["claim_line_id", "claim_status", "benifit_amount", "service_period_start_date", "person_id", "date_of_birth"]:
    for col in ["claim_line_id", "claim_status", "benifit_amount", "service_period_start_date", "date_of_birth"]:
        if col in med_cost_lf.columns:
            med_cost_lf[col] = med_cost_lf[col].astype(str)

    # med_cost_lf = med_cost_lf[["claim_line_id", "claim_status", "benifit_amount", "service_period_start_date", "person_id", "date_of_birth"]].drop_duplicates()
    med_cost_lf = med_cost_lf[["claim_line_id", "claim_status", "benifit_amount", "service_period_start_date", "date_of_birth"]].drop_duplicates()

    med_cost_lf = pl.from_pandas(med_cost_lf).lazy().filter(pl.col("date_of_birth") != "None", pl.col("date_of_birth") != "nan", pl.col("claim_status") != "Rejected").with_columns(pl.col("benifit_amount").cast(pl.Float64), pl.col("service_period_start_date").cast(pl.Date), pl.col("date_of_birth").cast(pl.Date))

    med_cost_lf = med_cost_lf.with_columns((pl.col("service_period_start_date").dt.year() - pl.col("date_of_birth").dt.year()).alias("age")).with_columns(pl.when(pl.col("age").is_between(0, 18)).then(pl.lit("0-18")).when(pl.col("age").is_between(19, 26)).then(pl.lit("19-26")).when(pl.col("age").is_between(27, 40)).then(pl.lit("27-40")).when(pl.col("age").is_between(41, 64)).then(pl.lit("41-64")).otherwise(pl.lit("65+")).alias("age_bracket"))

    # return med_cost_lf.rename({"service_period_start_date": "claim_date", "person_id": "cohort_id", "benifit_amount": "claim_amount", "age_bracket": "cohort_name"})
    return med_cost_lf.rename({"service_period_start_date": "claim_date", "benifit_amount": "claim_amount", "age_bracket": "cohort_name"})

# Get employees genders from a group
def get_cohort_demographics_genders(eg_nid):

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

    # for col in ["claim_line_id", "claim_status", "benifit_amount", "service_period_start_date", "person_id", "gender_source_value"]:
    for col in ["claim_line_id", "claim_status", "benifit_amount", "service_period_start_date", "gender_source_value"]:
        if col in med_cost_lf.columns:
            med_cost_lf[col] = med_cost_lf[col].astype(str)

    # med_cost_lf = med_cost_lf[["claim_line_id", "claim_status", "benifit_amount", "service_period_start_date", "person_id", "gender_source_value"]].drop_duplicates()
    med_cost_lf = med_cost_lf[["claim_line_id", "claim_status", "benifit_amount", "service_period_start_date", "gender_source_value"]].drop_duplicates()

    med_cost_lf = pl.from_pandas(med_cost_lf).lazy().filter(pl.col("claim_status") != "Rejected").with_columns(pl.col("benifit_amount").cast(pl.Float64), pl.col("service_period_start_date").cast(pl.Date))

    # return med_cost_lf.rename({"service_period_start_date": "claim_date", "person_id": "cohort_id", "benifit_amount": "claim_amount", "gender_source_value": "cohort_name"})
    return med_cost_lf.rename({"service_period_start_date": "claim_date", "benifit_amount": "claim_amount", "gender_source_value": "cohort_name"})

# Get employees relationship description from a group
def get_cohort_demographics_relationships(eg_nid):

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

    # for col in ["claim_line_id", "claim_status", "benifit_amount", "service_period_start_date", "person_id", "relationship_desc"]:
    for col in ["claim_line_id", "claim_status", "benifit_amount", "service_period_start_date", "relationship_desc"]:
        if col in med_cost_lf.columns:
            med_cost_lf[col] = med_cost_lf[col].astype(str)

    # med_cost_lf = med_cost_lf[["claim_line_id", "claim_status", "benifit_amount", "service_period_start_date", "person_id", "relationship_desc"]].drop_duplicates()
    med_cost_lf = med_cost_lf[["claim_line_id", "claim_status", "benifit_amount", "service_period_start_date", "relationship_desc"]].drop_duplicates()

    med_cost_lf = pl.from_pandas(med_cost_lf).lazy().filter(pl.col("claim_status") != "Rejected").with_columns(pl.col("benifit_amount").cast(pl.Float64), pl.col("service_period_start_date").cast(pl.Date))

    # return med_cost_lf.rename({"service_period_start_date": "claim_date", "person_id": "cohort_id", "benifit_amount": "claim_amount", "relationship_desc": "cohort_name"})
    return med_cost_lf.rename({"service_period_start_date": "claim_date", "benifit_amount": "claim_amount", "relationship_desc": "cohort_name"})

if __name__ == "__main__":
    print(get_cohort_demographics_ages("PS").collect())
    print(get_cohort_demographics_genders("PS").collect())
    print(get_cohort_demographics_relationships("PS").collect())