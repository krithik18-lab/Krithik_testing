from glob import glob
import pandas as pd
import polars as pl
from sys import exit

# Get individual medical provider
def get_cohort_individual_medical_provider(eg_nid):

    files = glob(f"/home/azureuser/Operations/{eg_nid}/DB_TRANSFORMATIONS_PROD/Helper_Scripts/Prep_Data/omop_data/medical/{eg_nid}*medical_provider*.parquet")
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
        med_prov_lf = pd.concat(dataframes, ignore_index=True)
    else:
        med_prov_lf = pd.DataFrame()

    # for col in ["claim_id", "pcp_npi_number", "provider_name"]:
    for col in ["claim_id", "provider_name"]:
        if col in med_prov_lf.columns:
            med_prov_lf[col] = med_prov_lf[col].astype(str)

    # med_prov_lf = med_prov_lf[["claim_id", "pcp_npi_number", "provider_name"]].drop_duplicates()
    med_prov_lf = med_prov_lf[["claim_id", "provider_name"]].drop_duplicates()

    med_prov_lf = pl.from_pandas(med_prov_lf).lazy()

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

    for col in ["claim_id", "claim_status", "benifit_amount", "service_period_start_date"]:
        if col in med_cost_lf.columns:
            med_cost_lf[col] = med_cost_lf[col].astype(str)

    med_cost_lf = med_cost_lf[["claim_id", "claim_status", "benifit_amount", "service_period_start_date"]].drop_duplicates()

    med_cost_lf = pl.from_pandas(med_cost_lf).lazy().filter(pl.col("claim_status") != "Rejected").with_columns(pl.col("benifit_amount").cast(pl.Float64), pl.col("service_period_start_date").cast(pl.Date))

    # return med_prov_lf.join(other=med_cost_lf, on="claim_id", how="inner", coalesce=True).rename({"service_period_start_date": "claim_date", "pcp_npi_number": "cohort_id", "benifit_amount": "claim_amount", "provider_name": "cohort_name"})
    return med_prov_lf.join(other=med_cost_lf, on="claim_id", how="inner", coalesce=True).rename({"service_period_start_date": "claim_date", "benifit_amount": "claim_amount", "provider_name": "cohort_name"})


# Get medical provider speciality
def get_cohort_medical_provider_speciality(eg_nid):

    files = glob(f"/home/azureuser/Operations/{eg_nid}/DB_TRANSFORMATIONS_PROD/Helper_Scripts/Prep_Data/omop_data/medical/{eg_nid}*medical_provider*.parquet")
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
        med_prov_lf = pd.concat(dataframes, ignore_index=True)
    else:
        med_prov_lf = pd.DataFrame()

    # for col in ["claim_id", "provider_type", "provider_speciality_desc"]:
    for col in ["claim_id", "provider_speciality_desc"]:
        if col in med_prov_lf.columns:
            med_prov_lf[col] = med_prov_lf[col].astype(str)

    # med_prov_lf = med_prov_lf[["claim_id", "provider_type", "provider_speciality_desc"]].drop_duplicates()
    med_prov_lf = med_prov_lf[["claim_id", "provider_speciality_desc"]].drop_duplicates()


    med_prov_lf = pl.from_pandas(med_prov_lf).lazy()

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

    for col in ["claim_id", "claim_status", "benifit_amount", "service_period_start_date"]:
        if col in med_cost_lf.columns:
            med_cost_lf[col] = med_cost_lf[col].astype(str)

    med_cost_lf = med_cost_lf[["claim_id", "claim_status", "benifit_amount", "service_period_start_date"]].drop_duplicates()

    med_cost_lf = pl.from_pandas(med_cost_lf).lazy().filter(pl.col("claim_status") != "Rejected").with_columns(pl.col("benifit_amount").cast(pl.Float64), pl.col("service_period_start_date").cast(pl.Date))

    # return med_prov_lf.join(other=med_cost_lf, on="claim_id", how="inner", coalesce=True).rename({"service_period_start_date": "claim_date", "provider_type": "cohort_id", "benifit_amount": "claim_amount", "provider_speciality_desc": "cohort_name"})
    return med_prov_lf.join(other=med_cost_lf, on="claim_id", how="inner", coalesce=True).rename({"service_period_start_date": "claim_date", "benifit_amount": "claim_amount", "provider_speciality_desc": "cohort_name"})


# Get individual rx provider
def get_cohort_individual_rx_provider(eg_nid):

    files = glob(f"/home/azureuser/Operations/{eg_nid}/DB_TRANSFORMATIONS_PROD/Helper_Scripts/Prep_Data/omop_data/rx/{eg_nid}*rx_provider*.parquet")
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
        rx_prov_lf = pd.concat(dataframes, ignore_index=True)
    else:
        rx_prov_lf = pd.DataFrame()

    # for col in ["claim_id", "claim_status", "prescriber_provider_npi", "prescriber_name"]:
    for col in ["claim_id", "claim_status", "prescriber_name"]:
        if col in rx_prov_lf.columns:
            rx_prov_lf[col] = rx_prov_lf[col].astype(str)

    # rx_prov_lf = rx_prov_lf[["claim_id", "claim_status", "prescriber_provider_npi", "prescriber_name"]].drop_duplicates()
    rx_prov_lf = rx_prov_lf[["claim_id", "claim_status", "prescriber_name"]].drop_duplicates()

    rx_prov_lf = pl.from_pandas(rx_prov_lf).lazy().filter(pl.col("claim_status") != "Rejected")

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

    # return rx_prov_lf.join(other=rx_drug_cost_lf, on="claim_id", how="inner", coalesce=True).rename({"paid_date": "claim_date", "prescriber_provider_npi": "cohort_id", "paid_amount": "claim_amount", "prescriber_name": "cohort_name"})
    return rx_prov_lf.join(other=rx_drug_cost_lf, on="claim_id", how="inner", coalesce=True).rename({"paid_date": "claim_date", "paid_amount": "claim_amount", "prescriber_name": "cohort_name"})

if __name__ == "__main__":
    print(get_cohort_individual_medical_provider("PS").collect())
    print(get_cohort_medical_provider_speciality("PS").collect())
    print(get_cohort_individual_rx_provider("PS").collect())