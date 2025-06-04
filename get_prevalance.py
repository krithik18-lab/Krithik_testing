import pandas as pd

def get_prevalence() -> pd.DataFrame:
    # Filter data for the latest plan year
    df = med_input_df_1[med_input_df_1["plan_year_latest"] == latest_plan_year]

    # Identify diagnosis columns
    diag_columns = [col for col in df.columns if "condition_" in col and "source_value" in col]

    # Collecting patient-condition pairs
    records = []
    for col in diag_columns:
        subset = df[["group_number", "person_id", "plan_year_latest", col]].copy()
        subset = subset.rename(columns={col: "ICD_code"}).dropna()
        subset = subset[subset["ICD_code"] != ""]
        records.append(subset)

    # Combining all chronic condition rows
    chronic_df = pd.concat(records).drop_duplicates() if records else pd.DataFrame(columns=["group_number", "person_id", "plan_year_latest", "ICD_code"])

    # Match ICD codes to disease names
    chronic_df = chronic_df.merge(chronic_dict, left_on="ICD_code", right_on="code", how="inner")
    chronic_df = chronic_df[["group_number", "plan_year_latest", "person_id", "disease"]].drop_duplicates()

    # Counting unique chronic diseases per patient
    chronic_counts = (
        chronic_df.groupby(["group_number", "plan_year_latest", "person_id"])
        .agg({"disease": lambda x: len(set(filter(None, x)))})
        .reset_index()
    )

    # All unique patients for the plan year
    all_patients = df[["group_number", "plan_year_latest", "person_id"]].drop_duplicates()

    # All patients WITHOUT chronic conditions
    chronic_patients = chronic_counts[["person_id"]].drop_duplicates()
    non_chronic = all_patients[~all_patients["person_id"].isin(chronic_patients["person_id"])]
    
    # non-chronic members with disease count = 0
    non_chronic = non_chronic.assign(disease=0)

    # Combining chronic and non-chronic members
    final_df = pd.concat([chronic_counts, non_chronic], ignore_index=True)

    return final_df


