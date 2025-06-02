import polars as pl
import os

def process_cohort_data(existing_df: pl.DataFrame, new_df: pl.DataFrame) -> str:
    # Combine both DataFrames (existing + new)
    df = pl.concat([existing_df, new_df])

    # Sort by 'pct_increase' descending and take the top result
    df_sorted = df.sort("pct_increase", descending=True).head(1)

    # Format output as text without headers
    output_lines = [
        f"{row['cohort_name']}---{row['pct_increase']:.2f}%"
        for row in df_sorted.iter_rows(named=True)
    ]

    return "\n".join(output_lines)


# === Paths ===
existing_path = "/home/azureuser/krithik_testing/cohort_spend.csv"  # this is your main file
new_path = "/home/azureuser/krithik_testing/new_data.csv"           
output_path = "/home/azureuser/krithik_testing/output.txt"

# === Load CSVs ===
existing_df = pl.read_csv(existing_path)

# Check for new data file; if not present, use an empty DataFrame
if os.path.exists(new_path):
    new_df = pl.read_csv(new_path)
else:
    new_df = pl.DataFrame({col: [] for col in existing_df.columns})

# === Process and Get Output ===
output = process_cohort_data(existing_df, new_df)

# === Print to terminal ===
print(output)

# === Append to output.txt ===
with open(output_path, "a") as f:
    f.write(output + "\n")
