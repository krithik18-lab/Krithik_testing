import polars as pl
from datetime import datetime, timedelta
from sys import exit

# Assuming claims_df is a Polars DataFrame with the following schema:
# - cohort_id: identifier for the cohort (string)
# - cohort_name: human-readable cohort name (string)
# - claim_date: the date of the claim (date/datetime)
# - claim_amount: the dollar amount of the claim (float)


def compute_top_spending_cohorts(claims_df: pl.DataFrame, reference_date: str, number_of_rows: int = 3) -> pl.DataFrame:
    """
    Compute the top 3 outlier cohorts based on historical (up to last 24-months) spend.

    Parameters
    ----------
    claims_df : pl.DataFrame
        A DataFrame containing claims with columns: cohort_id, cohort_name, claim_date, claim_amount.
    reference_date : str
        A YYYY-MM-DD string indicating the month for which to compute outliers (e.g., '2025-04-30').
    number_of_rows : int
        The number of rows to return (default is 3).

    Returns
    -------
    pl.DataFrame
        A DataFrame with top 3 outlier cohorts sorted by spend, containing columns:
        cohort_id, cohort_name, spend
    """

    # Parse reference_date into datetime object
    reference_date = datetime.strptime(reference_date, "%Y-%m-%d")
    twenty_four_months_ago_from_reference_date = (reference_date - timedelta(days=365*2)).replace(day=1)

    # Create a LazyFrame for performance
    lf = claims_df.lazy()

    # 1) Extract historical (up to last 24-months) data
    previous_24_months_to_current_date = (
        lf
        .with_columns(
            pl.col("claim_date").cast(pl.Date).alias("date")
        )
        .filter(
            (pl.col("date") >= pl.lit(twenty_four_months_ago_from_reference_date)) &
            (pl.col("date") <= pl.lit(reference_date))
        )
        .group_by(pl.col("cohort_name"))
        .agg(
            pl.sum("claim_amount").alias("spend")
        )
        .sort(pl.col("spend"), descending=True, nulls_last=True)
        .limit(number_of_rows)
    )

    # Collect to materialize results
    return previous_24_months_to_current_date