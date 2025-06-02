from datetime import datetime, timedelta
import polars as pl
from sys import exit
# Assuming claims_df is a Polars DataFrame with the following schema:
# - cohort_id: identifier for the cohort (string)
# - cohort_name: human-readable cohort name (string)
# - claim_date: the date of the claim (date/datetime)
# - claim_amount: the dollar amount of the claim (float)


def compute_emerging_cohorts(claims_df: pl.DataFrame, reference_date: str, number_of_rows: int = 3, include_infinity_percentage: bool = False) -> pl.DataFrame:
    """
    Compute the top  emergent cohorts based on last 12 months average spend vs. last 3 years average spend.

    Parameters
    ----------
    claims_df : pl.DataFrame
        A DataFrame containing claims with columns: cohort_id, cohort_name, claim_date, claim_amount.
    reference_date : str
        A YYYY-MM-DD string indicating the month for which to compute outliers (e.g., '2025-04-30').
    number_of_rows : int
        The number of rows to return (default is 3).
    include_infinity_percentage : bool
        Whether to include cohorts with infinite percentage increase, observed with the average of historical spend is zero (default is False).

    Returns
    -------
    pl.DataFrame
        A DataFrame with top 'number_of_rows' outlier cohorts sorted by percent increase, containing columns:
        cohort_id, cohort_name, avg_1y_spend, historical_24m, pct_increase
    """

    # Create a LazyFrame for performance
    lf = claims_df.lazy()

    # Parse reference_date into datetime object
    reference_date = datetime.strptime(reference_date, "%Y-%m-%d")
    last_1_year_from_reference_date = (reference_date - timedelta(days=365))
    three_years_ago_from_reference_date = (reference_date - timedelta(days=365*3))

    earliest_claim_date = lf.collect().sort(pl.col("claim_date"), descending=False, nulls_last=True).head(1).select(pl.col("claim_date")).item()
    earliest_claim_date = earliest_claim_date.date() if isinstance(earliest_claim_date, datetime) else earliest_claim_date

    # Ensure reference_date is a date object
    reference_date = reference_date.date() if isinstance(reference_date, datetime) else reference_date

    # Proceed only if earliest claim date is beyond 2 years ago, in order to ensure presence of reasonable amount of historical data
    if earliest_claim_date > reference_date - timedelta(days=365*2):
        #print("Emergent spending data is not available, due to the lack of sufficient amount of historical data.")
        return None

    # 1) Extract last 3 months average data
    last_1_year_average_spend = (
        lf
        .with_columns(
            pl.col("claim_date").cast(pl.Date).alias("date")
        )
        .filter(
            (pl.col("date") >= pl.lit(last_1_year_from_reference_date)) &
            (pl.col("date") <= pl.lit(reference_date))
        )
        .group_by(pl.col("cohort_name"))
        .agg(
            (pl.sum("claim_amount")/12).alias("avg_1y_spend")
        )
        .with_columns(pl.col("avg_1y_spend").fill_null(0.0).fill_nan(0.0))
        .filter(pl.col("avg_1y_spend") > 0)
    )

    # 2) Compute 24-month window prior to reference month
    historical_3_years = (
        lf
        .with_columns(
            pl.col("claim_date").cast(pl.Date).alias("date")
        )
        .filter(
            (pl.col("date") >= pl.lit(three_years_ago_from_reference_date)) &
            (pl.col("date") < pl.lit(reference_date))
        )
        .group_by(pl.col("cohort_name"))
        .agg(
            (pl.sum("claim_amount")/36).alias("avg_3y_spend")
        )
        .with_columns(pl.col("avg_3y_spend").fill_null(0.0).fill_nan(0.0))
    )

    # 3) Join last 3 months average and historical averages
    if include_infinity_percentage:
        outlier_df = (
        last_1_year_average_spend
        .join(historical_3_years, on=["cohort_name"], how="left")
        .with_columns(
            pl.when(pl.col("avg_3y_spend") != 0)
            .then(((pl.col("avg_1y_spend") - pl.col("avg_3y_spend")) / 
                pl.col("avg_3y_spend")) * 100)
            .otherwise(float("inf"))
            .alias("pct_increase")
        )
        .sort(["avg_1y_spend", "pct_increase"], descending=True, nulls_last=True)
        .limit(number_of_rows)
    )
    else:
        outlier_df = (
        last_1_year_average_spend
        .join(historical_3_years, on=["cohort_name"], how="left")
        .with_columns(
            pl.when(pl.col("avg_3y_spend") != 0)
            .then(((pl.col("avg_1y_spend") - pl.col("avg_3y_spend")) / 
                pl.col("avg_3y_spend")) * 100)
            .otherwise(float("inf"))
            .alias("pct_increase")
        )
        .filter(pl.col("pct_increase") != float("inf"))
        .sort(["avg_1y_spend", "pct_increase"], descending=True, nulls_last=True)
        .limit(number_of_rows)
    )
    

    # Collect to materialize results
    return outlier_df