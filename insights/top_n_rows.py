import polars as pl

def new_top_n_rows(df: pl.DataFrame, n: int, analysis_type: str):
    
    
    df = df.with_columns(pl.col("cohort_name").str.strip_chars()).filter(pl.col("cohort_name") != "*", pl.col("cohort_name") != "None")



    if analysis_type.lower() == "top spending":
        
        df = df.sort("spend", descending=True)
        
        df = df.head(n)

        df.columns = [col if col != "spend" else "value" for col in df.columns]
        return df
    
    elif analysis_type.lower() == "surge in spending" or analysis_type.lower() == "emerging spending":
        df = df.sort("pct_increase", descending=True)

        df = df.head(n)

        df.columns = [col if col != "pct_increase" else "value" for col in df.columns]
        df = df.select(["cohort_name", "value"])
     
   
        return df 
    else:
        print("Analysis type not recognized. Please use 'top spending' or 'surge in spending'or 'emerging spending'.")
        return None
