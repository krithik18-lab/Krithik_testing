import polars as pl
from pymongo import MongoClient, DESCENDING, collection as PyMongoCollection
import datetime
from datetime import date
from typing import List, Dict, Any, Optional, Tuple
import os
from dotenv import load_dotenv

def insert_spending_analysis(
    df: pl.DataFrame,
    analysis_type: str,
    collection: PyMongoCollection.Collection, # Expects a Pymongo Collection object
    eg_nid: str,
    reference_date: str = date.today().isoformat()
) -> Optional[Any]:
    """
    Inserts data from a Polars DataFrame into the provided MongoDB collection
    according to the specified schema and logic.

    Args:
        df (pl.DataFrame): Input Polars DataFrame with 'cohort_name' (str)
                           and 'spend' (float) columns.
        analysis_type (str): The type of analysis (e.g., 'top spending',
                             'surge in spending', 'emerging spending').
        collection (pymongo.collection.Collection): The MongoDB collection object
                                                    to insert data into.
        eg_nid (str): The employer identifier
        reference_date (str, optional): The reference date for the analysis.
                                        Defaults to the current date.

    Returns:
        Optional[Any]: The ObjectId of the inserted document, or None if insertion failed.
    """
    # Input validation
    if not isinstance(df, pl.DataFrame):
        raise TypeError("Input 'df' must be a Polars DataFrame.")
    if not all(col in df.columns for col in ['cohort_name', 'value']):
        raise ValueError("DataFrame must contain 'cohort_name' and 'value' columns.")
    if df['cohort_name'].dtype != pl.String and df['cohort_name'].dtype != pl.Utf8: # Polars uses Utf8 for strings
        raise TypeError("DataFrame 'cohort_name' column must be of string type.")
    if df['value'].dtype != pl.Float64:
        raise TypeError("DataFrame 'value' column must be of f64 (float) type.")
    if analysis_type not in ['top spending', 'surge in spending', 'emerging spending']:
        print(f"Warning: analysis_type '{analysis_type}' is not one of the expected values.")
    if not isinstance(collection, PyMongoCollection.Collection):
        raise TypeError("Input 'collection' must be a pymongo.collection.Collection instance.")
    if not isinstance(eg_nid, str) or not eg_nid.strip():
        raise ValueError("Input 'eg_nid' must be a non-empty string.")
    if not isinstance(reference_date, str) or not reference_date.strip() or len(reference_date) != 10:
        raise ValueError("Input 'reference_date' must be a non-empty string of length 10 (YYYY-MM-DD).")

    try:
        # 1. Determine reference_date (MongoDB connection is now passed in)
        current_reference_date_str = datetime.datetime.strptime(reference_date, "%Y-%m-%d")

        # 2. Find the latest previous document for this analysis_type and eg_nid
        previous_doc_query = {
            "analysis_type": analysis_type,
            "eg_nid": eg_nid  # MODIFIED: Added eg_nid to the query
        }
        previous_doc = collection.find_one(
            previous_doc_query,
            sort=[("reference_date", DESCENDING)]
        )

        previous_reference_date_str: Optional[str] = None
        previous_cohort_positions: Dict[str, int] = {}
        is_first_document_for_this_key = True # Flag to check if this is the first document for this analysis_type and eg_nid

        if previous_doc:
            is_first_document_for_this_key = False # A previous document exists
            previous_reference_date_str = previous_doc.get("reference_date")
            if "result" in previous_doc and isinstance(previous_doc["result"], list):
                for index, cohort_data in enumerate(previous_doc["result"]):
                    if isinstance(cohort_data, dict) and "cohort_name" in cohort_data:
                        previous_cohort_positions[cohort_data["cohort_name"]] = index
        
        # 3. Transform DataFrame rows and calculate cohort_trend
        result_list: List[Dict[str, Any]] = []
        data_rows = df.to_dicts() # Convert DataFrame to list of dictionaries

        for current_index, row_data in enumerate(data_rows):
            current_cohort_name = row_data['cohort_name']
            current_value = row_data['value']

            if is_first_document_for_this_key:
                # MODIFIED: If it's the first document for this analysis_type & eg_nid, trend is neutral.
                cohort_trend = "neutral"
            else: 
                # Not the first document, apply existing trend logic
                # Determine cohort_trend based on new logic
                if current_cohort_name in previous_cohort_positions:
                    previous_index = previous_cohort_positions[current_cohort_name]
                    if current_index < previous_index:
                        cohort_trend = "increasing"
                    elif current_index > previous_index:
                        cohort_trend = "decreasing"
                    else: # current_index == previous_index
                        cohort_trend = "neutral"
                else: # cohort_name not in previous_cohort_positions (it's a new cohort in the top list)
                    cohort_trend = "increasing" # new cohorts are now "increasing"
            
            result_list.append({
                "cohort_name": current_cohort_name,
                "value": current_value,
                "cohort_trend": cohort_trend
            })

        # 4. Construct the document to insert
        document_to_insert = {
            "analysis_type": analysis_type,
            "eg_nid": eg_nid,
            "reference_date": current_reference_date_str,
            "previous_reference_date": previous_reference_date_str,
            "result": result_list
        }

        # 5. Insert the document
        insert_result = collection.insert_one(document_to_insert)
        
        print(f"Successfully inserted document with ID: {insert_result.inserted_id} for analysis type '{analysis_type}' and eg_nid '{eg_nid}'.")
        return insert_result.inserted_id

    except Exception as e:
        print(f"An error occurred during insertion: {e}")
        return None
    # No 'finally client.close()' here, as client is managed externally

