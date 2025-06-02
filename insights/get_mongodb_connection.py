import polars as pl
from pymongo import MongoClient, DESCENDING, collection as PyMongoCollection
from datetime import date
from typing import Optional, Tuple
import os
from dotenv import load_dotenv
from pprint import pprint

def get_mongo_collection_from_env() -> Tuple[Optional[PyMongoCollection.Collection], Optional[MongoClient]]:
    """
    Reads MongoDB connection parameters from a .env file, establishes a connection,
    and returns the MongoDB collection object and the client instance.

    Required .env variables:
        MONGO_URI (str): The MongoDB connection URI.
        DATABASE_NAME (str): The name of the MongoDB database.
        COLLECTION_NAME (str): The name of the MongoDB collection.

    Returns:
        Tuple[Optional[pymongo.collection.Collection], Optional[pymongo.MongoClient]]: 
        A tuple containing the collection object and the client instance, 
        or (None, None) if connection fails or variables are missing.
    """
    load_dotenv() # Load variables from .env file

    mongo_uri = os.environ.get("MONGO_URI")
    database_name = os.environ.get("DATABASE_NAME")
    collection_name = os.environ.get("COLLECTION_NAME")

    if not all([mongo_uri, database_name, collection_name]):
        print("Error: MONGO_URI, DATABASE_NAME, or COLLECTION_NAME not found in .env file.")
        return None, None

    client = None
    try:
        client = MongoClient(mongo_uri)
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster') # Verify connection
        db = client[database_name]
        collection_obj = db[collection_name]
        print(f"Successfully connected to MongoDB: {database_name}/{collection_name}")
        return collection_obj, client
    except Exception as e:
        print(f"Failed to connect to MongoDB or retrieve collection: {e}")
        if client:
            client.close()
        return None, None

if __name__ == "__main__":
    collection_obj, client = get_mongo_collection_from_env()
