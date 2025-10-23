# ingest_data.py
import os
import sys
from sodapy import Socrata
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import certifi

# --- Configuration ---
# Socrata API details for KC 311 data
SOCRATA_DOMAIN = "data.kcmo.org"
SOCRATA_DATASET_ID = "d4px-6rwg"
# The number of records to fetch in a single API call
SOCRATA_LIMIT = 10000

# MongoDB Atlas configuration
# The connection string is retrieved from an environment variable for security
MONGO_CONNECTION_STRING = os.environ.get("MONGO_CONNECTION_STRING")
DB_NAME = "kc_311_db"
COLLECTION_NAME = "raw_requests"


def ingest_data():
    """
    Connects to the Socrata API to fetch 311 data and loads it into
    a 'raw' collection in MongoDB, performing a full refresh.
    """
    if not MONGO_CONNECTION_STRING:
        print("ERROR: MONGO_CONNECTION_STRING environment variable not set.", file=sys.stderr)
        sys.exit(1)

    mongo_client = None  # Initialize client to None for the finally block
    try:
        # --- 1. EXTRACT: Fetch data from Socrata API ---
        print("Fetching data from Socrata API...")
        socrata_client = Socrata(SOCRATA_DOMAIN, None)
        results = socrata_client.get(SOCRATA_DATASET_ID, limit=SOCRATA_LIMIT)

        if not results:
            print("No data fetched from API. Exiting.")
            return

        print(f"Successfully fetched {len(results)} records.")

        # --- 2. LOAD: Connect to MongoDB and insert data ---
        print("Connecting to MongoDB Atlas...")
        mongo_client = MongoClient(MONGO_CONNECTION_STRING, tlsCAFile=certifi.where())
        # The ismaster command is cheap and does not require auth. It's a good way to verify the connection.
        mongo_client.admin.command('ismaster')
        print("MongoDB connection successful.")

        db = mongo_client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Perform a "full refresh" by deleting old data
        print(f"Deleting old data from '{COLLECTION_NAME}' collection...")
        delete_result = collection.delete_many({})
        print(f"Deleted {delete_result.deleted_count} records.")

        # Insert the new data
        print(f"Inserting {len(results)} new records...")
        collection.insert_many(results)
        print("Data insertion complete.")

    except ConnectionFailure as e:
        print(f"ERROR: Could not connect to MongoDB: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        # Ensure the client connection is always closed
        if mongo_client:
            mongo_client.close()
            print("MongoDB connection closed.")

if __name__ == "__main__":
    ingest_data()
    print("\nIngestion script finished successfully.")