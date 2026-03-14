from src.db.list_new_staging_files import list_new_staging_files
from src.db.load_one_staging_file import load_one_staging_file


if __name__ == "__main__":
    new_files = list_new_staging_files()

    if not new_files:
        print("No new staging parquet files found.")
    else:
        parquet_key = new_files[0]
        print(f"Loading: {parquet_key}")
        load_one_staging_file(parquet_key)
        print("Loaded successfully.")
