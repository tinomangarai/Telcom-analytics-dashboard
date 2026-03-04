import pandas as pd
from sqlalchemy import create_engine
import os

# -------------------------------
# SQL Server connection (Windows Auth)
# -------------------------------
server = '.'
database = 'TelcoDB'
driver = 'ODBC+Driver+17+for+SQL+Server'

connection_string = f"mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection=yes"
engine = create_engine(connection_string, fast_executemany=True)

# -------------------------------
# ETL function to load CSVs
# -------------------------------
def load_csv_to_sql(file_path, table_name):
    print(f"🔄 Loading {file_path} into [{table_name}]...")
    try:
        df = pd.read_csv(file_path)

        # Drop identity/PK columns
        identity_columns = {
            'Customers': 'customer_id',
            'Billing': 'bill_id',
            'Usage': 'usage_id',
            'Complaints': 'complaint_id'
        }

        if table_name in identity_columns:
            id_col = identity_columns[table_name]
            if id_col in df.columns:
                df.drop(columns=[id_col], inplace=True)

        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"✅ Successfully loaded into [{table_name}]")
    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")

# -------------------------------
# Main ETL runner
# -------------------------------
def run_etl():
    folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'daily_csvs'))

    if not os.path.exists(folder):
        raise FileNotFoundError(f"❌ Folder not found: {folder}")

    files = os.listdir(folder)

    # Load customers first
    for f in files:
        if 'customers' in f.lower():
            load_csv_to_sql(os.path.join(folder, f), 'Customers')

    # Load daily transactional data
    for f in files:
        path = os.path.join(folder, f)
        if 'usage' in f.lower():
            load_csv_to_sql(path, 'Usage')
        elif 'billing' in f.lower():
            load_csv_to_sql(path, 'Billing')
        elif 'complaints' in f.lower():
            load_csv_to_sql(path, 'Complaints')

# -------------------------------
# Entry point
# -------------------------------
if __name__ == '__main__':
    run_etl()
