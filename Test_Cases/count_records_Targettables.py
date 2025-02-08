import pandas as pd

from datetime import datetime
from Utilities.Source_Target_DB_conn import MYSQL_DB_Conn, Oracle_DB_Conn
from Utilities.logging import Logs

dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logger = Logs.Log_Gen(f'C:\\Users\\Sangram\\PycharmProjects\\Configdrivenapproach\\Logs\\Targetcountcheck_dim_{dt}.log')

source_db_conn = MYSQL_DB_Conn()
target_db_conn = Oracle_DB_Conn()

# Function to get all table names for Oracle DB
def get_table_names(target_db_conn):
    query = "SELECT table_name FROM user_tables"  # Change to user_tables for Oracle
    tables_df = pd.read_sql(query, target_db_conn)
    return tables_df['table_name'].tolist()


# Function to get row count for each table
def get_record_counts(target_db_conn, tables):
    record_counts = {}
    for table in tables:
        query = f"SELECT COUNT(*) FROM {table}"
        count_df = pd.read_sql(query, target_db_conn)
        record_counts[table] = count_df.iloc[0, 0]  # Extract the count from the dataframe
    return record_counts


# Function to write the table existence and record count to Excel
def write_to_excel(record_counts):
    dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = f"C:\\Users\\Sangram\\PycharmProjects\\Configdrivenapproach\\Output_Result\\Target_Count_check_validation_{dt}.xlsx"

    # Convert the dictionary to a DataFrame
    record_counts_df = pd.DataFrame(record_counts.items(), columns=['Table Name', 'Record Count'])

    # Write to Excel
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        record_counts_df.to_excel(writer, sheet_name="Target Table Existence", index=False)

    logger.info(f"Target All Table Records Count results have been written to {output_path}")


def main():
    try:
        # Get table names from Oracle DB
        tables = get_table_names(target_db_conn)

        # Get record counts for each table
        record_counts = get_record_counts(target_db_conn, tables)

        # Write results to Excel
        write_to_excel(record_counts)
    except Exception as e:
        # Log any error during the main process
        logger.error(f"An error occurred during the execution: {e}")


# Execute the script
if __name__ == "__main__":
    main()


