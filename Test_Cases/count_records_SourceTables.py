import os
import pandas as pd
from datetime import datetime
from Utilities.Source_Target_DB_conn import MYSQL_DB_Conn
from Utilities.logging import Logs

# Ensure the Output_Result folder exists
output_folder = r"C:\Users\Sangram\PycharmProjects\Configdrivenapproach\Output_Result"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Initialize logger with timestamped log file
dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_path = f'C:\\Users\\Sangram\\PycharmProjects\\Configdrivenapproach\\Logs\\Sourcecountcheck_dim_{dt}.log'
logger = Logs.Log_Gen(log_path)

# Initialize database connection
source_db_conn = MYSQL_DB_Conn()


# Function to get all table names from MySQL DB
def get_table_names(source_db_conn):
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'source';"  # Replace with your schema
    tables_df = pd.read_sql(query, source_db_conn)

    # Debug: Print the columns and first few rows of the dataframe
    logger.info(f"Columns in the tables dataframe: {tables_df.columns}")
    logger.info(f"First few rows of tables dataframe: {tables_df.head()}")

    # Handle potential column name variations
    if 'table_name' in tables_df.columns:
        return tables_df['table_name'].tolist()
    elif 'TABLE_NAME' in tables_df.columns:  # MySQL might return the column in uppercase
        return tables_df['TABLE_NAME'].tolist()
    else:
        logger.error("Column 'table_name' or 'TABLE_NAME' not found in the query result.")
        raise ValueError("The query did not return a valid 'table_name' column.")


# Function to get row count for each table
def get_record_counts(source_db_conn, tables):
    record_counts = {}
    for table in tables:
        query = f"SELECT COUNT(*) FROM {table}"
        count_df = pd.read_sql(query, source_db_conn)
        record_counts[table] = count_df.iloc[0, 0]  # Extract the count from the dataframe
    return record_counts


# Function to write the table existence and record count to Excel
def write_to_excel(record_counts):
    dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = os.path.join(output_folder, f"SourceCount_check_validation_{dt}.xlsx")

    # Convert the dictionary to a DataFrame
    record_counts_df = pd.DataFrame(record_counts.items(), columns=['Table Name', 'Record Count'])

    # Write to Excel using XlsxWriter engine
    try:
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            record_counts_df.to_excel(writer, sheet_name="Table Record Counts", index=False)

        # Log success
        logger.info(f"Table records count results have been written to {output_path}")
    except Exception as e:
        logger.error(f"Error writing to Excel: {e}")
        raise


# Main function to execute the logic
def main():
    try:
        # Get table names from MySQL DB
        tables = get_table_names(source_db_conn)

        # Get record counts for each table
        record_counts = get_record_counts(source_db_conn, tables)

        # Write results to Excel
        write_to_excel(record_counts)
    except Exception as e:
        # Log any error during the main process
        logger.error(f"An error occurred during the execution: {e}")


# Execute the script
if __name__ == "__main__":
    main()
