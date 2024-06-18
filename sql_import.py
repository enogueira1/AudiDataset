import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Define the connection parameters
conn_params = {
    'dbname': 'Audi',
    'user': 'postgres',
    'password': 'Password',
    'host': 'localhost',
    'port': '5432'
}

def clean_and_convert_data(df):
    """Convert data types and handle NaNs."""
    # Columns that should be integers
    integer_columns = ['SaleId', 'HouseholdId', 'VehicleYear', 'BestMatch', 'BestMatchTypeId', 'ReportedMonthNumber',
                       'TotalNetSales']

    for col in integer_columns:
        if col in df.columns:
            # Convert to numeric, coerce errors, and then to nullable Int64
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(pd.Int64Dtype())

    # Ensure DateKey is in datetime format
    if 'DateKey' in df.columns:
        df['DateKey'] = pd.to_datetime(df['DateKey'], errors='coerce')

    # Replace remaining NaN values with None for non-integer columns
    non_integer_columns = df.select_dtypes(exclude=['Int64']).columns
    df[non_integer_columns] = df[non_integer_columns].where(pd.notna(df[non_integer_columns]), None)

    return df


def replace_na_with_none(data):
    """Replace pd.NA with None for compatibility with psycopg2."""
    return [[None if isinstance(item, pd._libs.missing.NAType) else item for item in row] for row in data]


# Establish a connection to the database
try:
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    # SQL command to create the table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS audi (
        SaleId INTEGER PRIMARY KEY,
        HouseholdId INTEGER,
        VIN CHAR(17),
        DealerCode VARCHAR(7),
        FirstName VARCHAR(35),
        LastName VARCHAR(35),
        Address1 VARCHAR(50),
        City VARCHAR(35),
        State CHAR(3),
        ZipCode CHAR(6),
        PhoneNumber VARCHAR(12),
        EmailAddress VARCHAR(49),
        VehicleYear INTEGER,
        VehicleModel VARCHAR(20),
        VehicleTrim VARCHAR(15),
        VehicleModelCode VARCHAR(6),
        BestMatch INTEGER,
        BestMatchTypeId INTEGER,
        SalesType VARCHAR(2),
        SpecialProgramCode VARCHAR(9),
        ReportedMonthNumber INTEGER,
        SaleOptionList VARCHAR(87),
        SaleExteriorColor VARCHAR(49),
        SaleInteriorColor VARCHAR(54),
        TransmissionTypeCode CHAR(3),
        BodyType VARCHAR(21),
        OwnerCompanyName VARCHAR(87),
        DriverFirstName VARCHAR(35),
        DriverLastName VARCHAR(35),
        DriverCompanyName VARCHAR(87),
        DriverAddress VARCHAR(50),
        DriverCity VARCHAR(35),
        DriverState CHAR(3),
        DriverZipCode CHAR(7),
        DriverHomePhone VARCHAR(12),
        DriverBusinessPhone VARCHAR(12),
        OnlineLeadId VARCHAR(10),
        BusinessPhone VARCHAR(12),
        TotalNetSales INTEGER,
        DateKey DATE
    );
    """

    cur.execute(create_table_query)
    conn.commit()

    # Load the data from the CSV file into a pandas DataFrame
    csv_file_path = 'sales.csv'
    df = pd.read_csv(csv_file_path, dtype=str, low_memory=False)

    # Clean and convert data types
    df = clean_and_convert_data(df)

    # Print data type information for debugging
    print(df.dtypes)

    # Insert the DataFrame records one by one
    insert_query = """
    INSERT INTO audi (
        SaleId, HouseholdId, VIN, DealerCode, FirstName, LastName, Address1, City, State, ZipCode, 
        PhoneNumber, EmailAddress, VehicleYear, VehicleModel, VehicleTrim, VehicleModelCode, BestMatch, 
        BestMatchTypeId, SalesType, SpecialProgramCode, ReportedMonthNumber, SaleOptionList, 
        SaleExteriorColor, SaleInteriorColor, TransmissionTypeCode, BodyType, OwnerCompanyName, 
        DriverFirstName, DriverLastName, DriverCompanyName, DriverAddress, DriverCity, 
        DriverState, DriverZipCode, DriverHomePhone, DriverBusinessPhone, OnlineLeadId, 
        BusinessPhone, TotalNetSales, DateKey
    ) VALUES %s
    """

    # Create a list of tuples from the DataFrame values
    data_tuples = [tuple(x) for x in df.to_numpy()]

    # Replace pd.NA with None
    data_tuples = replace_na_with_none(data_tuples)

    # Print the first few rows to debug data
    print(data_tuples[:5])

    # Use psycopg2's execute_values to efficiently insert data
    execute_values(cur, insert_query, data_tuples)

    # Commit the transaction
    conn.commit()

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the connection
    if cur:
        cur.close()
    if conn:
        conn.close()
