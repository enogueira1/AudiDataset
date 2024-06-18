import pandas as pd
import os

def load_dataset(file_path):
    """Load dataset from a CSV file."""
    try:
        df = pd.read_csv(file_path, dtype=str, low_memory=False)
        return df
    except Exception as e:
        print(f"Error loading dataset: {e}")
        return None

def replace_na_and_empty(df):
    """Replace NaN values and empty strings with None."""
    try:
        df = df.where(pd.notna(df), None)
        df = df.applymap(lambda x: None if x == '' else x)
        return df
    except Exception as e:
        print(f"Error in replace_na_and_empty: {e}")
        return df

def drop_columns(df):
    """Drop unnecessary columns."""
    try:
        columns_to_drop = ['Address2', 'VehicleMake', 'PhoneLeadBestId', 'DataLastRefreshedDate',
                           'SaleDate', 'ZipCodeSuffix', 'DriverZipCodeSuffix', 'ReportedDate', 'Created',
                           'Modified']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
        return df
    except Exception as e:
        print(f"Error in drop_columns: {e}")
        return df

def proper_case_and_lowercase_emails(df):
    """Convert specified columns to proper case and emails to lowercase."""
    try:
        columns_to_proper_case = [
            'City', 'FirstName', 'LastName', 'Address1', 'SaleExteriorColor',
            'SaleInteriorColor', 'BodyType', 'OwnerCompanyName', 'DriverFirstName',
            'DriverLastName', 'DriverCompanyName', 'DriverAddress', 'DriverCity'
        ]
        for col in columns_to_proper_case:
            if col in df.columns:
                df[col] = df[col].str.title()

        if 'EmailAddress' in df.columns:
            df['EmailAddress'] = df['EmailAddress'].str.lower()

        return df
    except Exception as e:
        print(f"Error in proper_case_and_lowercase_emails: {e}")
        return df

def format_phone_numbers(df):
    """Format phone numbers and remove trailing '0000'."""
    try:
        phone_columns = ['PhoneNumber', 'DriverHomePhone', 'DriverBusinessPhone', 'BusinessPhone']
        for phone_col in phone_columns:
            if phone_col in df.columns:
                df[phone_col] = df[phone_col].str.replace(r'(\d{3}) (\d{3}-\d{4})', r'\1-\2', regex=True)

        phone_columns_with_zeros = ['DriverBusinessPhone', 'BusinessPhone']
        for phone_col in phone_columns_with_zeros:
            if phone_col in df.columns:
                df[phone_col] = df[phone_col].str.replace(r'\s0000$', '', regex=True)

        return df
    except Exception as e:
        print(f"Error in format_phone_numbers: {e}")
        return df

def replace_directional_abbreviations(df):
    """Replace directional abbreviations like 'Sw' with 'SW', 'Nw' with 'NW', etc., ensuring whole word replacements."""
    try:
        directional_map = {
            r'\bSw\b': 'SW', r'\bNw\b': 'NW', r'\bSe\b': 'SE', r'\bNe\b': 'NE'
        }
        columns_to_check = [
            'Address1', 'DriverAddress'
        ]
        for col in columns_to_check:
            if col in df.columns:
                for key, value in directional_map.items():
                    df[col] = df[col].str.replace(key, value, regex=True)
        return df
    except Exception as e:
        print(f"Error in replace_directional_abbreviations: {e}")
        return df

def clean_data(df):
    """Master function to clean the dataframe using smaller functions."""
    df = replace_na_and_empty(df)
    df = drop_columns(df)
    df = proper_case_and_lowercase_emails(df)
    df = format_phone_numbers(df)
    df = replace_directional_abbreviations(df)
    return df

def save_dataset(df, temp_file_path, final_file_path):
    """Save the cleaned dataset to a CSV file."""
    try:
        df.to_csv(temp_file_path, index=False, na_rep='NULL')
        os.rename(temp_file_path, final_file_path)
    except Exception as e:
        print(f"Error saving dataset: {e}")

def main():
    file_path = 'sale.csv'
    temp_file_path = 'sales_temp.csv'
    final_file_path = 'sales.csv'

    df = load_dataset(file_path)
    if df is not None:
        df = clean_data(df)
        save_dataset(df, temp_file_path, final_file_path)

if __name__ == "__main__":
    main()

