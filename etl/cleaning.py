import pandas as pd


def make_age_group(age):
    if pd.isna(age):
        return "Unknown"
    elif age < 18:
        return "Under 18"
    elif age <= 29:
        return "18-29"
    elif age <= 44:
        return "30-44"
    elif age <= 59:
        return "45-59"
    else:
        return "60 and above"


def main():
    # -----------------------------
    # 1. Load raw datasets
    # -----------------------------
    airline_path = "data/raw/Airline Dataset Updated - v2.csv"
    airport_path = "data/raw/airports.csv"

    airline_df = pd.read_csv(airline_path)
    airport_df = pd.read_csv(airport_path)

    print("Airline dataset shape:", airline_df.shape)
    print("Airline columns:")
    print(airline_df.columns.tolist())
    print()

    print("Airports dataset shape:", airport_df.shape)
    print("Airports columns:")
    print(airport_df.columns.tolist())
    print()

    # -----------------------------
    # 2. Rename incorrect column
    # -----------------------------
    airline_df = airline_df.rename(
        columns={"Arrival Airport": "Departure Airport Code"}
    )

    # -----------------------------
    # 3. Drop redundant column
    # -----------------------------
    if "Continents" in airline_df.columns:
        airline_df = airline_df.drop(columns=["Continents"])

    # -----------------------------
    # 4. Convert date
    # -----------------------------
  
    # Step 1: Convert column to string to ensure uniform processing
    airline_df["Departure Date"] = airline_df["Departure Date"].astype(str)

    # Step 2: Standardise date separators by replacing '-' with '/'
    # This ensures all dates follow the same format (e.g., 12/05/2025)
    airline_df["Departure Date"] = airline_df["Departure Date"].str.replace("-", "/", regex=False)

    # Step 3: Convert to datetime format
    # 'dayfirst=True' is important because the dataset follows dd/mm/yyyy format
    # 'errors="coerce"' will convert invalid formats to NaT for later inspection
    airline_df["Departure Date"] = pd.to_datetime(
        airline_df["Departure Date"], errors="coerce", dayfirst=True
    )

    airline_df["Year"] = airline_df["Departure Date"].dt.year
    airline_df["Month"] = airline_df["Departure Date"].dt.month
    airline_df["Quarter"] = airline_df["Departure Date"].dt.quarter

    # -----------------------------
    # 5. Create age group
    # -----------------------------
    airline_df["Age Group"] = airline_df["Age"].apply(make_age_group)

    # -----------------------------
    # 6. Check missing values
    # -----------------------------
    print("Missing values in airline dataset:")
    print(airline_df.isna().sum())
    print()

    print("Duplicate rows in airline dataset:", airline_df.duplicated().sum())
    print()

   # -----------------------------
    # Airports dataset cleaning
    # -----------------------------

    # Step 1: Select relevant columns only
    # Exclude 'continent' due to high proportion of missing values
    airport_keep_cols = [
        "name",
        "iata_code",
        "iso_country",
        "iso_region",
        "municipality",
        "type",
        "scheduled_service",
        "latitude_deg",
        "longitude_deg",
    ]

    airport_df = airport_df[airport_keep_cols].copy()


    # Step 2: Remove rows without IATA code
    # IATA code is required for integration with airline dataset
    airport_df = airport_df[airport_df["iata_code"].notna()]


    # Step 3: Check missing values for key attributes
    print("Missing values in cleaned airports dataset:")
    print(airport_df.isna().sum())
    print()


    # Step 4: Check municipality missing values separately
    missing_municipality = airport_df["municipality"].isna().sum()
    total_rows = len(airport_df)

    print("Missing municipality:", missing_municipality)
    print("Total rows:", total_rows)
    print("Percentage:", (missing_municipality / total_rows) * 100)
    print()

    # -----------------------------
    # 8. Save cleaned datasets
    # -----------------------------
    airline_output = "data/processed/airline_cleaned.csv"
    airport_output = "data/processed/airports_cleaned.csv"

    airline_df.to_csv(airline_output, index=False)
    airport_df.to_csv(airport_output, index=False)

    print("Saved:", airline_output)
    print("Saved:", airport_output)

if __name__ == "__main__":
    main()