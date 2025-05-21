import pandas as pd

# Load all datasets
fact_bookings = pd.read_csv("fact_bookings.csv")
fact_agg_bookings = pd.read_csv("fact_aggregated_bookings.csv")
dim_rooms = pd.read_csv("dim_rooms.csv")
dim_hotels = pd.read_csv("dim_hotels.csv")
dim_date = pd.read_csv("dim_date.csv")

# Standardize column names: lowercase and strip whitespace
def clean_columns(df):
    df.columns = df.columns.str.strip().str.lower()
    return df

fact_bookings = clean_columns(fact_bookings)
fact_agg_bookings = clean_columns(fact_agg_bookings)
dim_rooms = clean_columns(dim_rooms)
dim_hotels = clean_columns(dim_hotels)
dim_date = clean_columns(dim_date)

# Convert date columns to datetime
fact_bookings["check_in_date"] = pd.to_datetime(fact_bookings["check_in_date"], errors="coerce")
fact_bookings["booking_date"] = pd.to_datetime(fact_bookings["booking_date"], errors="coerce")
fact_bookings["checkout_date"] = pd.to_datetime(fact_bookings["checkout_date"], errors="coerce")
fact_agg_bookings["check_in_date"] = pd.to_datetime(fact_agg_bookings["check_in_date"], errors="coerce")
dim_date["date"] = pd.to_datetime(dim_date["date"], errors="coerce")

# Merge: fact_bookings + dim_rooms on room_category = room_class
merged = pd.merge(fact_bookings, dim_rooms, left_on="room_category", right_on="room_class", how="left")

# Merge: + dim_hotels on property_id
merged = pd.merge(merged, dim_hotels, on="property_id", how="left")

# Merge: + dim_date on check_in_date = date
merged = pd.merge(merged, dim_date, left_on="check_in_date", right_on="date", how="left")

# Merge: + fact_aggregated_bookings on property_id, room_category, check_in_date
merged = pd.merge(
    merged,
    fact_agg_bookings,
    on=["property_id", "room_category", "check_in_date"],
    how="left"
)

# Drop redundant columns if any
merged.drop(columns=["room_class", "date"], inplace=True, errors="ignore")

# Save the final master sheet
merged.to_csv("master_sheet.csv", index=False)

print("✅ Master sheet created successfully as 'master_sheet.csv'")
