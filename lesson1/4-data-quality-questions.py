import polars as pl
from cuallee import Check, CheckLevel

# Read CSV file into Polars DataFrame
df = pl.read_csv("./data/sample_data.csv")

# Question: Check for Nulls on column Id and that Customer_ID column is unique
# check docs at https://canimus.github.io/cuallee/polars/ on how to define a check and run it.
# you will end up with a dataframe of results, check that the `status` column does not have any "FAIL" in it

import polars as pl
from cuallee import Check, CheckLevel, run_checks

# Read CSV file into Polars DataFrame
df = pl.read_csv("./data/sample_data.csv")

# Define checks using cuallee
checks = [
    Check(
        name="Check for Nulls in Id",
        level=CheckLevel.ERROR,
        condition=df['Id'].is_null(),
        message="Id column should not contain null values."
    ),
    Check(
        name="Check for Unique Customer_ID",
        level=CheckLevel.ERROR,
        condition=df['Customer_ID'].is_unique(),
        message="Customer_ID column should be unique."
    )
]

# Run checks
results = run_checks(df, checks)

# Filter results to ensure no "FAIL" in status column
filtered_results = results.filter(pl.col("status") != "FAIL")

# Print results summary
print("Results Summary:")
print(filtered_results)

# Optionally, iterate over detailed results
for result in filtered_results.iterrows():
    print(f"{result['name']}: {result['status']}")


