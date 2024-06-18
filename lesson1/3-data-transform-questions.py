print(
    "################################################################################"
)
print("Use standard python libraries to do the transformations")
print(
    "################################################################################"
)

# Question: How do you read data from a CSV file at ./data/sample_data.csv into a list of dictionaries?
import pandas as pd

df=pd.read_csv('filepath')

data_list = df.to_dict(orient='records')

print(data_list)






# Question: How do you remove duplicate rows based on customer ID?
# Initialize an empty set to track seen CustomerIDs
seen_ids = set()

# Initialize an empty list to store unique dictionaries
unique_data_list = []

# Iterate through each dictionary in data_list
for d in data_list:
    customer_id = d['CustomerID']
    # Check if CustomerID is already seen
    if customer_id not in seen_ids:
        # If not seen, add it to the set and append the dictionary to unique_data_list
        seen_ids.add(customer_id)
        unique_data_list.append(d)

# unique_data_list now contains only the unique dictionaries based on CustomerID
print(unique_data_list)



# Question: How do you handle missing values by replacing them with 0?

# Replace missing values with 0
df.fillna(0, inplace=True)


# Question: How do you remove outliers such as age > 100 or purchase amount > 1000?
df_new=df[(df['age']<100) & (df['purchase amount']<1000)]

# Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male)?
gender_mapping = {'Female': 0, 'Male': 1}

# Apply the mapping to the Gender column
df['Gender'] = df['Gender'].map(gender_mapping)

# Print the updated DataFrame
print(df)

# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns?

df['first_name','last_name']=df['Customer_Name'].str.split(' ',expand=True)

df.drop(columns=['Customer_Name'], inplace=True)

# Question: How do you calculate the total purchase amount by Gender?
data_purchase=df.groupby('gender')('purchase').sum()

# Question: How do you calculate the average purchase amount by Age group?
# assume age_groups is the grouping we want
# hint: Why do we convert to float?

# Example dataset (you would have your actual data here)
purchases = [
    {"age": 25, "amount": 50},
    {"age": 35, "amount": 75},
    {"age": 45, "amount": 60},
    {"age": 55, "amount": 90},
    {"age": 65, "amount": 80},
    {"age": 28, "amount": 55},
    {"age": 37, "amount": 70},
    {"age": 47, "amount": 65},
    {"age": 57, "amount": 85},
    {"age": 67, "amount": 95}
]

# Initialize age groups with empty lists
age_groups = {"18-30": [], "31-40": [], "41-50": [], "51-60": [], "61-70": []}

# Classify purchases into age groups
for purchase in purchases:
    age = purchase["age"]
    amount = purchase["amount"]
    
    if 18 <= age <= 30:
        age_groups["18-30"].append(amount)
    elif 31 <= age <= 40:
        age_groups["31-40"].append(amount)
    elif 41 <= age <= 50:
        age_groups["41-50"].append(amount)
    elif 51 <= age <= 60:
        age_groups["51-60"].append(amount)
    elif 61 <= age <= 70:
        age_groups["61-70"].append(amount)

# Calculate average purchase amount for each age group
average_amounts = {}
for age_group, amounts in age_groups.items():
    if amounts:  # Ensure there are purchases in the age group
        average_amount = sum(amounts) / len(amounts)
        average_amounts[age_group] = average_amount
    else:
        average_amounts[age_group] = 0  # or handle as needed if no purchases in the group

# Display the results
for age_group, avg_amount in average_amounts.items():
    print(f"Average purchase amount for {age_group}: ${avg_amount:.2f}")

age_groups = {"18-30": [], "31-40": [], "41-50": [], "51-60": [], "61-70": []}

# Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group?
your_total_purchase_amount_by_gender = {} # your results should be assigned to this variable


average_purchase_by_age_group = {} # your results should be assigned to this variable

print(f"Total purchase amount by Gender: {your_total_purchase_amount_by_gender}")
print(f"Average purchase amount by Age group: {average_purchase_by_age_group}")

print(
    "################################################################################"
)
print("Use DuckDB to do the transformations")
print(
    "################################################################################"
)

# Question: How do you connect to DuckDB and load data from a CSV file into a DuckDB table?
# Connect to DuckDB and load data

import duckdb
import pandas as pd

# Connect to DuckDB
conn = duckdb.connect(database=':memory:', read_only=False)

# Example CSV file path
csv_file = 'path_to_your_file.csv'

# Load CSV into a pandas DataFrame
df = pd.read_csv(csv_file)

# Insert DataFrame into DuckDB table
table_name = 'my_table'
df.to_sql(table_name, conn, index=False, if_exists='replace')

# Commit the transaction
conn.commit()

# Execute a query
cursor = conn.cursor()
cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")

# Fetch and print results
results = cursor.fetchall()
for row in results:
    print(row)

# Close the connection
conn.close()




# Read data from CSV file into DuckDB table

# Question: How do you remove duplicate rows based on customer ID in DuckDB?

Duckdb

# Question: How do you handle missing values by replacing them with 0 in DuckDB?

# UPDATE sales_data
# SET sales_amount = COALESCE(sales_amount, 0)
# WHERE sales_amount IS NULL;



# Question: How do you remove outliers (e.g., age > 100 or purchase amount > 1000) in DuckDB?

new_data=f"{Delete age,purchase
            from table
            where age < 100 and purchase < 100}"

# Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male) in DuckDB?
select case when gender='male' then 1 else o end as gender_new

# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns in DuckDB?
SELECT
    SUBSTRING(Customer_Name FROM 1 FOR POSITION(' ' IN Customer_Name) - 1) AS First_Name,
    SUBSTRING(Customer_Name FROM POSITION(' ' IN Customer_Name) + 1) AS Last_Name
FROM your_table;



# Question: How do you calculate the total purchase amount by Gender in DuckDB?
select sum(amount) as  total
from table
group by gender

# Question: How do you calculate the average purchase amount by Age group in DuckDB?
SELECT
    CASE
        WHEN age BETWEEN 18 AND 30 THEN '18-30'
        WHEN age BETWEEN 31 AND 40 THEN '31-40'
        WHEN age BETWEEN 41 AND 50 THEN '41-50'
        WHEN age BETWEEN 51 AND 60 THEN '51-60'
        WHEN age BETWEEN 61 AND 70 THEN '61-70'
        ELSE 'Unknown'
    END AS age_group,
    AVG(purchase_amount) AS avg_purchase_amount
FROM customer_transactions
GROUP BY age_group
ORDER BY age_group;


# Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group in DuckDB?
print("====================== Results ======================")
print("Total purchase amount by Gender:")
print("Average purchase amount by Age group:")
