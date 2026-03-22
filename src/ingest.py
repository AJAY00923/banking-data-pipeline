# import pandas as pd
# from transform import transform_data
# # file_path
# file_path = "data/raw/transactions.csv"

# # read data
# df = pd.read_csv(file_path)

# print("Data loaded Successfully")
# print("shape:", df.shape)

# df = transform_data(df)

# print("\nColumns:", df.columns)
# # select top 5 rows of the dataframe
# print("\nFirst 5 rows", df.head())
# # checking Null values
# print("\nNull values per Column:")
# print(df.isnull().sum())
# #checking duplicate values
# print("\nDuplicate Transaction IDs:")
# print(df["TransactionID"].duplicated().sum())
# #checking dataTypes
# print("\nData types:")
# print(df.dtypes)

import pandas as pd
from transform import transform_data

# file path
file_path = "data/raw/transactions.csv"

# read data
df = pd.read_csv(file_path)

print("Data loaded Successfully")
print("Shape:", df.shape)

# apply transformation
df = transform_data(df)

print("\nAfter Transformation:")
print(df.head())

output_path = "data/processed/transactions_processed.csv"
df.to_csv(output_path, index = False)

print("\nProcessed file saved successfully")
print("saved to :", output_path)