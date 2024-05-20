# Databricks notebook source
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import lit
import logging

# COMMAND ----------

logging.basicConfig(level=logging.INFO)

# COMMAND ----------

schema = StructType([
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("DOB", StringType(), True)
])

# Sample data
data = [("John", "Doe", "1990-01-01"), ("Jane", "Smith", "1985-05-15")]

df_one = spark.createDataFrame(data,schema)

# COMMAND ----------

schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name_in_record_", StringType(), True),
    StructField("date_of_birth", StringType(), True)
])

# Sample data
data = [("Peter", "Parker", "2001-05-02"), ("Mary", "Jane", "2004-08-03")]

df_two = spark.createDataFrame(data,schema)

# COMMAND ----------

schema = StructType([
    StructField("name", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("birthYearMonthDate", StringType(), True),
])

# Sample data
data = [("Bruce", "Wayne", "1995-03-08"), ("Diana", "Prince", "1995-03-01")]

df_three = spark.createDataFrame(data,schema)

# COMMAND ----------

df_three

# COMMAND ----------

destination_df_schema = StructType(
    [
        StructField("First_Name",StringType(),True),
        StructField("Last_Name",StringType(),True),
        StructField("Date_Of_Birth",StringType(),True),
    ]
)

# COMMAND ----------

destination_df = spark.createDataFrame(data = [], schema = destination_df_schema)

# COMMAND ----------

column_name_mappings = {
    'First_Name' : ['FirstName','first_name','name'], #Exact matches in column name
    'Last_Name' : ['LastName','last_name_in_record_','Last_Name'], 
    'Date_Of_Birth' : ['DOB', 'date_of_birth', ('birth','Date')] #starting and ending match in column name
}

# COMMAND ----------

# def prefix_suffix_column_name_matching(column_names, prefix, suffix):
#     for column_name in column_names:
#         if column_name.startswith(prefix.lower()) and column_name.endswith(suffix.lower()):
#             logging.info(f"Matched column '{column_name}' with prefix '{prefix}' and suffix '{suffix}'.")
#             return column_name
#     logging.info(f"No match found with prefix '{prefix}' and suffix '{suffix}'.")
#     return None

# COMMAND ----------

def prefix_suffix_column_name_matching(column_names, prefix, suffix):
    matched_columns = []
    for column_name in column_names:
        if column_name.startswith(prefix.lower()) and column_name.endswith(suffix.lower()):
            matched_columns.append(column_name)
    
    if len(matched_columns) > 1:
        logging.warning(f"Multiple matches found for prefix '{prefix}' and suffix '{suffix}': {matched_columns}. Using the first match '{matched_columns[0]}'.")
    
    if matched_columns:
        logging.info(f"Matched column '{matched_columns[0]}' with prefix '{prefix}' and suffix '{suffix}'.")
        return matched_columns[0]
    
    logging.info(f"No match found with prefix '{prefix}' and suffix '{suffix}'.")
    return None

# COMMAND ----------

# Function to handle renaming or adding a column with conflict checks
def handle_column_renaming(df, column_name, new_column_name, mapped_column_names):
    if new_column_name in mapped_column_names:
        logging.warning(f"Conflict detected: Column '{new_column_name}' already exists. Skipping renaming of '{column_name}'.")
    else:
        logging.info(f"Renaming column '{column_name}' to '{new_column_name}'.")
        df = df.withColumnRenamed(column_name, new_column_name)
        mapped_column_names.add(new_column_name)
    return df, mapped_column_names

# COMMAND ----------

# Function to rename or add columns based on column name mappings
def rename_or_add_column_with_column_name_matching(df, column_name_mappings):
    column_names = [column_name.lower() for column_name in df.columns]
    mapped_column_names = set()
    new_df = df

    for new_column_name, old_column_names in column_name_mappings.items():
        logging.info(f"Processing new column name: {new_column_name}")
        is_column_mapped_flag = False
        for old_column_name in old_column_names:
            if isinstance(old_column_name, tuple):
                prefix, suffix = old_column_name
                column_match = prefix_suffix_column_name_matching(column_names, prefix, suffix)
                if column_match is not None:
                    new_df, mapped_column_names = handle_column_renaming(new_df, column_match, new_column_name, mapped_column_names)
                    is_column_mapped_flag = True
                    break
            elif isinstance(old_column_name, str) and old_column_name.lower() in column_names:
                logging.info(f"Exact match found for old column name '{old_column_name}'.")
                new_df,mapped_column_names  = handle_column_renaming(new_df, old_column_name, new_column_name, mapped_column_names)
                is_column_mapped_flag = True
                break
        if not is_column_mapped_flag:
            logging.info(f"No match found for '{new_column_name}'. Adding new column with null values.")
            new_df = new_df.withColumn(new_column_name, lit(None))
    new_df_columns = column_name_mappings.keys()
    new_df.select(*new_df_columns)
    return new_df

# COMMAND ----------

def harmonize_data(destination_df):
   dataframes = [df_one, df_two, df_three]
   for dataframe in dataframes:
      parsed_df = rename_or_add_column_with_column_name_matching(dataframe, column_name_mappings)
      destination_df = destination_df.union(parsed_df)
      display(destination_df)

   return destination_df

# COMMAND ----------

harmonized_df = harmonize_data(destination_df)

# COMMAND ----------

display(harmonized_df)

# COMMAND ----------


