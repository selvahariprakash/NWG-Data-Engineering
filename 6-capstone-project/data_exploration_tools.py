# Import Libraries
import pandas as pd
import missingno as msno
import matplotlib.pyplot as plt
pd.set_option('display.width',170, 'display.max_rows',200, 'display.max_columns',900)

#Function to print dataframe information
def print_dataframe_info(df):
    """
    This function prints a summary of important information about a pandas DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame to analyze and summarize.

    Functionality:
    1. Prints general information about the DataFrame, such as the number of rows, columns, column headers, and data types.
    2. Displays information about missing values in each column.
    3. Reports the number of duplicated rows in the DataFrame.
    4. Shows the number of unique values in each column and prints the first five unique values for inspection.
    5. Visualizes the distribution of missing data using the Missingno matrix.

    Output:
    The function does not return any value. It prints the summary directly to the console.
    """

    # Print an empty line for formatting
    print("\n")
    
    # Print general DataFrame information
    print("Dataframe Information", "\n")
    print("Number of Rows:", df.shape[0])  # Print the number of rows in the DataFrame
    print("Number of Columns:", df.shape[1], "\n")  # Print the number of columns in the DataFrame
    
    # Print the column headers
    print("Column Headers:", list(df.columns.values), "\n")
    
    # Print the data types of each column
    print("Data Types:")
    print(df.dtypes)
    
    # Print an empty line for formatting
    print("\n")
    
    # Print missing values information
    print("Missing Values Information", "\n")
    for columns in df:
        values = pd.isnull(df[columns]).sum()  # Count missing values in each column
        print(f"Column {columns} has {values} missing values.")

    # Print an empty line for formatting
    print("\n")
    
    # Print duplicated rows information
    print("Duplicated Rows Information", "\n")
    duplicate_count = df.duplicated().sum()  # Count duplicate rows in the DataFrame
    print(f"Number of duplicate rows: {duplicate_count}")
        
    # Print an empty line for formatting
    print("\n")
    
    # Print unique values information for each column
    print("Unique Values Information", "\n")
    for columns in df:
        values = df[columns].unique()  # Get unique values in the column
        print(f"Column {columns} has {values.size} unique values.")  # Print the number of unique values
        print(values[0:5], "\n")  # Print the first five unique values for inspection
        
    # Print an empty line for formatting
    print("\n", "Missingno Matrix")
    
    # Display a Missingno matrix for visualizing missing data distribution
    msno.matrix(df, figsize=(10, 6), fontsize=12)
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=90)
    
    # Show the plot
    plt.show()


#Function to drop fields with missing data more than 90%
def drop_missing_fields(raw_df, df_name, threshold=0.9):
    """
    Drops columns from the DataFrame that have more than the specified threshold of missing values.
    
    Parameters:
    - raw_df: The DataFrame to be processed.
    - df_name: A string representing the name of the DataFrame, used for print statements.
    - threshold: The proportion of missing values required to drop the column. Default is 0.9 (90%).

    Returns:
    - clean_df: A DataFrame with the columns dropped if they have more than the threshold of missing values.
    """
    print(f'-- Dataframe Cleaning --', '\n')
    print('Creating a copy of raw dataframe to begin with.')
    clean_df = raw_df.copy()
    
    print(f"{df_name} has {clean_df.shape[1]} fields prior to cleaning.")
    print(f"{df_name} has {clean_df.shape[0]} rows prior to cleaning.")
    print(f"Checking for fields in {df_name} that have more than {threshold*100}% of missing data and will be dropped.")
    
    # Calculate the proportion of missing values for each column
    missing_proportion = clean_df.isnull().mean()
    
    # Identify columns that have more missing values than the threshold
    columns_to_drop = missing_proportion[missing_proportion > threshold]
    
    # Print the columns to be dropped along with the percentage of missing data
    for column, proportion in columns_to_drop.items():
        print(f"Dropping column '{column}' with {proportion:.2%} missing data")
    
    # Drop those columns from the DataFrame
    clean_df.drop(columns=columns_to_drop.index, inplace=True)
    clean_df.dropna(how='all', inplace = True)
    
    print('Dropping missing fields complete.')
    print(f"{df_name} now has {clean_df.shape[1]} fields after cleaning.")
    print(f"{df_name} now has {clean_df.shape[0]} rows after cleaning.", '\n')
    
    return clean_df

def count_parquet_files(spark, file_paths):
    """
    This function takes a Spark session and a dictionary of file paths, reads the Parquet files into Spark DataFrames,
    and returns a dictionary with the count of records for each DataFrame.

    Arguments:
    spark : SparkSession : Spark session object to read the files.
    file_paths : dict : Dictionary where keys are descriptive names (e.g., 'dim_demographics')
                        and values are the corresponding file paths.

    Returns:
    dict : Dictionary with the count of records for each DataFrame.
    """
    counts = {}
    
    for name, path in file_paths.items():
        df = spark.read.parquet(path)
        record_count = df.count()
        counts[name] = record_count
    
    return counts

def count_parquet_files(spark, file_paths):
    """
    This function takes a Spark session and a dictionary of file paths, reads the Parquet files into Spark DataFrames,
    and returns a dictionary with the count of records for each DataFrame.

    Arguments:
    spark : SparkSession : Spark session object to read the files.
    file_paths : dict : Dictionary where keys are descriptive names (e.g., 'dim_demographics')
                        and values are the corresponding file paths.

    Returns:
    dict : Dictionary with the count of records for each DataFrame.
    """
    counts = {}
    
    for name, path in file_paths.items():
        df = spark.read.parquet(path)
        record_count = df.count()
        df.printSchema()
        counts[name] = record_count
    
    return counts

def null_value_check(df, column_names):
    """
    Perform null value check on the specified columns in the DataFrame.

    Args:
    df : pyspark.sql.DataFrame : Spark DataFrame to check
    column_names : list : List of column names to check for null values

    Returns:
    None
    """
    for column in column_names:
        null_count = df.filter(F.col(column).isNull()).count()
        if null_count > 0:
            print(f"Warning: Column '{column}' contains {null_count} null values!")
        else:
            print(f"Column '{column}' has no null values.")