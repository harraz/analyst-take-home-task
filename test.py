import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as Func

def read_csv_to_pd(file_url: str) -> pd.DataFrame:
    """
    Reads CSV data from a given URL into a pandas DataFrame.
    
    Parameters:
        file_url (str): The URL pointing to the CSV file.
    
    Returns:
        pd.DataFrame: Loaded pandas DataFrame.
    """
    print(f"Downloading and reading CSV from: {file_url}")
    pd_df = pd.read_csv(file_url)
    # print("Pandas DataFrame head:")
    # print(pd_df.head())
    return pd_df

def convert_pd_to_spark(spark: SparkSession, pd_df: pd.DataFrame):
    """
    Converts a pandas DataFrame to a Spark DataFrame.
    
    Parameters:
        spark (SparkSession): The active Spark session.
        pd_df (pd.DataFrame): A pandas DataFrame.
    
    Returns:
        pyspark.sql.dataframe.DataFrame: The converted Spark DataFrame.
    """
    spark_df = spark.createDataFrame(pd_df)
    return spark_df

def process_file(spark: SparkSession, file_name: str, base_url: str):
    """
    Processes a CSV file by reading it into a pandas DataFrame from a URL,
    converting it into a Spark DataFrame, and printing the first few rows.
    
    Parameters:
        spark (SparkSession): The active Spark session.
        file_name (str): The CSV file name.
        base_url (str): The base URL where the CSV files are located.
    
    Returns:
        tuple: A tuple containing the pandas DataFrame and the Spark DataFrame.
    """
    file_url = f"{base_url}{file_name}"
    print(f"\nProcessing file: {file_name}")
    
    # Read into a pandas DataFrame
    pd_df = read_csv_to_pd(file_url)
    
    # Convert pandas DataFrame to Spark DataFrame
    spark_df = convert_pd_to_spark(spark, pd_df)
    
    # Display the head of the Spark DataFrame
    # print("Spark DataFrame head:")
    # spark_df.show(5)
    
    return pd_df, spark_df

def main():
    # Create a Spark session
    spark = SparkSession.builder.appName("ModularCSVLoader").getOrCreate()
    
    # Base URL for all the CSV files
    base_url = "https://raw.githubusercontent.com/chop-analytics/analyst-take-home-task/master/datasets/"
    
    # List of file names to process
    file_names = [
        "allergies.csv",
        "encounters.csv",
        "medications.csv",
        "patients.csv",
        "procedures.csv"
    ]
    
    # Dictionaries to store the resulting DataFrames for further processing/joining.
    # pandas_dfs = {}
    spark_dfs = {}
    
    # Process each file and store the DataFrames into the dictionaries
    for file_name in file_names:
        name_key = file_name.replace('.csv', '')
        pd_df, spark_df = process_file(spark, file_name, base_url)
        # pandas_dfs[name_key] = pd_df
        spark_dfs[name_key] = spark_df
    

    # filter encounters for Drug Overdose
    # enc_df = spark_dfs['encounters'].filter(spark_dfs['encounters'].REASONCODE == '55680006')

    # filter encounters for Drug Overdose and STOPDATE > 1999-07-15
    # date formate 2018-12-20 22:57:39
    enc_df = spark_dfs['encounters'].filter(
        (spark_dfs['encounters'].REASONCODE == '55680006') &
        (
            Func.to_timestamp(spark_dfs['encounters'].STOP, "yyyy-MM-dd HH:mm:ss") >
            Func.lit("1999-07-15 00:00:00")
        )
    )

    # filter patients not null and BIRTHDATE > current date
    # date format 2018-12-20 22:57:39
    patient_df = spark_dfs['patients'].filter((spark_dfs['patients'].BIRTHDATE.isNotNull()) &
        (
            Func.to_timestamp(spark_dfs['patients'].DEATHDATE, "yyyy-MM-dd HH:mm:ss") < Func.current_timestamp()
        ) &
        (
            Func.floor(
                Func.datediff(
                    Func.current_date(),
                    Func.to_date(spark_dfs['patients'].BIRTHDATE, "yyyy-MM-dd HH:mm:ss")) / 365
            ).between(18, 35)
        )
    )

    patient_df.show(5)

    joined_df = patient_df.join(enc_df, enc_df.PATIENT == patient_df.Id, "inner") \
        .select(enc_df.PATIENT, enc_df.REASONCODE, enc_df.STOP, patient_df.BIRTHDATE)
    
    print("Joined DataFrame:")
    joined_df.show(5)

    # Stop the Spark session when done
    spark.stop()

if __name__ == "__main__":
    main()
