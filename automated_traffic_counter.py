"""
This Spark script processes traffic data to analyse car counts over time. It reads data from a specified input file,
preprocesses it to extract relevant fields, and performs various aggregations to report:
- Total number of cars seen per dataset (file).
- Total number of cars seen per day.
- Top 3 intervals with the most cars seen, accounting for ties.
- The 1.5 hour period(s) with the least cars.

Usage:
    python automated_traffic_counter.py <file_path>

Where <file_path> is the path to the input data file.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import split, sum as _sum, to_date, min as _min, row_number as _row_number, col, rank
import argparse

def initialize_spark_session(app_name):
    """
    Initializes and returns a SparkSession with the given application name.

    :param app_name: Name of the Spark application.
    :return: A SparkSession object.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_and_preprocess_data(spark, file_path):
    """
    Reads raw traffic data from a text file and preprocesses it to extract timestamps and car counts.

    :param spark: SparkSession object.
    :param file_path: Path to the input data file.
    :return: A DataFrame with columns for timestamp, date, and number of cars.
    """
    try:
        df_raw = spark.read.text(file_path)
    except Exception as e:
        raise RuntimeError(f"Failed to read data from {file_path}: {e}")
    
    try:
        df = df_raw.select(
            split(df_raw['value'], ' ')[0].alias('timestamp'),
            split(df_raw['value'], ' ')[1].alias('number_of_cars')
        ).withColumn("timestamp", col("timestamp").cast("timestamp"))\
         .withColumn("date", to_date("timestamp"))\
         .withColumn("number_of_cars", col("number_of_cars").cast("integer"))
    except Exception as e:
        raise RuntimeError(f"Data preprocessing failed: {e}")
    
    return df

def print_total_number_of_cars(df):
    """
    Prints the total number of cars seen in the dataset.

    :param df: DataFrame containing the traffic data.
    """
    total_number_of_cars = df.agg(_sum("number_of_cars").alias("total_number_of_cars")).collect()[0][0]
    print(f'Total number of cars seen: {total_number_of_cars}')

def print_total_number_of_cars_per_day(df):
    """
    Prints the total number of cars seen per day.

    :param df: DataFrame containing the traffic data.
    """
    df_grouped = df.groupBy("date").agg(_sum("number_of_cars").alias("total_number_of_cars_per_day")).orderBy("date").collect()
    
    print(f'\nTotal number of cars seen per day:')
    for row in df_grouped:
        print(f"{row['date']} {row['total_number_of_cars_per_day']}")

def print_top_3_intervals_with_most_cars(df):
    """
    Prints the top 3 intervals with the most cars seen, including any ties.

    :param df: DataFrame containing the traffic data.
    """
    window = Window.orderBy(df["number_of_cars"].desc())
    ranked_df = df.withColumn("rank", rank().over(window))
    df_top_3 = ranked_df.filter(col("rank") <= 3).select("timestamp", "number_of_cars", "rank").orderBy("rank", "timestamp").collect()
    
    print(f'\nTop intervals with most cars seen:')
    for row in df_top_3:
        print(f"{row['timestamp']} {row['number_of_cars']}")

def find_least_cars_period(df):
    """
    Identifies and prints the 1.5 hour period(s) with the least number of cars.

    :param df: DataFrame containing the traffic data.
    """
    df_sorted = df.orderBy("timestamp")
    window = Window.orderBy("timestamp")
    window_rolling = window.rowsBetween(0, 2)
    df_with_sum = df_sorted.withColumn("rolling_sum", _sum("number_of_cars").over(window_rolling))\
                           .withColumn("row_number", _row_number().over(window))
    total_rows = df_with_sum.count()
    df_with_valid_windows = df_with_sum.filter(f"row_number <= {total_rows - 2}")
    
    min_rolling_sum = df_with_valid_windows.agg(_min("rolling_sum").alias("min_rolling_sum")).collect()[0][0]
    min_period = df_with_sum.filter(f"rolling_sum = {min_rolling_sum}").select("row_number").rdd.flatMap(lambda x: x).collect()
    
    extended_period = []
    for row_number in min_period:
        extended_period.extend([row_number, row_number + 1, row_number + 2])
    
    min_period_sql_list = ", ".join([str(num) for num in extended_period])
    least_cars_period = df_with_sum.filter(f"row_number in ({min_period_sql_list})").select("timestamp", "number_of_cars").collect()
    
    print("\n1.5 hour period(s) with least cars")
    for row in least_cars_period:
        print(f"{row['timestamp']} {row['number_of_cars']}")

if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Process traffic data.')
    parser.add_argument('file_path', type=str, help='Path to the input data file')
    
    try:
        args = parser.parse_args()
    except Exception as e:
        parser.print_help()
        raise SystemExit(f"Error parsing arguments: {e}")
    
    spark = None
    try:
        spark = initialize_spark_session("Automated Traffic Counter")

        # Suppress warning messages
        spark.sparkContext.setLogLevel("ERROR")

        parser = argparse.ArgumentParser(description='Process traffic data.')
        parser.add_argument('file_path', type=str, help='Path to the input data file')
        args = parser.parse_args()
        
        df = read_and_preprocess_data(spark, args.file_path)
        
        print_total_number_of_cars(df)
        print_total_number_of_cars_per_day(df)
        print_top_3_intervals_with_most_cars(df)
        find_least_cars_period(df)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if spark:
            spark.stop()