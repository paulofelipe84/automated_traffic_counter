import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from automated_traffic_counter import (
    initialize_spark_session, 
    read_and_preprocess_data, 
    print_total_number_of_cars, 
    print_total_number_of_cars_per_day, 
    print_top_3_intervals_with_most_cars, 
    find_least_cars_period
)

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("Testing").getOrCreate()

def test_print_total_number_of_cars(spark, capfd):
    data = [("2023-04-01 12:00:00", 10), ("2023-04-01 12:15:00", 5)]
    df = spark.createDataFrame(data, ["timestamp", "number_of_cars"])
    
    print_total_number_of_cars(df)
    
    out, err = capfd.readouterr()
    
    assert "Total number of cars seen: 15" in out, f"Expected result not found in output"

def test_print_total_number_of_cars_per_day(spark, capfd):
    data = [("2023-04-01 12:00:00", 10), ("2023-04-01 12:15:00", 5), ("2023-04-02 12:00:00", 20)]
    df = spark.createDataFrame(data, ["timestamp", "number_of_cars"]).withColumn("timestamp", col("timestamp").cast("timestamp"))\
        .withColumn("date", to_date("timestamp"))\
        .withColumn("number_of_cars", col("number_of_cars").cast("integer"))
    
    print_total_number_of_cars_per_day(df)
    
    out, err = capfd.readouterr()
    
    expected_output = "\nTotal number of cars seen per day:\n2023-04-01 15\n2023-04-02 20"
    for line in expected_output.split('\n'):
        assert line in out, f"Expected line '{line}' not found in output"


def test_print_top_3_intervals_with_most_cars(spark, capfd):
    data = [
        ("2023-04-01 12:00:00", 10),
        ("2023-04-01 12:30:00", 20),
        ("2023-04-01 13:00:00", 5),
        ("2023-04-01 13:30:00", 30),
        ("2023-04-01 14:00:00", 25)
    ]
    df = spark.createDataFrame(data, ["timestamp", "number_of_cars"])\
              .withColumn("timestamp", col("timestamp").cast("timestamp"))\
              .withColumn("number_of_cars", col("number_of_cars").cast("integer"))

    print_top_3_intervals_with_most_cars(df)

    out, err = capfd.readouterr()

    expected_lines = [
        "\nTop intervals with most cars seen:",
        "2023-04-01 13:30:00 30",
        "2023-04-01 14:00:00 25",
        "2023-04-01 12:30:00 20"
    ]
    for line in expected_lines:
        assert line in out, f"Expected line '{line}' not found in output"

def test_print_top_intervals_with_most_cars_including_ties(spark, capfd):
    data = [
        ("2023-04-01 12:00:00", 10),
        ("2023-04-01 12:30:00", 20),
        ("2023-04-01 13:30:00", 20),  # Tie for second place
        ("2023-04-01 15:00:00", 15),
        ("2023-04-01 18:00:00", 15)   # Tie for third place
    ]
    df = spark.createDataFrame(data, ["timestamp", "number_of_cars"])\
              .withColumn("timestamp", col("timestamp").cast("timestamp"))\
              .withColumn("number_of_cars", col("number_of_cars").cast("integer"))

    print_top_3_intervals_with_most_cars(df)

    out, err = capfd.readouterr()

    expected_output = [
        "\nTop intervals with most cars seen:",
        "2023-04-01 12:30:00 20",
        "2023-04-01 13:30:00 20",  # Tie included
        "2023-04-01 15:00:00 15",
        "2023-04-01 18:00:00 15"   # Tie included
    ]
    for line in expected_output:
        assert line in out, f"Expected line '{line}' not found in output"


def test_find_least_cars_period(spark, capfd):
    data = [
        ("2023-04-01 12:00:00", 10),
        ("2023-04-01 12:30:00", 5),
        ("2023-04-01 13:30:00", 20),
        ("2023-04-01 14:00:00", 30),
        ("2023-04-01 16:00:00", 2),
        ("2023-04-01 16:30:00", 1),
        ("2023-04-01 18:30:00", 3)
    ]
    df = spark.createDataFrame(data, ["timestamp", "number_of_cars"])\
              .withColumn("timestamp", col("timestamp").cast("timestamp"))\
              .withColumn("number_of_cars", col("number_of_cars").cast("integer"))

    find_least_cars_period(df)

    out, err = capfd.readouterr()

    expected_output = [
        "\n1.5 hour period(s) with least cars",
        "2023-04-01 16:00:00 2",
        "2023-04-01 16:30:00 1",
        "2023-04-01 18:30:00 3"
    ]
    for line in expected_output:
        assert line in out, f"Expected line '{line}' not found in output"
