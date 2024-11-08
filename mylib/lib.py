import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from pyspark.sql.types import (
     StructType, 
     StructField, 
     IntegerType, 
)

LOG_FILE = "pyspark_output.md"

def log_output(operation, output, query=None):
    """adds to a markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query: 
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")

def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

def end_spark(spark):
    spark.stop()
    return "stopped spark session"

def extract(
    url="https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/births/US_births_2000-2014_SSA.csv",
    file_path="data/birth.csv",
    directory="data",
):
    """Extract a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, "wb") as f:
            f.write(response.content)
        return file_path
    else:
        print(f"Failed to download. Status code: {response.status_code}")
        return None

def load_data(spark, data="data/birth.csv", name="Birth"):
    """load data"""
    # data preprocessing by setting schema
    schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("date_of_month", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("births", IntegerType(), True)
    ])

    df = spark.read.option("header", "true").schema(schema).csv(data)

    log_output("load data", df.limit(10).toPandas().to_markdown())

    return df

def describe(df):
    summary_stats_str = df.describe().toPandas().to_markdown()
    log_output("describe data", summary_stats_str)

    return df.describe().show()

def query(spark, df, query, name): 
    """queries using spark sql"""
    df = df.createOrReplaceTempView(name)

    log_output("query data", spark.sql(query).toPandas().to_markdown(), query)

    return spark.sql(query).show()

def transform(df):
    """Transforms the 'month' column from int to abbreviated month name."""
    
    # Define the mapping for month numbers to month abbreviations
    df = df.withColumn(
        "month_str",
        when(col("month") == 1, "Jan")
        .when(col("month") == 2, "Feb")
        .when(col("month") == 3, "Mar")
        .when(col("month") == 4, "Apr")
        .when(col("month") == 5, "May")
        .when(col("month") == 6, "Jun")
        .when(col("month") == 7, "Jul")
        .when(col("month") == 8, "Aug")
        .when(col("month") == 9, "Sep")
        .when(col("month") == 10, "Oct")
        .when(col("month") == 11, "Nov")
        .when(col("month") == 12, "Dec")
        .otherwise("Unknown")  # Just in case there's an invalid month value
    )

    log_output("transform month column", df.limit(10).toPandas().to_markdown())

    return df.show()

