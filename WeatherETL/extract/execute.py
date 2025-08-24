from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, when
import os

def main():

    spark = SparkSession.builder.appName("WeatherETL_Extract").getOrCreate()
    
    input_path = "/home/ishika/WeatherETL_Project/input/weatherNepal_CSV.csv"
    output_path = "/home/ishika/WeatherETL_Project/extract/output/raw_weather.parquet"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
 
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df = df.withColumn(
        "Date",
        when(col("Months").rlike(r"\d{4}-\d{2}"), to_date(col("Months"), "yyyy-MM"))
        .otherwise(None)
    )

    print("Raw Data Sample:")
    df.show(5)
    df.printSchema()
    
    df.write.mode("overwrite").parquet(output_path)
    print(f"Extract step completed. Raw data saved at {output_path}")
    
    spark.stop()

if __name__ == "__main__":
    main()