from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, avg
import os

def main():
    spark = SparkSession.builder.appName("WeatherETL_Transform").getOrCreate()

    raw_parquet_path = "/home/ishika/WeatherETL_Project/extract/output/raw_weather.parquet"
    output_base = "/home/ishika/WeatherETL_Project/transform/output"
    os.makedirs(output_base, exist_ok=True)

    df = spark.read.parquet(raw_parquet_path)

    df = df.withColumn("Date", col("Date").cast("date"))

    station_cols = [c for c in df.columns if c not in ("Date", "Indicators", "Months")]

    rainfall_df = (
        df.filter(col("Indicators") == "Rainfall")
          .groupBy(year("Date").alias("Year"))
          .agg(*[avg(col(c).cast("float")).alias(f"{c}_AvgRainfall") for c in station_cols])
    )

    max_temp_df = (
        df.filter(col("Indicators") == "Max Temp")
          .groupBy(year("Date").alias("Year"))
          .agg(*[avg(col(c).cast("float")).alias(f"{c}_AvgMaxTemp") for c in station_cols])
    )

    rainfall_path = os.path.join(output_base, "rainfall_avg")
    max_temp_path = os.path.join(output_base, "max_temp_avg")
    rainfall_df.write.mode("overwrite").parquet(rainfall_path)
    max_temp_df.write.mode("overwrite").parquet(max_temp_path)

    rainfall_df.write.mode("overwrite").csv(rainfall_path + "_csv", header=True)
    max_temp_df.write.mode("overwrite").csv(max_temp_path + "_csv", header=True)

    print("Transform step completed. Outputs saved as Parquet & CSV.")
    spark.stop()

if __name__ == "__main__":
    main()