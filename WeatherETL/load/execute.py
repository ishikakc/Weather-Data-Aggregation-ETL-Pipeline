from pyspark.sql import SparkSession
import os

def main():
    
    jdbc_driver_path = "/home/ishika/WeatherETL_Project/jars/postgresql-42.6.0.jar"

    PG_URL = "jdbc:postgresql://localhost:5432/weather_db"
    PG_USER = "weather_user"
    PG_PASSWORD = " " #Replace with YOUR_PASSWORD
    PG_DRIVER = "org.postgresql.Driver"

    pg_properties = {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": PG_DRIVER
    }

    spark = SparkSession.builder \
        .appName("WeatherETL_Load") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()

    try:
        base_path = "/home/ishika/WeatherETL_Project/transform/output"
        max_temp_path = os.path.join(base_path, "max_temp_avg")
        rainfall_path = os.path.join(base_path, "rainfall_avg")

        max_temp_df = spark.read.parquet(max_temp_path)
        rainfall_df = spark.read.parquet(rainfall_path)

        max_temp_df.write.jdbc(
            url=PG_URL,
            table="max_temperature",
            mode="overwrite",   
            properties=pg_properties
        )

        rainfall_df.write.jdbc(
            url=PG_URL,
            table="average_rainfall",
            mode="overwrite",
            properties=pg_properties
        )

        print("Data successfully loaded into PostgreSQL.")

    except Exception as e:
        print(f"Failed to load data into PostgreSQL: {e}")

    finally:
        spark.stop()
        print("Spark stopped successfully.")

if __name__ == "__main__":

    main()


