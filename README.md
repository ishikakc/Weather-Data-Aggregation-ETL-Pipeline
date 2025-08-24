#🌦️ Weather ETL Project 🌦️

This project implements a complete **ETL (Extract, Transform, Load) pipeline** using **PySpark** and **PostgreSQL** for processing and analyzing Nepal’s weather data.

---

## 📌 Overview
The pipeline follows three main steps:
1. **Extract** – Read and clean raw weather data from CSV.
2. **Transform** – Aggregate rainfall and maximum temperature averages by year and station.
3. **Load** – Store transformed data into PostgreSQL for analytics and reporting.

The pipeline ensures clean, structured datasets that can be easily used for dashboards, visualization, or further analysis.

---

## ⚙️ Technologies Used
- **Python 3.9+**
- **Apache Spark (PySpark)**
- **PostgreSQL**
- **JDBC Driver** (`postgresql-42.6.0.jar`)
- **VS Code / Ubuntu Terminal**

---

## 🛠️ Setup Instructions

### 1. Install Dependencies
Ensure you have the following installed:
- [Python 3](https://www.python.org/)
- [Apache Spark](https://spark.apache.org/downloads.html)
- [PostgreSQL](https://www.postgresql.org/download/)

## Install Python packages:

***pip install pyspark
pip install psycopg2 ***

---
### 2. PostgreSQL Setup
**CREATE DATABASE weather_db;
CREATE USER weather_user WITH PASSWORD 'your_password_here';
GRANT ALL PRIVILEGES ON DATABASE weather_db TO weather_user; **

---

### 3. JDBC Driver

**WeatherETL_Project/jars/postgresql-42.6.0.jar**

---

### 📖 User Manual

**Step 1: Extract
python3 extract/extract_weather.py**

**Step 2: Transform
python3 transform/transform_weather.py**

**Step 3: Load
python3 load/load_weather.py**





