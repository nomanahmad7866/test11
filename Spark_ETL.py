from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_date, sum as spark_sum, lag, when
from pyspark.sql.window import Window

#SparkSession
spark = SparkSession.builder \
    .appName("COVID-19 ETL") \
    .getOrCreate()

#file paths
confirmed_csv_file = "Raw Data/time_series_covid19_confirmed_global.csv"
recovered_csv_file = "Raw Data/time_series_covid19_recovered_global.csv"
deaths_csv_file = "Raw Data/time_series_covid19_deaths_global.csv"

# Reading CSV files into Spark DataFrames
confirmed_data_df = spark.read.option("header", "true").csv(confirmed_csv_file)
recovered_data_df = spark.read.option("header", "true").csv(recovered_csv_file)
deaths_data_df = spark.read.option("header", "true").csv(deaths_csv_file)

# Transformation functions
def clean(df):
    cleaned_df = df.drop("Province/State", "Lat", "Long") \
                   .groupBy("Country/Region") \
                   .agg(*[spark_sum(col(c)).alias(c) for c in df.columns[4:]]) \
                   .withColumnRenamed("Country/Region", "country")
    return cleaned_df
    
def convert(df):
    id_vars = ['country']
    value_vars = df.columns[1:] 
    melted_df = df.selectExpr(*id_vars, f"stack({len(value_vars)}, {', '.join(value_vars)}) as (date_number)")
    melted_df = melted_df.withColumn("date", expr("split(date_number, ',')[0]")) \
        .withColumn("number", expr("split(date_number, ',')[1]")) \
        .drop("date_number")
    return melted_df


def calculate_daily_change(df):
    for date_col in df.columns[1:]: 
        windowSpec = Window.partitionBy("country").orderBy(date_col)
        df = df.withColumn(f"prev_{date_col}", lag(date_col, 1).over(windowSpec))
        df = df.withColumn(f"amount_of_increase_{date_col}",
                           when(col(f"prev_{date_col}").isNull(), 0)
                           .otherwise(col(date_col) - col(f"prev_{date_col}")))
        df = df.na.fill(0)
    return df


# Apply cleaning and conversion functions to each dataset
confirmed_clean_df = clean(confirmed_data_df)
recovered_clean_df = clean(recovered_data_df)
deaths_clean_df = clean(deaths_data_df)

confirmed_convert_df = convert(confirmed_clean_df)
recovered_convert_df = convert(recovered_clean_df)
deaths_convert_df = convert(deaths_clean_df)

# Calculate daily changes
confirmed_daily_df = calculate_daily_change(confirmed_convert_df)
recovered_daily_df = calculate_daily_change(recovered_convert_df)
deaths_daily_df = calculate_daily_change(deaths_convert_df)

# Rename columns
confirmed_daily_df = confirmed_daily_df.withColumnRenamed("date", "date") \
    .withColumnRenamed("number", "accumulated_confirmed") \
    .withColumnRenamed("amount_of_increase", "increase_of_confirmed")

recovered_daily_df = recovered_daily_df.withColumnRenamed("date", "date") \
    .withColumnRenamed("number", "accumulated_recovered") \
    .withColumnRenamed("amount_of_increase", "increase_of_recovered")

deaths_daily_df = deaths_daily_df.withColumnRenamed("date", "date") \
    .withColumnRenamed("number", "accumulated_deaths") \
    .withColumnRenamed("amount_of_increase", "increase_of_deaths")

# Show the results
print("Confirmed DataFrame:")
confirmed_daily_df.show(10)

print("Recovered DataFrame:")
recovered_daily_df.show(10)

print("Deaths DataFrame:")
deaths_daily_df.show(10)

# Joining confirmed, recovered, and deaths dataframes on country
combined_df = confirmed_agg_df.join(recovered_agg_df, "country", "inner").join(deaths_agg_df, "country", "inner")

////c:postgresql:dbserver", "schema.tablename", properties={"user": "username", "password": "password"}