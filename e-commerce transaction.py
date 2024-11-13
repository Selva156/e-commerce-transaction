from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

spark = SparkSession.builder.appName("Transactions Analysis").getOrCreate()

schema = StructType([
    StructField("trans_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("pro_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("trans_date", DateType(), True)
])

data = [
    ("T1", "U1", "P1", "Electronics", 100.0, date(2024, 1, 1)),
    ("T2", "U1", "P2", "Electronics", 150.0, date(2024, 1, 2)),
    ("T3", "U2", "P3", "Clothing", 80.0, date(2024, 2, 1)),
    ("T4", "U2", "P4", "Clothing", 120.0, date(2024, 2, 2)),
    ("T5", "U3", "P5", "Pharmacy", 40.0, date(2024, 3, 1)),
    ("T6", "U3", "P6", "Electronics", 70.0, date(2024, 3, 2)),
    ("T7", "U3", "P7", "Pharmacy", 60.0, date(2024, 3, 3)),
    ("T8", "U3", "P8", "Pharmacy", 50.0, date(2024, 3, 4)),
    ("T9", "U2", "P9", "Groceries", 60.0, date(2024, 5, 4)),
    ("T10", "U1", "P10", "Groceries", 120.0, date(2024, 5, 5)),
    ("T11", "U3", "P11", "Groceries", 120.0, date(2024, 5, 5))
]

df = spark.createDataFrame(data, schema)

spending_df = df.groupBy("user_id").agg(
    F.sum("amount").alias("total_spent"),
    F.avg("amount").alias("avg_transaction")
)

favorite_category_df = df.groupBy("user_id", "category") \
    .count() \
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("user_id").orderBy(F.desc("count"))
    )) \
    .filter(F.col("rank") == 1) \
    .select("user_id", F.col("category").alias("favorite_category"))

result_df = spending_df.join(favorite_category_df, on="user_id", how="left")

result_df.show()

