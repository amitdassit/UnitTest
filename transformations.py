# transformations.py
from pyspark.sql import functions as F

def add_total_column(df):
    return df.withColumn("total", F.col("price") * F.col("qty"))

def filter_expensive(df, min_price):
    return df.filter(F.col("price") >= min_price)

def normalize_category(df):
    return df.withColumn(
        "category_norm",
        F.lower(F.trim(F.col("category")))
    )
