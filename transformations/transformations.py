from pyspark.sql import functions as F
import dlt
from pyspark.sql.functions import when, col

@dlt.table(name="bronze_data")
def bronze_data():
        df = spark.table("default.ad_10000_records")
        df_renamed = df.select([
        F.col(col).alias(col.replace(" ", "_")) for col in df.columns
        ])
        return df_renamed

@dlt.table(name="silver_data")
def silver_data():
    df = dlt.read("bronze_data") \
            .na.drop("any") \
            .drop("City", "Ad_Topic_Line", "Timestamp")

    df = df.withColumn("Gender", )
    return df

