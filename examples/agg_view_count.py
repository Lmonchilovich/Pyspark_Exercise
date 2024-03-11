
from pyspark.sql.functions import date_format, col, sum
from pyspark.sql import SparkSession

# Loading the data
path_video_samples = "YOUR_S3_BUCKET_PATH_HERE"  # parquet dataset

# Initiating the spark session
spark = SparkSession.builder.getOrCreate()

# Read the parquet file and display the data
df_samples = spark.read.parquet(path_video_samples)
df_samples.show(20)

# Format the date from YYYY-MM-DD to YYYY-MM
df_month = df_samples.withColumn("month", date_format(col('date'), "yyyy-MM"))

# Aggregate the values by pyspark.sql.DataFrame.groupBy for channel id, video id, and month columns
df_agg = df_month.groupBy('channel_id', 'video_id', 'month').agg(sum('viewCount').alias('monthlyViews'))

df_monthly_views = df_agg
df_monthly_views.show(20)




