import datetime
from pyspark.sql.functions import col, collect_list
from pyspark.sql import SparkSession

path_video_samples = "YOUR_S3_BUCKET_PATH_HERE"  # parquet dataset
spark = SparkSession.builder.getOrCreate()

# Loading the data

# Read the paquet file and display the data
df_samples = spark.read.parquet(path_video_samples)
df_samples.show(20)

# Create list of dates

start_date = datetime.date(2023, 2, 2)
end_date = datetime.date(2023, 4, 18)
curr_date = start_date

# List to collect dates
date_list = []

# Iterate through dates between first video and last date collected(2023-04-18)
while curr_date <= end_date:
    date_list.append(curr_date.strftime("%Y-%m-%d"))
    curr_date += datetime.timedelta(days=1)

# Map the list for pyspark dataframe
mapped_list = list(map(lambda el: [el], date_list))

# Turn date_list into a pyspark dataframe
date_df = spark.createDataFrame(mapped_list, ["date"])

# get the video id's into a dataframe
video_df = df_samples.select(col('video_id'), col("channel_id"), col("created_date")).distinct()

# Cross Join with video id to get a dataframe of all uploads
crossJoin_df = date_df.crossJoin(video_df)

# Join df_samples and crossJoin_df using left anti to get the missing date format
missing_date = crossJoin_df.join(df_samples,["video_id", "date"], "leftanti")
missing_date.show(20)

# Collect each of the missing dates
df_missing_dates = missing_date.groupBy("channel_id", "video_id").agg(collect_list("date").alias("missing_dates"))
df_missing_dates.show(20)
