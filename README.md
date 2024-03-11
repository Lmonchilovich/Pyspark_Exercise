# Aggregated Monthly Views Spark Processor

## Overview
This Spark script processes video viewing statistics from a sample dataset to produce aggregated view counts per channel and video on a monthly basis. It's designed to handle large datasets efficiently, leveraging PySpark's distributed data processing capabilities.

## Problem Statement
Given a dataset with daily level data including `channel_id`, `video_id`, `viewCount`, and `date`, the script aggregates the total views for each video by channel per month. The goal is to transform the dataset to include `channel_id`, `video_id`, `month`, and `aggViewCount`, where `aggViewCount` represents the sum of views for each channel and video pair over all days of the month.

## Features
- Initiates a Spark session for data processing.
- Reads a Parquet dataset from an S3 bucket.
- Formats date columns from `YYYY-MM-DD` to `YYYY-MM`.
- Aggregates view counts by `channel_id`, `video_id`, and `month`.
- Outputs a DataFrame with aggregated monthly views.

## Usage
1. Ensure you have PySpark installed and configured in your environment.
2. Update the `path_video_samples` variable to point to your dataset location.
3. Run the script in your Spark environment.

## Dependencies
- PySpark

## Dataset
The script expects a Parquet formatted dataset located in an S3 bucket. The dataset should contain `channel_id`, `video_id`, `viewCount`, and `date` columns.

-Example 

<img width="1240" alt="sample dataset example photo to show data format and contents" src="https://github.com/Lmonchilovich/Pyspark_Exercise/assets/117610295/09821db5-4074-4e0b-89bf-2c2059f7d1e7">


## Output
The script produces a DataFrame with the following columns:
- `channel_id`
- `video_id`
- `month`
- `monthlyViews`

`monthlyViews` is calculated as the sum of `viewCount` for each `channel_id` and `video_id` pair for each month.

-Example

<img width="1229" alt="result of monthly video views aggregation" src="https://github.com/Lmonchilovich/Pyspark_Exercise/assets/117610295/2ae916ca-3039-415c-9c78-d6c0f21c35d2">


---

# Identifying Missing Dates in Video Data

## Overview
This Spark script identifies missing daily data for video uploads from a sample dataset. It addresses potential gaps due to failures in pulling data from external APIs, specifically targeting instances where YouTube API may not provide data for certain dates.

## Problem Statement
The script is designed to identify missing dates for each video in the dataset. Given that data should be available on a daily level up to a specific end date, this tool finds the gaps where data is missing. The output is a new DataFrame listing each video with its `channel_id`, `video_id`, `created_date`, and the dates when data was not available (`missing_dates`).

## Features
- Initiates a Spark session.
- Reads data from a specified Parquet file in an S3 bucket.
- Generates a comprehensive list of dates between two specified dates.
- Identifies missing data points by performing a cross join and a left anti join.
- Aggregates missing dates for each video.
- Supports the creation of DataFrames with a detailed list of missing dates.

## Usage
1. Ensure PySpark and Python's datetime module are available in your environment.
2. Modify the `path_video_samples` to the location of your dataset.
3. Define the `start_date` and `end_date` according to your data's timeframe.
4. Execute the script within your Spark environment to identify missing data points.

## Dependencies
- PySpark
- Python's datetime module

## Dataset
The dataset, expected in Parquet format and located in an S3 bucket, should contain details like `channel_id`, `video_id`, and `created_date`.


## Output
Produces two possible DataFrames:
1. A DataFrame formatted with a single `missing_date` for each record, if implemented.
2. A DataFrame with `channel_id`, `video_id`, `created_date`, and `missing_dates` (a list of missing dates for each video).

Both outputs aim to highlight the absence of data on specific dates for further analysis or remediation actions.

-Example 1: 

<img width="1236" alt="example photo of the first DataFrame result where there is a missing date for each record " src="https://github.com/Lmonchilovich/Pyspark_Exercise/assets/117610295/00603454-7d3e-423f-884c-9a5f43baa446">

-Example 2:

<img width="1230" alt="example photo of the second DataFrame Results where there is a list of missing dates for each video" src="https://github.com/Lmonchilovich/Pyspark_Exercise/assets/117610295/ee38de84-f4cd-4d87-aeac-6cab298682b9">




---
