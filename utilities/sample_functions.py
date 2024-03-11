from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType


# schema for the return type of process_batch_of_videos
views_schema = ArrayType(
    StructType([
        StructField("video_id", StringType(), False),
        StructField("views", IntegerType(), False)
    ])
)


# YouTube API response simulator on a single video
def calculate_views(video_id):
    # for this excersise we'll just generate a number instead of using the API
    return int(abs(hash(video_id) % 1000))


# UDF helper function that takes a list of video_ids (as a list) and returns a list of tuples
# :param video_ids: a list of up to 10 video_id values
# :return a list of tuples (video_id, viewCount). Same size as the input
@udf(returnType=views_schema)
def process_batch_of_videos(video_ids):
    if len(video_ids) > 10:
        raise Exception(f"Too many ({len(video_ids)}) videos in the list. Expected up to 10")
    return [(vid, calculate_views(vid)) for vid in video_ids]