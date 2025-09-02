import dlt
from pyspark.sql.functions import col


@dlt.table
def silver_member_rating():
    bronze_member_rating_df = dlt.readStream("music.bronze_member_rating")
    silver_album_df = dlt.readStream("music.silver_album")
    silver_member_df = dlt.readStream("music.silver_album_club_member")

    silver_data = bronze_member_rating_df.join(
        silver_album_df,
        bronze_member_rating_df.album_name == silver_album_df["name"],
    ).join(
        silver_member_df,
        bronze_member_rating_df.member == silver_member_df.short_name,
    ).select(
        silver_album_df.artist_id,
        silver_album_df.id.alias("album_id"),
        silver_member_df.id.alias("member_id"),
        bronze_member_rating_df['week_number'],
        bronze_member_rating_df['meeting_date'],
        bronze_member_rating_df['rating'],
        bronze_member_rating_df['favorite_songs'],
    )
    return silver_data