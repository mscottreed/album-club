import dlt
from pyspark.sql.functions import col

@dlt.table
def silver_song():
    bronze_song_df = dlt.readStream("music.bronze_song")
    silver_album_df = dlt.readStream("music.silver_album")

    silver_song_df = bronze_song_df.join(
        silver_album_df,
        bronze_song_df.album_number == silver_album_df.id
    ).select(
        silver_album_df.artist_id,
        silver_album_df.id.alias("album_id"),
        bronze_song_df.name,
        bronze_song_df.track_number
    )
    return silver_song_df