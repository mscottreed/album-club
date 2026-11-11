-- Gold: a star schema over the silver tables, plus aggregates the dashboard reads directly.
-- Albums only: playlist ratings and favorites stay in silver. Album averages include guests.

CREATE MATERIALIZED VIEW dim_person
COMMENT "Members and guests."
AS SELECT person, first_name, last_initial, role
FROM silver_people;

CREATE MATERIALIZED VIEW dim_album
COMMENT "One row per album listen: names as the club wrote them, the meeting, and what enrichment found."
AS SELECT
  l.listen_id,
  concat_ws(' - ', l.artist, l.album) AS label,
  l.artist,
  l.album,
  l.year,
  floor(l.year / 10) * 10 AS decade,
  m.genres,
  m.list_number,
  l.week,
  mt.meeting_date,
  l.selection_method,
  m.original_artist AS musicbrainz_artist,
  m.original_title AS musicbrainz_title,
  m.spotify_album_id,
  m.spotify_album
FROM silver_listens l
JOIN silver_meetings mt USING (week)
LEFT JOIN silver_album_matches m USING (listen_id)
WHERE l.listen = 'album';

CREATE MATERIALIZED VIEW dim_track
COMMENT "The tracks of each album's Spotify edition, in order."
AS SELECT
  t.listen_id,
  t.position,
  t.disc,
  t.track,
  -- without Spotify's remaster and mono/stereo notes, as enrichment compares them:
  -- "Dreams - 2004 Remaster" -> "Dreams"
  regexp_replace(
    regexp_replace(t.title, '(?i)\\s-\\s[^-]*\\b(remaster(ed)?|mono|stereo)\\b.*$', ''),
    '(?i)\\s*[(\\[][^)\\]]*\\b(remaster(ed)?|mono|stereo)\\b[^)\\]]*[)\\]]', ''
  ) AS title,
  t.title AS spotify_title,
  t.on_original_album,
  t.duration_ms
FROM silver_tracks t
LEFT SEMI JOIN dim_album a USING (listen_id);

CREATE MATERIALIZED VIEW fact_ratings
COMMENT "One row per person per album they rated."
AS SELECT r.listen_id, r.person, r.rating
FROM silver_ratings r
LEFT SEMI JOIN dim_album a USING (listen_id)
WHERE r.rating IS NOT NULL;

CREATE MATERIALIZED VIEW fact_track_picks
COMMENT "Each person's opinion of a track: ranked (rank 1 is their top favorite), liked, or not liked."
AS SELECT p.listen_id, p.person, p.position, p.rank, p.liked
FROM silver_track_picks p
LEFT SEMI JOIN dim_album a USING (listen_id)
WHERE p.position IS NOT NULL;

CREATE MATERIALIZED VIEW agg_album_ratings
COMMENT "Each album's club average, guests included."
AS SELECT listen_id, round(avg(rating), 2) AS average_rating, count(*) AS raters
FROM fact_ratings
GROUP BY listen_id;

-- The aggregates below have a row for each person and one for 'Club', everyone together,
-- so the dashboard can show either with the same query.

CREATE MATERIALIZED VIEW agg_album_rankings
COMMENT "Each person's albums ranked by their rating, and the club's by album average. Ties share a rank."
AS WITH ratings AS (
  SELECT person, listen_id, rating FROM fact_ratings
  UNION ALL
  SELECT 'Club', listen_id, average_rating FROM agg_album_ratings
)
SELECT person, listen_id, rating, rank() OVER (PARTITION BY person ORDER BY rating DESC) AS rank
FROM ratings;

CREATE MATERIALIZED VIEW agg_person_ratings
COMMENT "Each person's average rating and how many albums they've rated, and the club's."
AS SELECT
  coalesce(person, 'Club') AS person,
  round(avg(rating), 2) AS average_rating,
  count(DISTINCT listen_id) AS albums_rated
FROM fact_ratings
GROUP BY GROUPING SETS ((person), ());

CREATE MATERIALIZED VIEW agg_person_decades
COMMENT "Each person's average rating for albums from each decade, and the club's."
AS SELECT
  coalesce(r.person, 'Club') AS person,
  a.decade,
  round(avg(r.rating), 2) AS average_rating,
  count(DISTINCT r.listen_id) AS albums_rated
FROM fact_ratings r
JOIN dim_album a USING (listen_id)
GROUP BY GROUPING SETS ((r.person, a.decade), (a.decade));

CREATE MATERIALIZED VIEW agg_unrated_albums
COMMENT "Albums each member hasn't rated yet."
AS SELECT p.person, a.listen_id
FROM dim_person p
CROSS JOIN dim_album a
LEFT ANTI JOIN fact_ratings r ON r.person = p.person AND r.listen_id = a.listen_id
WHERE p.role = 'member';
