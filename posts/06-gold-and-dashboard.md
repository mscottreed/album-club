# Album Club on Databricks, Part 6: Gold Tables and a Dashboard

So far this series has been about getting the data right. I parsed five years of OneNote pages,
then matched every album to the Spotify edition we played and to its original release. This is
the post where I get to use it. I want each of us in the club to be able to pick our name and see
our top 10, our average by decade, and the albums we still haven't rated.

To get there I build gold tables on top of silver and an AI/BI dashboard that reads them. Both are
deployed by the bundle, like everything else in the series.

The code for this post is tagged `post-06` in
[mscottreed/album-club](https://github.com/mscottreed/album-club).

## What I wanted to see

Pick a person and you get:

- their average rating, overall and by decade
- their top 10 albums
- every album they've rated, best to worst
- the albums they haven't rated yet, with the club's average next to each one
- an album page with their rating, the club's average, and their favorite tracks

There's also a "Club" choice, which shows the same page for all of us together, so the top 10
becomes the club's top 10.

## Two loose ends first

Before building anything new I had two things to clean up.

Looking at the albums nobody had rated, I found Weezer's Blue Album (W153) and LCD Soundsystem's
*Sound of Silver* (W156). Both pages have ratings in OneNote, but in the export I made back in
part 3 their ratings tables are empty. A new export means firing up Windows OneNote again, and I
didn't want to go through all that for two tables. Instead I copied each table straight out of
OneNote as tab-separated text:

```text
Name	Album Rating	Favorite songs
Bill L	6.7	4, 9
Linda
...
```

The real files have our full names, so they live where the export does, in the `onenote_exports`
volume (as `ratings_tables/W153.tsv`) and my gitignored `data/` folder, and never in git. A new
bronze table loads them with Auto Loader. It's the same table that's on the page, so the parser
reads it with the same rules, and a pasted table replaces the page's table for that week:

```python
def parse_pasted_table(week: int, text: str, people: list[dict], favorites_overrides: dict) -> list[dict]:
    """Responses from a page's ratings table copied as tab-separated text, header first. For
    tables filled in after the export, so the page itself doesn't have them."""
    rows = [[cell.strip() for cell in line.split("\t")] for line in text.splitlines() if line.strip()]
    return _responses(week, rows, people, favorites_overrides)
```

The first run with this failed. I'd wrapped the UDF in `F.when(...)` so it would only run for
weeks with a pasted table, but Spark called it on every row anyway, and every other week handed
it `None`. Now the UDF checks for `None` itself.

*Kind of Blue* (W159) has no ratings in the export either, but we rated it after I started this
project, so it can wait for the app in part 7.

The second loose end was a bad match from part 5. W010, The Magnetic Fields' *69 Love Songs*, had
been matched on MusicBrainz to "69 Love Songs (sampler)", a promo that came out before the album.
The matcher ignores suffixes like "(sampler)" when it compares titles, so the sampler and the
album tied, and ties went to whichever came out first. The sampler has no official releases, so
W010 ended up with no original track list. Now a tie goes to the closer full title before the
earlier date. I reran the matching for every album and W010 was the only one that changed. All
69 songs now match the original album.

## A star schema in SQL

Gold is a star schema. If you haven't run into one, dimension tables describe things (people,
albums, tracks) and fact tables record what happened (someone rated an album, someone picked a
favorite song). Gold only covers albums; the two playlist listens stay in silver.

| Table | One row per | What's in it |
|---|---|---|
| `dim_person` | person | name, member or guest |
| `dim_album` | album we listened to | artist and album as we wrote them, year, decade, genres, 1001 number, meeting date, era |
| `dim_track` | track on the edition we played | position, disc and track, title |
| `fact_ratings` | person and album | rating |
| `fact_track_picks` | person and track | rank (1 is their top favorite), liked or not |

Parts 4 and 5 were Python, because parsing pages and matching albums needed Python libraries and
local tests. Gold is just joins and group-bys, so I wrote it in SQL, in one file
(`src/pipeline/gold.sql`) in the same pipeline. Lakeflow runs SQL and Python files side by side,
and the SQL can read the Python tables by name:

```sql
CREATE MATERIALIZED VIEW fact_ratings
COMMENT "One row per person per album they rated."
AS SELECT r.listen_id, r.person, r.rating
FROM silver_ratings r
LEFT SEMI JOIN dim_album a USING (listen_id)
WHERE r.rating IS NOT NULL;
```

Albums show up the way we wrote them on the pages, like "Led Zeppelin - II" and "Weezer (Blue
Album)", with MusicBrainz's names kept in their own columns. Track titles lose Spotify's remaster
notes, so "Dreams - 2004 Remaster" is just "Dreams".

## Doing the math ahead of time

The dashboard could work out the top 10s and averages itself. I decided to do that in gold
instead, so every query in the dashboard is a plain select. That's five more tables:

- `agg_album_ratings`: each album's club average (guests count) and how many of us rated it
- `agg_album_rankings`: each person's albums, ranked by their rating
- `agg_person_ratings`: each person's average and how many albums they've rated
- `agg_person_decades`: each person's average by decade
- `agg_unrated_albums`: the albums each member hasn't rated

The Club view needs the same numbers for all of us together. Instead of a second set of tables,
each one gets an extra row where the person is `Club`. For averages, `GROUPING SETS` does both in
one query. `(person)` gives a row per person, and `()` gives one row for everyone, with `person`
coming out null:

```sql
CREATE MATERIALIZED VIEW agg_person_ratings
COMMENT "Each person's average rating and how many albums they've rated, and the club's."
AS SELECT
  coalesce(person, 'Club') AS person,
  round(avg(rating), 2) AS average_rating,
  count(DISTINCT listen_id) AS albums_rated
FROM fact_ratings
GROUP BY GROUPING SETS ((person), ());
```

That makes the club's average an average of every rating, so an album seven of us rated counts
more than one a guest rated alone. For the club's rankings, albums are ranked by their average:

```sql
CREATE MATERIALIZED VIEW agg_album_rankings
COMMENT "Each person's albums ranked by their rating, and the club's by album average. Ties share a rank."
AS WITH ratings AS (
  SELECT person, listen_id, rating FROM fact_ratings
  UNION ALL
  SELECT 'Club', listen_id, average_rating FROM agg_album_ratings
)
SELECT person, listen_id, rating, rank() OVER (PARTITION BY person ORDER BY rating DESC) AS rank
FROM ratings;
```

Our ratings tie a lot. Bill R gave 11 albums an 8.0, and nine of Chantal R's albums are tied for
her second place. `rank()` gives tied albums the same rank, so a top 10 includes everything tied
at the cutoff, which makes Bill R's top 10 eighteen albums long. I could have broken the ties by
club average, but then one of Bill's 8.0s would outrank another 8.0 just because the rest of us
liked it more.

## The dashboard as a bundle resource

An AI/BI dashboard turns out to be a `.lvdash.json` file plus a few lines of YAML:

```yaml
resources:
  dashboards:
    album_club_dashboard:
      display_name: Album club
      file_path: ../src/dashboards/album_club.lvdash.json
      warehouse_id: ${var.warehouse_id}
      # The dashboard's queries name tables without a schema; each target points them at its own.
      dataset_catalog: ${var.catalog}
      dataset_schema: ${resources.schemas.album_club.name}
```

`dataset_catalog` and `dataset_schema` let one file work for dev and prod. The queries just say
`FROM agg_person_ratings`, and the bundle points them at `dev_scott_album_club` or `album_club`.
My Free Edition workspace has one SQL warehouse, so `warehouse_id` is a variable that looks it up
by name:

```yaml
  warehouse_id:
    description: SQL warehouse the dashboard runs on (Free Edition has one)
    lookup:
      warehouse: Serverless Starter Warehouse
```

The JSON file is the whole dashboard: the SQL for each dataset, the pages, and every widget with
its position and settings. I couldn't find docs for the format, and the examples I found online
didn't agree on key names, so I deployed it and looked at what rendered. The counters and charts
worked the first time. The two tables showed "Invalid widget definition is imported" until I
gave every column the full list of settings from a dashboard exported into Databricks'
[bundle-examples](https://github.com/databricks/bundle-examples) repo. Those column settings are
most of the file's 1,400 lines.

## Getting rid of "All"

The first version used a regular dashboard filter to pick the person, and filters come with an
"All" option. With All picked, the charts didn't look right. The decade bars stacked seven
people's averages on top of each other, way past 10, and the top 10 had everyone's favorites
mixed together. What I wanted All to show was the club's view, and a filter can't do that. A
dataset parameter can. Every query takes the person as `:person`:

```sql
SELECT r.rank, r.rating, a.label
FROM agg_album_rankings r
JOIN dim_album a USING (listen_id)
WHERE r.person = :person AND r.rank <= 10
```

The Person dropdown lists the members plus Club and starts on me. A parameter always has exactly
one value, so there's no All.

The Album page has its own parameter for the album, and I wanted it to start blank. A default
with no values at all broke the page with "Conflicting selections for parameter: album". A default
of an empty string works, and since no album has an empty name, the page says "No data" until you
pick one.

Parameters did cost me one thing I'd planned on. AI/BI dashboards can open another page when you
click a bar, already filtered to that bar, but the click gets passed as a filter, and my pages
take parameters. So clicking an album in the top 10 doesn't take you to the Album page. You pick
it from the dropdown there.

One more gotcha: after a pipeline run, the published dashboard kept showing the old numbers until
I hit refresh.

## The results

Here's where the numbers landed in dev. The gold tables:

| Table | Rows |
|---|---|
| `dim_person` | 12 |
| `dim_album` | 163 |
| `dim_track` | 2,149 |
| `fact_ratings` | 1,041 |
| `fact_track_picks` | 4,496 |
| `agg_album_rankings` | 1,203 |
| `agg_unrated_albums` | 110 |

The club averages 6.39 across 162 albums. *Hotel California* tops our list at 8.34, just ahead of
*Rumours* at 8.31. At the other end, seven of us gave Napalm
Death's *Scum* an average of 1.07, with Devo's *Q: Are We Not Men? A: We Are Devo!* at 1.79. The decades barely differ: the club's averages
run from 5.98 for the 2020s to 6.51 for the 2010s.

Each member's numbers come from `agg_person_ratings`, with the count from `agg_unrated_albums`.
My average the highest at a 7.06, and the Bill L's is the lowest at 5.5.

## Prod

<!-- Filled in after the prod deploy. -->

## Next: an app for entering this week's ratings

In post 7 I build a Databricks App for recording a meeting (the album, the date, and each of our
ratings and favorite songs) and work out how its data joins the OneNote history in silver.
