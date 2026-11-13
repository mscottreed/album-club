# Album Club on Databricks, Part 4: Parsing into Silver

At the end of [part 3](03-onenote-to-bronze.md), bronze had one row per OneNote page: a title, a
created date and a blob of HTML. In this post I turn those pages into silver tables you can
query: meetings, what we listened to, ratings, favorite songs and attendance. Most of the work is
plain Python. I had to decide what five years of shorthand meant, write that down, and test it.

The code for this post is tagged `post-04` in
[mscottreed/album-club](https://github.com/mscottreed/album-club).

## Writing down the rules

Every rule the parser follows is in [docs/data-decisions.md](../docs/data-decisions.md), and most
of them came from questions about the pages rather than from code. OneNote records when a page
was created, not when the meeting happened. W003, W109 and W133 each covered two or three albums,
with one page per album, and at W133 we only played some of the tracks from each album, listed in a line
under each ratings table. Twice, for *Dirt* and *Superunknown*, there was a playlist for members
who don't like grunge, and each person rated the album, the playlist, or both.

A blank rating means the person wasn't there, but one member sent in a favorite song afterwards,
so a favorite can exist without a rating. The albums were also picked three different ways over
the years: from the *1001 Albums* list, then by vote, then by me. I took the boundaries between
those eras from where the cycle pages sit in the OneNote section.

When I change a rule, the document changes in the same commit as the code.

## People

Names on the pages are written several ways: a full name, a first name and last initial, or just
a first name. The repo is public, so full names stay out of it. I keep a seed file in git that
lists each person by first name and last initial, with their role:

```csv
person,first_name,last_initial,role
Bill L,Bill,L,member
Bill R,Bill,R,member
Chantal R,Chantal,R,member
...
Craig M,Craig,M,guest
```

A name on a page matches a person by first name, plus the last initial when one is written.
"Linda" on its own matches Linda L because there's only one Linda. "Bill" on its own matches no
one, because there are two, and the parser flags it. Every name in the export matched.

The seeds live in `src/album_club/seeds/`, next to the code that reads them. `bundle deploy` syncs
them to the workspace with everything else, so there's one copy, and it's in git.

## The ratings table

161 of the 163 meeting pages have exactly one ratings table, and the header is nearly always
`Name | Album Rating | Favorite songs`. There are seven variants of that header in the export, so
the parser picks the table whose header has a `Name` column and a column with "Rating" in it.

The other two pages, W056 and W059, came through as a title and nothing else. They look fine in
OneNote, but the export turned all of their content into images, and the text survives only in
each image's `alt` attribute, one table cell per line (names shortened here):

```text
Name
Album Rating
Favorite songs
Bill L
3.5

Bill R

13
...
```

When a meeting page has no ratings table, the parser looks for an image whose alt text starts
with the table header and reads the cells in groups of the header's width.

## Shorthand

Most ratings are plain numbers. The parser reads `8?`, `?8.X?` and `3.x` as whole numbers, and
W122's `8.4 (A), 9.8 (P)` rates the album and the playlist in one cell.

Favorite songs are track numbers in ranked order, and that's where most of the shorthand is.
These are the forms that come up across 1,181 favorites cells:

| Cell | Meaning |
|---|---|
| `2, 3, 1` | Tracks 2, 3 and 1, in that order |
| `10, (2, 5, 8)` | Parentheses mark a tie, so they're ignored |
| `8, 1, 2, 4, ...` | These four, and I liked every other track |
| `2, 3, 1, ... (x5, 9, 10)` | These three, I liked the rest, except 5, 9 and 10 |
| `7, 2, 11, all but (10, 6)` | The same thing, written out |
| `Disc 1: 4, 7 Disc 2: 1, 3` | *The White Album* |
| `5-8, 10` | A range |
| `4, 3, 5, 1ii-v` | Track 1, movements ii to v |

`src/album_club/shorthand.py` breaks a cell into tokens (discs, `...`, ranges, track numbers with
or without an `x`, words and parentheses) and walks through them. Each cell becomes a list of
ranked favorites, a "likes the rest" flag, and a list of tracks the person didn't like:

```python
@dataclass
class Favorites:
    """Ranked favorites as (disc, track) pairs; disc is None unless the cell labels discs."""

    ranked: list[tuple[int | None, int]] = field(default_factory=list)
    likes_rest: bool = False
    excluded: list[tuple[int | None, int]] = field(default_factory=list)
    review_reasons: list[str] = field(default_factory=list)
```

When a cell has something the rules don't cover, the parser still returns its best reading and
adds a review reason, so nothing gets dropped without a flag. On the full export, 29 cells were
flagged. Twelve of them were notes the numbers didn't need, like `(lyrics)` or `outro`. For the
other 17, I had to say what the cell meant:

| Cell | What it meant |
|---|---|
| `5, 7, 8 (liked all except last 2-3)` | All except the last three tracks |
| `1, 2, 3, 5, 7, 9, 11 , 4 (^10, 8, 6)` | `^` marked tracks I didn't like |
| `3*, 5, 7, 10, 12 , 8*, but I can listen to them all` | Stars meant overplayed, and "can listen to" isn't liking |
| `1, 5, 6, 3, 7, 8, 9 (X 2 or 4)` | Didn't like 2 or 4 |

I put each answer in an overrides file in git. The override is clean shorthand that replaces the
original cell, and the note column says why:

```csv
week,person,listen,favorites,note
20,Julia R,,"5, 7, 8, ... x10, 11, 12",Liked all except the last 2-3 tracks; read as the last three (10-12)
43,Scott R,,"1, 2, 3, 5, 7, 9, 11, 4 (x10, 8, 6)",^ marks tracks not liked
```

Because the overrides use the same shorthand, they go through the same parser.
`tests/test_shorthand.py` covers each rule with made-up cells.

## Meeting dates

Seventeen pages have the meeting date written right above the ratings table, like
`Sunday June 30th, 2024`. The rest needed a rule. We almost always met on Sundays, so the meeting
date is the first Sunday after the page was created that's also after the previous meeting:

```python
def assign_meeting_dates(meetings: list[dict], overrides: dict[int, dt.date]) -> list[dict]:
    dated = []
    previous = None
    for m in sorted(meetings, key=lambda m: m["week"]):
        review = None
        if m["week"] in overrides:
            date, source = overrides[m["week"]], "override"
        elif m["body_date"] is not None:
            date, source = m["body_date"], "page"
        else:
            date, source = next_sunday(m["created"]), "sunday_rule"
            while previous is not None and date <= previous:
                date += dt.timedelta(days=7)
        ...
```

The previous-meeting check is there because I often created pages in batches. W009, W010, W011,
W013 and W015 were all created on January 30, 2022, in between pages created later. Working in
week order puts them on consecutive Sundays.

Checking the results turned up two problems. My first version took the first date anywhere on
the page, and some pages had other dates that happened to fall in range. Every real meeting date
sits right before the ratings table, so that's now the only place the parser looks.

The other was W114. Its page says "Sunday August 2nd, 2025", but August 2, 2025 was a Saturday.
The parser now checks a written weekday against the date and flags a mismatch. I think W114 was
the Sunday, so it has an entry in a `meeting_dates` override file.

Each meeting records where its date came from: `page`, `sunday_rule` or `override`.

## The silver pipeline

The parsing code is plain Python in `src/album_club/`, and I test it locally.
`src/pipeline/silver.py` wires it into the same pipeline as bronze. A UDF parses each page into a
struct, and the silver tables are materialized views built from it:

| Table | One row per |
|---|---|
| `silver_meetings` | meeting, with its date and where the date came from |
| `silver_listens` | album or playlist listened to at a meeting |
| `silver_ratings` | person per listen they rated or picked favorites for |
| `silver_favorites` | ranked favorite track |
| `silver_attendance` | person per meeting, present or not |
| `silver_people` | person, from the seed |
| `silver_review` | anything the parser flagged |

Bronze is a streaming table, which only adds rows for new files. A materialized view works
differently. It's defined by a query, and each pipeline update leaves it holding that query's
result over everything in its sources. Databricks refreshes some materialized views
incrementally, but the result is always the same as recomputing from scratch. That's what silver
needs, because when a parsing rule changes, it has to apply to every page, not just new ones.

`silver_ratings` has expectations that every rating is between 0 and 10 and every name matched a
person. An expectation is a data quality check declared on the table:

```python
@dp.expect("rating_in_range", "rating IS NULL OR rating BETWEEN 0 AND 10")
@dp.expect("person_matched", "person IS NOT NULL")
```

With plain `expect`, a row that fails is still written, and the pipeline counts the failures for
each expectation in the update's details. `expect_or_drop` would leave the row out, and
`expect_or_fail` would stop the update. Every expectation in this project is plain `expect`, so a
bad row stays in the table where I can see it.

Each meeting date depends on the one before it, so `silver_meetings` runs the date
function over every week at once with `applyInPandas`.

Three things broke on the way. The first was that `applyInPandas` call. I grouped by a literal to
get a single group, and the pipeline failed with "Found the unresolved operator:
unresolvedordinal(1)". Spark reads a literal integer in `groupBy` as a column position. Grouping by a named constant column works:

```python
return per_week.withColumn("all_weeks", F.lit(1)).groupBy("all_weeks").applyInPandas(...)
```

The next one needs a bit of background on how Spark runs code. Spark splits the work between a
driver, which runs your pipeline code and plans the queries, and workers, which carry out the
plan on chunks of the data. A Python UDF runs on the workers, so Spark pickles the function on
the driver and sends it over. Anything the function refers to has to travel with it or already be
installed on the worker.

The UDF failed on the workers with `ModuleNotFoundError: No module named 'album_club'`. The bronze UDF from part 3 had worked, so something in the larger silver parser was being pickled as
a reference to the package, which the workers then tried to import. I tried the bundle template's
approach of installing the project with `--editable ${workspace.file_path}`, and it didn't help.
cloudpickle has an API for this case, a local package the workers don't have:

```python
from pyspark import cloudpickle

import album_club

# The workers that run UDFs can't import album_club, so ship its code with each function instead.
cloudpickle.register_pickle_by_value(album_club)
```

I also changed where the seed files get read. The parsing functions used to open the CSVs
themselves, so every worker running the UDF had to find them. Now `silver.py` reads them once when
the pipeline loads, and passes people and overrides into the parsing functions as arguments. The
data goes to the workers inside the pickled UDF. That also makes the functions easier to test,
since a test can pass in made-up people.

Last, `silver_ratings` and `silver_favorites` came out empty. The function that attaches each
response to its listen joined two DataFrames derived from the same `silver_listens` read, one for
albums and one for playlists. I didn't need that join. A playlist listen's page number is the page
its responses are on, so one join does it:

```python
def _with_listen_id(df):
    """Attach the listen_id. A playlist listen's page_number is the page its responses are on."""
    listens = spark.read.table("silver_listens").select("week", "page_number", "listen", "listen_id")
    return df.join(listens, ["week", "page_number", "listen"])
```

## The results

In dev, the pipeline produces:

| Table | Rows |
|---|---|
| `silver_meetings` | 159 |
| `silver_listens` | 165 (163 albums, 2 playlists) |
| `silver_ratings` | 1,045 |
| `silver_favorites` | 3,936 |
| `silver_attendance` | 1,168 (1,013 present, 155 absent) |
| `silver_review` | 0 |

`silver_review` is empty because every flagged item now has an answer in git. The last two were
W114's date and W129, *A Christmas Gift For You From Phil Spector*, whose title has no artist.
W129 has an entry in a `listen_overrides` file that gives the artist as "Phil Spector / Various
Artists".

Here are the highest-rated albums with at least five ratings:

```sql
SELECT l.artist, l.album, round(avg(r.rating), 2) AS avg_rating, count(r.rating) AS ratings
FROM silver_listens l JOIN silver_ratings r USING (listen_id)
GROUP BY ALL HAVING ratings >= 5
ORDER BY avg_rating DESC
LIMIT 3
```

| artist | album | avg_rating | ratings |
|---|---|---|---|
| Eagles | Hotel California | 8.34 | 7 |
| Fleetwood Mac | Rumours | 8.31 | 7 |
| The Who | Who's Next | 8.17 | 7 |

## Left for post 5

Two of the data decisions need more than the pages. The *1001 Albums* list is a seed in git, but
matching albums to it by name misses too many over spelling ("Alanis Morrisette", "Red Hot Chilli
Peppers"), and the `#N` in the early titles comes from a different edition than the seed. In post
5 each album will have Spotify and MusicBrainz names to match on, so I moved the matching there.

Track counts wait too. "Likes the rest" can't become a list of tracks until I know how many
tracks the version we played had.

## Prod

Fast-forwarding `v2-from-scratch` ran the checks and the deploy:

```text
Files: 39 uploaded, 0 deleted
Resources: 0 created, 0 changed, 0 deleted, 7 unchanged
```

No resources changed. The pipeline already picks up everything in `src/pipeline/`, so
`silver.py` is just another file in the upload, and so are the seeds. The export from part 3 was
already in the prod volume, so there was nothing to upload by hand either. I ran the prod
pipeline:

```bash
databricks bundle run -t prod album_club_pipeline
```

Prod has the same row counts as dev in every silver table, and `silver_review` is empty there
too. The `account users` grant from part 3 covers new tables in the schema, so I could query them
without another grant.

## Next: enrichment

In post 5 I match each listen to Spotify and MusicBrainz: the edition we actually played, its
track list, and which tracks were on the original release. Then favorite track numbers can become
song names.
