# Album Club on Databricks, Part 5: Enrichment

In [part 4](04-parsing-into-silver.md) I parsed the OneNote pages into silver tables, but those
tables only know what I typed on the page. An album is an artist and a title, and a
favorite song is a track number. In this post I match every album to Spotify, for the edition we
played, and to MusicBrainz, for the original release. With both, track numbers become song titles
and bonus tracks get flagged. "Likes the rest" turns into a list of tracks, and albums get matched
to the *1001 Albums* list.

The code for this post is tagged `post-05` in
[mscottreed/album-club](https://github.com/mscottreed/album-club).

## Two APIs

We listen to every album on Spotify. When someone writes down track 7 as a favorite, they mean
track 7 of the Spotify edition we played, which isn't always track 7 of the original LP. The
pipeline uses the Web API with an app's client credentials,
which covers search and catalog lookups without anyone signing in. Apps in Development Mode have
had tighter limits since February 2026:

- Search returns at most 10 results.
- The batch lookups for albums and tracks are gone, so it's one request per album.
- A playlist's tracks only come back for the playlist's owner, not for client credentials.
- The app's owner needs Spotify Premium.

MusicBrainz is an open music database. Its release groups tie every edition of an album together,
so I use it to find the original release and its track list, and its genre tags. It needs no key,
but it asks for one request per second and a User-Agent that says who you are:

```python
# MusicBrainz asks every client to identify itself with a name and a way to get in touch.
USER_AGENT = "album-club/0.1 ( https://github.com/mscottreed/album-club )"
```

Both clients are in `src/album_club/apis.py`, using only the standard library. They retry on 429
and 5xx responses and on dropped connections, and MusicBrainz requests are spaced 1.1 seconds
apart.

## The secret scope

The Spotify client ID and secret are the first secrets in the project. The roadmap's one
exception to "everything is in git" is secret values, so the scope is a bundle resource and the
values go in by hand:

```yaml
secret_scopes:
  album_club_secrets:
    # Development mode doesn't prefix scope names, so dev sets its own name (databricks.yml).
    # The scope is defined here; the values aren't: see docs/secrets.md.
    name: ${var.secret_scope}
```

The variable defaults to `album_club`, and the dev target sets
`dev_${workspace.current_user.short_name}_album_club`, so dev and prod get separate scopes.
[docs/secrets.md](../docs/secrets.md) has the steps for creating the Spotify app and the
`databricks secrets put-secret` commands, which prompt for the value so it doesn't land in shell
history.

## Fetch, then match

Fetching is slow. At one request per second, MusicBrainz alone takes more than ten minutes for
the club's whole history, so each response should be fetched once and kept, and later runs
should only call the APIs for listens they haven't seen. That means the fetch has to read what it
saved last time, and a pipeline's tables can't read their own earlier output. My first try
checked for the saved responses by table name, and the pipeline blocked that too:

```text
py4j.security.Py4JSecurityException: Method public boolean
org.apache.spark.sql.classic.Catalog.tableExists(java.lang.String) is not whitelisted
```

So the fetching moved into a job task between two pipeline updates. The raw responses land as
files in a volume and Auto Loader loads them into bronze, the same way the OneNote export came in
for part 3. If an API is down, only the fetch fails. If I change a matching rule, the pipeline
reruns against the saved responses without calling either API. The job has three tasks:

```yaml
jobs:
  album_club_refresh:
    name: album_club_refresh
    tasks:
      - task_key: parse_pages
        pipeline_task:
          pipeline_id: ${resources.pipelines.album_club_pipeline.id}
      - task_key: fetch_enrichment
        depends_on:
          - task_key: parse_pages
        spark_python_task:
          python_file: ../src/fetch_enrichment.py
          parameters:
            - --catalog=${var.catalog}
            - --schema=${resources.schemas.album_club.name}
            - --responses-path=/Volumes/${var.catalog}/${resources.schemas.album_club.name}/${resources.volumes.api_responses.name}
            - --secret-scope=${resources.secret_scopes.album_club_secrets.name}
        environment_key: serverless
      - task_key: match
        depends_on:
          - task_key: fetch_enrichment
        pipeline_task:
          pipeline_id: ${resources.pipelines.album_club_pipeline.id}
```

The first update builds `silver_listens`. `fetch_enrichment.py` reads it, calls the APIs for
anything it hasn't asked before, and writes the raw responses as JSON lines to a new
`api_responses` volume. The second update loads those files into `bronze_api_responses`, one row
per request, and does the matching from there.

Both sides run the same function. `crawl()` takes a listen and a `get(source, request)` function
and makes every request that listen needs:

```python
def crawl(listen: dict, override: dict, get: Get) -> dict:
    """Every API response enrichment needs for one album listen."""
```

In the job, `get` calls the API and records the response. In the pipeline, it looks the request
up in bronze. Because the code is the same, the pipeline asks for exactly what the job fetched,
and the matching never touches the network. It also means I can rerun the matching locally
against a downloaded copy of the volume, which is how I checked every rule change below.

The matching runs as one `applyInPandas` group, since it needs every cached response at hand. It
failed on the workers at first with `No module named 'dbruntime'`. The function referenced
`apis.spotify_search` and the like through the module object, so cloudpickle pickled the module
itself, and a module loaded from workspace files carries Databricks' import hook, which the
workers can't load. Importing the names fixed it:

```python
# Import names, not the module: a UDF that references a module object pickles the module itself,
# and a workspace-file module carries Databricks' import hook, which workers can't load.
from album_club.apis import (
    MUSICBRAINZ,
    SPOTIFY,
    ...
)
```

## Finding the edition we played

64 meeting pages link to a Spotify track, and a linked track's album is the edition we played,
when it's the right album. Most links turned out to point to other songs, so only 5 albums come
from a track link. The rest come from search.

Spotify's search has field filters, like `album:"..." artist:"..."`, but with quotes and
punctuation they found nothing for many albums. The search sends plain words instead,
then tries again without the artist in case it's misspelled on the page.

A search result counts as the album when its artist and title are at least 85% similar to the
page's, after ignoring case, punctuation, a leading "The", "&" vs "and", and edition suffixes like
"(Remastered)". Three more rules came from results that were wrong:

- A name found whole inside the other counts. "James Brown" matches "James Brown & The Famous
  Flames". An album title needs at least two words for this, so "Live" doesn't match "Live at
  Leeds".
- A self-titled album has to be close, not just contained. Otherwise *Ray Charles* matched *The
  Genius of Ray Charles*.
- Names with different Roman numerals or volume numbers never match. "Van Halen" and "Van Halen
  II" are 86% similar, and *Van Halen* was getting *Van Halen II*.

That leaves several editions for most albums. The search keeps the ones where the most favorite
and excluded track numbers exist, and those usually all fit. Among them, the winner is the track
list most like the original release's, measured as the titles they share divided by all the
titles across both lists. For *Rubber Soul*, five editions fit every number, and Spotify lists the 68-track
Super Deluxe first. It has all the original's titles and 54 more tracks, so the 14-track 2009
remaster wins. When no titles match, as with live albums whose Spotify titles all end in "- Live
At The Apollo Theater, 1962", the track count closest to the original's decides.

## Overrides

Some pages use shorthand that no matching rule can read. Led Zeppelin's albums are written *I*,
*II* and *IV*, Frank Ocean's is *Blond/Blonde* (Blond on the cover, Blonde on Spotify), and Lana
Del Rey's is *NFR*. Those go in an `album_overrides` seed with the name to search for. For each of them I also looked up the
edition on Spotify and pinned it:

```csv
week,album,spotify_album_id,musicbrainz_release_group_id,year,note
51,Led Zeppelin,1J8QW9qsMLx3staWaHpQmU,,,"page says ""I"""
62,Led Zeppelin IV,5EyIDBAqhnlkAHqvPRwdbX,,,"page says ""IV"""
...
158,Norman Fucking Rockwell!,5XpEKORZ4y6OrCZSKsi46A,,,"page says ""NFR"""
```

The page's name stays as written in `silver_listens`. The override only changes what gets
searched for and matched.

Three more came from albums whose MusicBrainz year was more than a year off the page's, which
the pipeline flags for review. MusicBrainz has so many compilations called *The Beatles* that the
White Album didn't make the search's top 10, so its override pins the 1968 release group.
*Live at the Apollo* matched a 2005 reissue, and its override pins the 1963 King LP.

The third was Billie Holiday's self-titled album, where MusicBrainz says 1947 and the page says
1957. Both are right. Commodore released it in 1947 as an album of 78s with eight songs. The
edition we played is the 1957 Verve LP, which has those eight and four more. I count it as 1957,
so the override has a `year` column too.

## The original release and bonus tracks

A MusicBrainz release group holds every release of an album, from the original LP to each
country's pressings and every later CD and remaster. Picking the original from those took two
tries. The earliest official release is often an odd one. For *Rubber Soul* it was a 12-track
Canadian LP dated "1965", which sorts before the UK's "1965-12-03". The rule now is the earliest
release with the track count most of the group's releases share, with a year-only date sorting
after full dates in that year. Most *Rubber Soul* releases have the UK's 14 tracks.

Neither rule is right every time. Plain "earliest" picks the US soundtrack version of *A Hard
Day's Night*, which is missing five of the UK album's songs. The track-count rule goes wrong on
newer albums whose expanded reissues outnumber the original: most *Night Visions* releases have
20 tracks, so the 16-track Deluxe edition looked closest. Across all 163 albums the track-count
rule left fewer albums with flagged tracks, 33 against 40, so I kept it and pinned *Night
Visions* in the overrides. *Superunknown* is pinned too, after a 17-track Commentary edition
edged out the 15-track one by 0.941 to 0.938.

A track on the Spotify edition is on the original album when its title is at least 85% similar
to one of the original's titles, or one title is found whole inside the other, ignoring suffixes.
So "To Zion (feat. Carlos Santana)" is "To Zion", and "Steamroller Blues" is "Steamroller". Each
original track matches once,
in the edition's order, so on *Rumours (Super Deluxe)* the album tracks come first and the later
demos and live versions of the same songs are bonus tracks.

MusicBrainz's own titles still beat this in two places. The release it picks for *(What's the
Story) Morning Glory?* calls two short fragments "[untitled]", and the one for *Norman Fucking
Rockwell!* has censored titles.

## Tracks and picks

Two silver tables come out of the matching. `silver_tracks` has every track of every listen,
from the album's Spotify edition or from the playlist. `silver_track_picks` has a row for each
track a person mentioned, whether they ranked it or didn't like it. When someone likes the rest,
it also gets a row for every track they didn't mention.

A track number without a disc counts through the whole edition, and with a disc (`Disc 2: 3`) it
counts within that disc. An expectation checks that every number exists on the chosen edition.
Three cells had a favorite of `0`, which means no favorites, so `0` now parses as none.

Spotify won't return a playlist's tracks to an app that doesn't own the playlist, so the APIs
couldn't help with our two playlists. Their track lists are in a `playlist_tracks` seed instead.
*Superunknown*'s came from its OneNote page, and *Mellower Chains* from the playlist's public
Spotify embed page.

## The results

In dev:

| Table | Rows |
|---|---|
| `silver_album_matches` | 163 |
| `silver_tracks` | 2,176 |
| `silver_track_picks` | 4,521 (3,933 ranked, 522 from "likes the rest", 66 not liked) |
| `silver_review` | 0 |

All 163 albums have a Spotify edition and a MusicBrainz album. 148 editions came from search,
10 from overrides and 5 from track links, and 122 albums matched the *1001 Albums* list. 200
tracks across 30 albums aren't on the original release.

47 of the ranked favorites are bonus tracks. These are the ones picked most:

```sql
SELECT l.album, p.title, count(*) AS people
FROM silver_track_picks p JOIN silver_listens l USING (listen_id)
WHERE p.rank IS NOT NULL AND NOT p.on_original_album
GROUP BY ALL
ORDER BY people DESC, l.album
LIMIT 5
```

| album | title | people |
|---|---|---|
| Billie Holiday | On The Sunny Side Of The Street | 6 |
| Sublime | Trenchtown Rock | 3 |
| Surrealistic Pillow | In The Morning | 3 |
| Absolution | Fury | 2 |
| At Last! | Spoonful - Single Version | 2 |

"On The Sunny Side Of The Street" is one of the four tracks the 1957 Verve LP added to Billie
Holiday's 1947 album.

All the rules are in [docs/data-decisions.md](../docs/data-decisions.md), and
`tests/test_enrichment.py` covers each one with made-up albums.

## Prod

Fast-forwarding `v2-from-scratch` ran the checks and the deploy:

```text
Created secret_scopes.album_club_secrets
Created volumes.api_responses
Created secret_scopes.album_club_secrets.permissions
Updated pipelines.album_club_pipeline
Created jobs.album_club_refresh
Created jobs.album_club_refresh.permissions
Files: 49 uploaded, 0 deleted
Resources: 5 created, 1 changed, 0 deleted, 6 unchanged
```

The pipeline changed because it has a new setting, the path to the `api_responses` volume. The
new scope was empty, so I put the Spotify client ID and secret in it with `databricks secrets
put-secret`, as [docs/secrets.md](../docs/secrets.md) describes, and ran the job:

```bash
databricks bundle run -t prod album_club_refresh
```

Prod started with no saved responses, so this run fetched all of them and took 35 minutes. Every
Spotify edition matched dev's. One album didn't: prod flagged W086, Ray Charles's self-titled
album, with "MusicBrainz says 1962, the page says 1957". MusicBrainz's search had returned
different results than it did for dev and matched a 1962 album with the same title. Dev's match
had no date or track list, so it was never flagged, but it was wrong too. The 1957 Atlantic LP is
in MusicBrainz, tagged as a compilation because it collects singles, so the search passes over
it. W086 now has an override pointing at that release group.

TODO: second prod run.

## Next: gold tables and a dashboard

In post 6 I build gold tables on top of silver (per-member stats, top-rated albums, trends over
time) and an AI/BI dashboard, deployed as a bundle resource like everything else.
