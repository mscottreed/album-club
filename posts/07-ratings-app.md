# Album Club on Databricks, Part 7: An App for Entering Ratings

At every meeting each of us rates the album and names our favorite songs, and for five years
all of that went onto a OneNote page. We still use OneNote to run the meetings. But getting a
page into Databricks means exporting it from Windows OneNote, and I don't want to do that every
time we meet. So from now on I enter the ratings in an app, and the OneNote export stays a
one-time import of the history.

This post builds that app as a Databricks App, deployed by the bundle, and works out how what
it saves joins the OneNote history in silver.

The code for this post is tagged `post-07` in
[mscottreed/album-club](https://github.com/mscottreed/album-club).

## What the app does

I host the meetings, so the app is for me. I pick a meeting, or start a new one, and fill in a
table with a row for each member: their rating and their favorite songs. For a new meeting I
also enter the week, the date, the artist, album and year, and a Spotify link to the album we
played. Then I press Save.

A Databricks App is a web app that Databricks hosts for you inside the workspace, on serverless
compute. Only users of the workspace can open it, and they sign in the same way they sign in to
the workspace, so the app doesn't have to deal with logins. Apps can use Streamlit, Dash, Gradio,
Flask and a few other frameworks. I went with Streamlit, because its `st.data_editor` gives you an
editable table in one call:

```python
table = st.data_editor(
    pd.DataFrame([(n, *recorded.get(n, ("", ""))) for n in names], columns=["Name", "Rating", "Favorite songs"]),
    num_rows="dynamic",  # a guest can be added at the bottom
    hide_index=True,
    use_container_width=True,
)
```

Picking a meeting that's already recorded fills the table with what's there, so the app
doubles as a way to fix a rating.

## Where the ratings go

The roadmap had this down as a choice between a Delta table and Lakebase. Lakebase is Postgres
run by Databricks, made for apps that read and write one row at a time, and Free Edition gets
one. A Delta table is what every other table in this project is. But the tables in this schema
are managed by the pipeline, and only the pipeline can write to them, so the app would need a
table of its own and the pipeline would have to read it alongside the pages.

Part 6 already gave the pipeline a way in for ratings that aren't in the export. The Weezer and LCD
Soundsystem tables were copied out of OneNote as tab-separated text and dropped into the
`onenote_exports` volume, and the pipeline reads them with the same parser as the pages. The app
saves the same kind of file, so the parser and the rules from part 4 cover it with nothing new
to store them in. It also fits the medallion layout from part 3. The app only saves raw input
next to the export, Auto Loader turns it into bronze rows, and the pipeline stays the only thing
that writes tables.

The difference is that the app never overwrites anything. Each save is a new file with the time
in its name:

```text
ratings_tables/W159-20261112T043037283484.tsv
```

That means a week can have several files, so silver needs a rule for which row counts. For each
week, person and album, the row from the newest file wins, over older files and over the page:

```python
@dp.temporary_view(comment="Rows from ratings tables saved as files, pasted from OneNote or written by the app.")
def rating_rows():
    rows = (
        spark.read.table("bronze_pasted_ratings")
        .withColumn("week", F.regexp_extract("source_file", r"/W(\d{3})[^/]*\.tsv$", 1).cast("int"))
        .where("week IS NOT NULL")
        .select("week", "source_file_modified_at", F.explode(parse_pasted_table_udf("week", "text")).alias("r"))
    )
    newest = Window.partitionBy("week", _response_key(), "r.listen").orderBy(F.desc("source_file_modified_at"))
    return rows.withColumn("n", F.row_number().over(newest)).where("n = 1").select("week", "r")
```

It's per person, not per file, because of post 8. There, each member gets to fix their own
rating, which will be a file with one row in it. That row should replace their rating and leave
everyone else's alone.

A meeting that's newer than the export has no page at all, so the app saves the meeting too, as
a small JSON file in `meetings/`. Silver turns it into something shaped like a parsed page and
adds it to the real ones, and from there it goes through the same steps. Two things come from
the app instead of being worked out. The meeting date is the one I typed in, so it doesn't need
the Sunday rule from part 4. And the Spotify link picks the edition, the same way an
`album_overrides` entry does in part 5, so there's no search. An override in git still wins if
the link turns out to be wrong.

## Checking before saving

A rating the parser can't read would stop the pipeline, so the app checks every row with the
pipeline's own parser before it saves anything. That works because the app deploys the whole `src/` folder, the same one the
pipeline imports `album_club` from, so the shorthand rules from part 4 live in one place:

```python
for name, rating, favorites in rows:
    try:
        values = [value for value, _ in parse_rating(rating)]
    except ValueError:
        problems[name] = [f"can't read the rating {rating!r}"]
        continue
    if any(not 0 <= value <= 10 for value in values):
        problems[name] = ["the rating has to be between 0 and 10"]
        continue
    (response,) = parse_pasted_table(week, ratings_file([(name, rating, favorites)]), people, {})
    if response["review_reasons"]:
        problems[name] = response["review_reasons"]
```

Anything that would land in `silver_review`, like a name that isn't in the people seed, shows up
as a red error next to the name, and nothing is saved.

## The app as a bundle resource

The app is another resource in the bundle:

```yaml
resources:
  apps:
    ratings_app:
      name: album-club-ratings
      description: The host enters each meeting's album and ratings.
      source_code_path: ../src
      config:
        command: [streamlit, run, app/app.py]
        env:
          - name: ALBUM_CLUB_CATALOG
            value: ${var.catalog}
          - name: ALBUM_CLUB_SCHEMA
            value: ${resources.schemas.album_club.name}
          - name: ALBUM_CLUB_ADMIN
            value: ${var.admin_user}
          - name: DATABRICKS_WAREHOUSE_ID
            value_from: sql-warehouse
          - name: ALBUM_CLUB_REFRESH_JOB_ID
            value_from: refresh-job
```

Each app runs as its own service principal, and the `resources` list under the app is how you
give it access. Listing a volume or a table grants the service principal that permission, plus
the catalog and schema access to reach it, and `value_from` hands the resource's ID to the app
as an environment variable:

```yaml
      resources:
        - name: refresh-job
          job:
            id: ${resources.jobs.album_club_refresh.id}
            permission: CAN_MANAGE_RUN
        - name: sql-warehouse
          sql_warehouse:
            id: ${var.warehouse_id}
            permission: CAN_USE
        - name: onenote-exports
          uc_securable:
            securable_type: VOLUME
            securable_full_name: ${var.catalog}.${resources.schemas.album_club.name}.${resources.volumes.onenote_exports.name}
            permission: WRITE_VOLUME
        - name: dim-album
          uc_securable:
            securable_type: TABLE
            securable_full_name: ${var.catalog}.${resources.schemas.album_club.name}.dim_album
            permission: SELECT
        - name: silver-ratings
          uc_securable:
            securable_type: TABLE
            securable_full_name: ${var.catalog}.${resources.schemas.album_club.name}.silver_ratings
            permission: SELECT
```

So the app can use the exports volume, read the two tables it shows, and start the refresh job,
and it has no access to any other table in the schema.

An app can work with data in two ways. With app authorization, everything it does runs as its
service principal, which is what this app does. With user authorization, Databricks also hands
the app a token for the signed-in person, and the app can query as them, limited by their own
grants. The club members have no grants at all (part 6), so app authorization is the one that
works here. It's also what will let them fix their own ratings in post 8 without getting any
access to the data.

The service principal's credentials reach the app as environment variables that Databricks
sets. The Databricks SDK finds them on its own, so `WorkspaceClient()` and `Config()` need no
arguments and there are no secrets in the code. `value_from` is only for the IDs of the
resources:

```python
config = Config()  # the app's service principal, from the environment


def save(path: str, text: str) -> None:
    WorkspaceClient().files.upload(f"{EXPORTS}/{path}", io.BytesIO(text.encode()), overwrite=False)
```

Deploying an app takes two steps. `databricks bundle deploy` uploads `src/` to the workspace
and creates or updates the app, and `databricks bundle run ratings_app` takes a snapshot of that
code, installs its packages and starts the app. A code change needs both, or the app keeps
running the old snapshot.

The packages come from the app's own folder, `src/`, not from the repo's `pyproject.toml`. If
there's a `requirements.txt` there, the app installs it with pip on Python 3.11, with some
libraries preinstalled. If there's a `pyproject.toml` and a `uv.lock` instead, it installs from
the lock file with uv, using the Python version from `requires-python`, with nothing
preinstalled. I wasn't going to argue for lock files in part 1 and then skip one here, so `src/`
has a small uv project of its own for the app:

```toml
[project]
name = "album_club_ratings_app"
version = "0.0.1"
requires-python = ">=3.12,<3.13"
dependencies = [
    "beautifulsoup4>=4.15.0",
    "databricks-sdk",
    "databricks-sql-connector",
    "pandas",
    "streamlit",
]

[tool.uv]
package = false
```

`uv lock --project src` writes `src/uv.lock`, and the app's build log shows it being used:

```text
[BUILD] Using CPython 3.12.15
[BUILD] [INFO] Dependencies installed successfully with uv.
```

So the app runs on the same Python 3.12 as the jobs and the pipeline, with every package pinned.
Since nothing is preinstalled, the list has everything the code imports, including `pandas`,
which `app.py` uses directly.

One gotcha: development mode prefixes jobs and pipelines with `[dev scott]`, but it doesn't
prefix app names. My first dev deploy created an app called `album-club-ratings`, which is the
name prod wants. The dev target now sets its own name:

```yaml
targets:
  dev:
    resources:
      apps:
        ratings_app:
          name: dev-${workspace.current_user.short_name}-album-club-ratings
```

## Only the host

Everyone in the workspace can open the app, since post 8 is going to need that. Databricks puts
the signed-in user's email in the `X-Forwarded-Email` header of every request, so the app checks
it against `admin_user`, the same variable that holds the schema grant:

```python
email = st.context.headers.get("X-Forwarded-Email", "")
if email.lower() != ADMIN.lower():
    st.info("Entering a meeting is for the host.")
    st.stop()
```

Anyone else gets that one line.

## Saving starts the refresh

My first version saved the files and told me the ratings would show up after the next run of
`album_club_refresh`. That's a second step I'd have to remember. Now Save starts the job:

```python
save(file_name("ratings_tables", week, "tsv", now), ratings_file(rows))
WorkspaceClient().jobs.run_now(job_id=int(os.environ["ALBUM_CLUB_REFRESH_JOB_ID"]))
st.success(f"Saved W{week:03d} and started album_club_refresh. It will be on the dashboard in about 10 minutes.")
```

That's what the `refresh-job` resource with `CAN_MANAGE_RUN` is for. In prod the job runs one
at a time with queueing on, so if I save twice in a row the second run waits for the first, and
it picks up both saves.

## Kind of Blue

Miles Davis' *Kind of Blue* (W159) was the album I left out of part 6, since we rated it after I
started this project. It has a OneNote page, so I only needed the ratings part of the app. I
entered them in dev, and after the refresh W159 had 5 ratings and 11 favorite-song picks.
`fact_ratings` went from 1,041 rows to 1,046 and the unrated albums from 110 to 105, so W159 was
the only thing that changed. Nothing landed in `silver_review`.

One Free Edition limit showed up while I was testing. An app stops 24 hours after it starts, and
when I went back to it the next day I got "App Not Available". `databricks bundle run
ratings_app` starts it again. For now that means starting it before a meeting; part 9 is about
running things on a schedule, and starting the app can go there too.

## Prod

<!-- TODO: the CI deploy output, starting the prod app, and entering W159 there. -->

## Next: members fixing their own ratings

In post 8 each member gets to open the app and correct their own rating and favorite songs,
using the signed-in email to work out who they are. Their fix is a one-row file, and the
newest-row rule from this post makes it replace only their row.
