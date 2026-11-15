# Album Club on Databricks, Part 7: An App for Entering Ratings

At every meeting each of us rates the album and names our favorite songs, and for five years
all of that went onto a OneNote page. We still use OneNote to run the meetings. But getting a
page into Databricks means exporting it from Windows OneNote, and I don't want to do that every
time we meet. So from now on I enter the ratings in an app, and the OneNote export stays a
one-time import of the history.

This post builds that app as a Databricks App, deployed by the bundle, and works out how the
ratings it saves join the OneNote history in silver.

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

The app never overwrites a file, though. Each save is a new file with the time in its name:

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
adds it to the real ones, and from there it goes through the same steps. The meeting date is the
one I typed in, so it doesn't need the Sunday rule from part 4. The Spotify link picks the
edition, the same way an `album_overrides` entry does in part 5, so there's no search. An
override in git still wins if the link turns out to be wrong.

## Checking before saving

A rating the parser can't read would stop the pipeline, so the app checks every row with the
pipeline's own parser before it saves anything. That works because the app deploys the whole
`src/` folder, the same one the pipeline imports `album_club` from, so the shorthand rules from
part 4 live in one place:

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

It lives in `resources/album_club.app.yml`, next to the bundle's other resources:

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

The app gets the exports volume, the two tables it shows and the refresh job, and no other table
in the schema.

Everything the app does runs as that service principal. Databricks calls this app
authorization. Apps can also use user authorization, where Databricks hands the app a token for
the signed-in person and the app queries as them, limited by their own grants. The club members
have no grants at all (part 6), so app authorization is the one that works here. It's also what
will let them fix their own ratings in post 8 without getting any access to the data.

The service principal's credentials reach the app as environment variables too, separate from
the `value_from` ones. The Databricks SDK finds them on its own, so `WorkspaceClient()` and
`Config()` need no arguments and there are no secrets in the code:

```python
config = Config()  # the app's service principal, from the environment


def save(path: str, text: str) -> None:
    WorkspaceClient().files.upload(f"{EXPORTS}/{path}", io.BytesIO(text.encode()), overwrite=False)
```

`databricks bundle deploy` uploads `src/` to the workspace and creates or updates the app, and
`databricks bundle run ratings_app` takes a snapshot of that code, installs its packages and
starts the app. Until the second command runs, the app keeps running the old snapshot.

The packages come from the app's own folder, `src/`, not from the repo's `pyproject.toml`. If
there's a `requirements.txt` there, the app installs it with pip on Python 3.11, with some
libraries preinstalled. If there's a `pyproject.toml` and a `uv.lock` instead, it installs from
the lock file with uv, using the Python version from `requires-python`, with nothing
preinstalled. Part 1 is where I picked uv for its lock file, so `src/` has a small uv project of
its own for the app:

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

The app now runs on the same Python 3.12 as the jobs and the pipeline, with every package
pinned. Nothing is preinstalled, so the list has everything the code imports, including
`pandas`, which `app.py` uses directly.

Development mode prefixes jobs and pipelines with `[dev scott]`, but it leaves app names alone,
so my first dev deploy created an app called `album-club-ratings`, the name prod wants. The dev
target now sets its own name:

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
the only thing that changed. The parser didn't flag any of the new rows, so `silver_review`
stayed empty.

On Free Edition an app stops 24 hours after it starts, and when I went back to the dev app the
next day I got "App Not Available". Starting it again brings back the last deployment, so for
now I start it before a meeting. Part 9 is about running things on a schedule, and starting the
app can go there too.

## Prod

Fast-forwarding `v2-from-scratch` ran the checks and then the deploy, and the deploy failed on
the app:

```text
Error: cannot create resources.apps.ratings_app: Cannot add volume "workspace.album_club.onenote_exports": all account users lack USE CATALOG permission on catalog "workspace", and the user does not have MANAGE permission on the catalog to grant it to the app's service principal. (403 PERMISSION_DENIED)
```

Every app gets a new service principal, and to reach a volume it needs `USE CATALOG` on the
catalog. The bundle grants that only if every account user already has it, or if whoever is
deploying has `MANAGE` on the catalog. In dev the deploy runs as me, and I'm an admin. In prod it
runs as the CI service principal, which only has the `CREATE SCHEMA` grant from part 3.

`USE CATALOG` on its own doesn't show anyone any data. It only lets them look inside the catalog,
and the club members already had it through the workspace's users group. So I granted it to
`account users`, another one-time command like the one in part 3:

```bash
databricks grants update catalog workspace --json '{"changes": [{"principal": "account users", "add": ["USE_CATALOG"]}]}'
```

Re-running the failed deploy created the app:

```text
Created apps.ratings_app
Created apps.ratings_app.permissions
Files: 60 uploaded, 0 deleted
Resources: 2 created, 0 changed, 0 deleted, 14 unchanged
```

Creating an app doesn't start it. `databricks bundle run ratings_app -t prod` would, but from my
laptop it hits the same `run_as` check as the dashboard did in part 6:

```text
Error: apps do not support a setting a run_as user that is different from the owner.
```

For the first prod start I used the apps commands directly. `databricks apps start` starts
the app's compute, and `databricks apps deploy` takes the snapshot of the code CI uploaded. The
start command and environment variables in the bundle don't come along on their own when you
deploy this way, and my first try ran the wrong thing and crashed. The second passed them in a
JSON file with the same command and variables as the bundle:

```bash
databricks apps start album-club-ratings
databricks apps deploy album-club-ratings --json @app-deploy.json
```

After that, starting the app is just `databricks apps start album-club-ratings`, which brings
back the last deployment with its settings.

Then I entered *Kind of Blue* in the prod app. Saving started `album_club_refresh`, which took
11.6 minutes, and prod came out the same as dev. W159 has 5 ratings and 11 favorite-song picks,
`fact_ratings` has 1,046 rows, there are 105 unrated albums, and `silver_review` is still empty.

## Next: members fixing their own ratings

In post 8 each member gets to open the app and correct their own rating and favorite songs,
using the signed-in email to work out who they are. Their fix is a one-row file, and the
newest-row rule from this post makes it replace only their row.
