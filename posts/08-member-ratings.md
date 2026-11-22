# Album Club on Databricks, Part 8: Members Change Their Own Ratings

Each member of the club can now open the ratings app, sign in, and change their own rating and
favorite songs for any of the 160 meetings. They still don't get access to any of the data behind
it.

Every save is also a record of the change, with who made it, when, and the value before and
after. That's a history of every change, and it means I can copy a change from dev to prod and
have it land the same way.

The code for this post is tagged `post-08` in
[mscottreed/album-club](https://github.com/mscottreed/album-club).

## Who's signed in

Part 7 already reads the signed-in email from the `X-Forwarded-Email` header to check for the
host. For members the app needs to turn that email into a name from the people seed, like
`Bill L`. The emails aren't going in git, so they live in a `members.csv` file in the
`onenote_exports` volume, next to the raw export, and in my gitignored `data/` folder:

```text
email,person
someone@example.com,Bill L
```

The app reads it with the Files API, the same way it saves files. The parser keeps a line only
if the name on it is in the people seed, so a typo in the file can't let someone write a rating
for a name the pipeline doesn't know:

```python
def members_by_email(text: str, people: list[dict]) -> dict[str, str]:
    known = {p["person"] for p in people}
    return {
        row[0].strip().lower(): row[1].strip()
        for row in csv.reader(io.StringIO(text))
        if len(row) == 2 and row[1].strip() in known
    }
```

My first `members.csv` in prod had no header line, and the first version of this read it with
`csv.DictReader`, which would have treated a member's line as the header. Now the header is
optional, and since it isn't a known name either, it gets dropped with the rest of the lines
that don't match.

Then the app picks a page based on who you are:

```python
email = st.context.headers.get("X-Forwarded-Email", "").lower()
person = members().get(email)
host = email == ADMIN.lower()

if person and host:
    mine, meeting = st.tabs(["My ratings", "Enter a meeting"])
    ...
elif host:
    enter_meeting("host")
elif person:
    my_ratings(person)
else:
    st.info("The app doesn't know who you are yet. Ask the host to add your email.")
```

I'm a member and the host, so I get both pages as tabs. A member gets "My ratings": a meeting,
a Rating box and a Favorite songs box, filled in with what's recorded.

The app still runs as its service principal, with the grants from part 7. A member can change their
own row through the app. They still can't query a table or open the volume.

## Finding a meeting out of 160

With 160 meetings in the list, scrolling to the one you want takes a while. Streamlit's
`st.selectbox` has a `filter_mode` option, and `"contains"` matches anywhere in the label, so
typing "kind" finds "W159 Miles Davis - Kind of Blue":

```python
choice = st.selectbox("Meeting", labels, key="my_meeting", filter_mode="contains")
```

When I tried the page in dev, I changed my rating, saw "Press Enter to apply" under the box,
pressed Enter, and switched meetings. Nothing had been saved. In Streamlit, Enter in a text
input only updates the value, and the Save button is what saves it. Putting the boxes in an
`st.form` makes Enter submit the form, the same as pressing Save:

```python
with st.form(f"my_form_{week}"):
    rating = st.text_input("Rating", rating)
    favorites = st.text_input("Favorite songs", favorites)
    submitted = st.form_submit_button("Save", type="primary")
```

## A save is a change record

In part 7 a save wrote the whole ratings table for the week as a tab-separated file, and the newest
file's row won for each person. A one-row file would have worked for a member's change, but it
wouldn't say who saved it or what it replaced. Now each save writes a change record instead, as JSON
in a `rating_changes/` folder in the volume:

```json
{
  "week": 155,
  "saved_at": "2026-11-21T21:58:31.997289+00:00",
  "saved_by": "Scott R",
  "changes": [
    {
      "name": "Scott R",
      "from": {"rating": "8.4", "favorites": "2, 8, 3, 5, 10, 12, 13"},
      "to": {"rating": "8.45", "favorites": "2, 8, 3, 5, 10, 12, 13"}
    }
  ]
}
```

That's me changing Muse's *Absolution* from 8.4 to 8.45 in dev. `saved_by` is a name from the
seed, never an email. The file name has the same time in it,
`rating_changes/W155-20261121T215831997289.json`, and the app never overwrites a file.

The "My ratings" page and the host's grid both write change records now. The grid only writes the
rows that differ from what's recorded, so if I fix one person's favorites, the record has one change
in it. Saving without changing anything says "Nothing changed, so there's nothing to save" and
writes nothing.

### Why record changes

The simplest way to store an edit is to update it in place, so there's one current copy of each
rating, as a row in a Lakebase table or a file per week that the app rewrites. Reading it is easy,
but each save erases the one before. Nobody can see who changed a rating or what it used to be,
and moving a fix from dev to prod means copying the current state over, which wipes out anything
that changed in prod in the meantime.

Part 7's approach kept every save as its own file, so the history was there, but each file was a
snapshot of the whole week. To find out what changed, you'd have to compare it with the file
before it. A snapshot also writes every row that was on the page, changed or not. Say I have the
host grid open for a meeting, and while I'm fixing one person's favorites, Linda L changes her
rating on her own page. When I press Save, my snapshot still has her old rating in it, because
that's what the grid loaded, and since my file is newer, her old rating wins.

A change record holds only what the save changed, with who, when, and the value before and after.
In that same situation, my save only has the favorites I fixed, so Linda's new rating stays. The current state is every change
applied in `saved_at` order, so copying the files to another environment and running the pipeline
gets the same result, and copying them twice doesn't change it. And because each change says what
it expected to replace, the pipeline can tell when that wasn't the case.

This is also how changes usually get out of an OLTP database. The database behind an app updates
rows in place, and a pipeline that needs those changes doesn't re-read the whole table every time.
It reads change data capture (CDC), a feed with a row for each insert, update and delete, in order.
SQL Server's CDC writes change tables next to the source table, and Postgres exposes its
write-ahead log through logical decoding. A change event usually carries the row before and after
and when it happened, which isn't far from my `from`, `to` and `saved_at`.

Those changes often get published to Kafka, which keeps each topic as an append-only log, in order
within each partition. A consumer can go back to an earlier offset and read the changes again, the
way I can copy change files to prod and rerun the pipeline. A compacted topic keeps only the newest
event for each key, which is what `rating_rows` does for each week, person and album. On the
Databricks side, Lakeflow's AUTO CDC (`create_auto_cdc_flow`) applies a feed like that to a table and uses a
`sequence_by` column to put the changes in order, the job `saved_at` does here. The ratings app
has no database in the middle, so it writes the feed itself.

## Silver reads a change like a pasted table

The app's files don't get a parser of their own. Silver turns each change into a one-row ratings
table, header and all, and sends it through `parse_pasted_table`, the function that already reads
the tables I copied out of OneNote in part 6:

```python
F.concat(
    F.lit("\t".join(HEADER) + "\n"),
    F.concat_ws("\t", "change.name", "change.to.rating", "change.to.favorites"),
    F.lit("\n"),
).alias("text")
```

Those rows go into the same view as the pasted tables, `file_rows`, and the newest row for each
week, person and album still wins. Part 7 judged "newest" by when the file was written, but copying
a file from dev to prod writes it again, so a copied change would look newer than anything saved in
prod before it. Now each row is ordered by when it was saved. For a change record that's `saved_at`
from inside the file. For a TSV it's the time in the file name, or the file's modified time for the
ones I named by hand, like `W153.tsv`:

```python
def _saved_at():
    stamp = F.regexp_extract("source_file", r"-(\d{8}T\d{12})\.\w+$", 1)
    return F.when(stamp != "", F.to_timestamp(F.concat(stamp, F.lit("Z")), "yyyyMMdd'T'HHmmssSSSSSSX")).otherwise(
        F.col("source_file_modified_at")
    )
```

A copied change keeps its place in line, and copying it a second time doesn't change anything.

Every change also goes into a new table, `silver_rating_changes`, with the week, who saved it, when,
and the rating and favorites before and after.

The `from` values are there for one more check. If a change's `from` doesn't match the row it
replaced, silver still applies the change, because it's newer, and adds a line to
`silver_review`. Say my Muse rating had been changed to 8.3 in prod before I made the change in
dev, and then I copied the dev change over. The line would look like this:

```text
change saved 2026-11-21 21:58:31.997289 by Scott R expected '8.4' / '2, 8, 3, 5, 10, 12, 13' but it was '8.3' / '2, 8, 3, 5, 10, 12, 13'
```

The dev change still wins, since it was saved later, and I get told to look at it.

The app needs the same ordering. A member who saves twice before the refresh finishes should see
their second change starting from their first one, so the app reads the saved change records for the
week and applies them on top of what's in silver, in `saved_at` order:

```python
def apply_changes(current, change_files):
    current = dict(current)
    for record in sorted((json.loads(text) for text in change_files), key=lambda r: r["saved_at"]):
        for change in record["changes"]:
            current[change["name"]] = (change["to"]["rating"], change["to"]["favorites"])
    return current
```

## The bundle sets permissions back

After a deploy, the dev app failed with this:

```text
PermissionDenied: Principal 'app-3pai1y dev-scott-album-club-ratings' does not have USE SCHEMA on Schema 'workspace.dev_scott_album_club'.
```

When the bundle creates the app, the app's `resources` give its service principal `USE SCHEMA`
on the schema, as part 7 describes. But the schema has its own `grants` list in the bundle, and
every deploy sets the schema's grants back to that list. The list only had me on it, so each
deploy took the app's grant away. I checked by granting it again by hand and deploying, and it
was gone again.

The fix is to put the app's service principal on the list. The bundle knows its ID, so the YAML
can refer to it:

```yaml
grants:
  - principal: ${var.admin_user}
    privileges: [USE_SCHEMA, SELECT, READ_VOLUME, WRITE_VOLUME]
  # The ratings app's resources grant its service principal USE_SCHEMA here, but each deploy
  # sets this schema's grants back to this list and drops it, so it has to be listed too.
  - principal: ${resources.apps.ratings_app.service_principal_client_id}
    privileges: [USE_SCHEMA]
```

Deploys now report nothing changed on the schema, and the grant stays put in dev and prod.

The refresh job had the same problem, only in prod. My first save in the prod app wrote its change
record and then failed to start the refresh:

```text
PermissionDenied: User 12400c6e-cc24-4344-9c68-edda8f97b9d4 does not have Manage Run or Owner or Admin permissions on job 1096495008725221.
```

The prod target in `databricks.yml` has a `permissions` list that gives the CI service principal
and the admins `CAN_MANAGE` on everything the bundle deploys. That includes the job, so each prod
deploy set the job's permissions back to that list and dropped the `CAN_MANAGE_RUN` the app's
`resources` had given it. Dev has no list like that, which is why the dev app could still start
its job. The fix is the same as for the schema. The job lists the app's service principal itself:

```yaml
album_club_refresh:
  name: album_club_refresh
  permissions:
    - service_principal_name: ${resources.apps.ratings_app.service_principal_client_id}
      level: CAN_MANAGE_RUN
```

## The security scan

Each app deployment gets a security scan, which is in Beta. It doesn't block the deploy, but the
findings are there to read through `databricks api get` on the deployment's `scanFindings`. It
flagged five queries that built SQL with f-strings, because they put the catalog and schema in
front of each table name. Those values come from the bundle's environment variables, not from
the person using the app, but the connector can set the catalog and schema on the connection, so
the queries don't need them at all:

```python
sql.connect(
    server_hostname=config.host,
    http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
    credentials_provider=lambda: config.authenticate,
    catalog=CATALOG,
    schema=SCHEMA,
)
```

Now every query is a plain string with parameters:

```python
query("SELECT person, rating_raw, favorites_raw FROM silver_ratings WHERE week = :week", {"week": week})
```

## The dashboard kept its old numbers

After my first changes in prod, the refresh finished and every table had the new ratings, but the
dashboard still showed the old ones. A dashboard published with embedded credentials, like the one
from part 6, keeps one cache of query results that all viewers share. The cache can be up to 24
hours old, and it stays that way when a pipeline refresh puts new data in the tables.

A job can refresh a dashboard, so `album_club_refresh` now ends with a step that reruns the
dashboard's queries:

```yaml
- task_key: refresh_dashboard
  depends_on:
    - task_key: match
  dashboard_task:
    dashboard_id: ${resources.dashboards.album_club_dashboard.id}
    warehouse_id: ${var.warehouse_id}
```

In dev it adds about 40 seconds to the refresh. Someone who already has the dashboard open still
has to reload the page to see the new numbers.

## Smaller fixes

The first time I pasted a column of ratings into the host's grid, Save failed with
`ValueError: too many values to unpack`. The cells I'd copied from OneNote had returns in them,
and a return inside a cell starts a new row in a tab-separated table. Now any run of whitespace
in a cell becomes a single space before it's saved.

Pasting a column also needs the grid's rows in the same order as the OneNote table, so the
members in the people seed are now in that order.

Some meetings had two albums, or an album and a playlist, and those ratings look like
`8.4 (A), 9.8 (P)`. The app handles one album per meeting, so it says so and won't save those
weeks.

And the dashboard's favorite tracks table has a Length column now. Spotify had been giving us
each track's `duration_ms` since part 5, but the dashboard's dataset didn't select it.

## Trying it

In dev, the Muse change above came through with my name and the save time in
`silver_rating_changes`, the rating is 8.45 everywhere downstream, and `silver_review` stayed
empty.

I'd entered the Beatles' *Sgt. Pepper's Lonely Hearts Club Band* (W160) in dev and never in
prod. Copying its files into the prod volume and running the refresh gave prod the same 7
ratings and 34 favorite-song picks as dev, with nothing to review.

After deploying this post's code to prod, the refresh left everything that was already there
alone. `silver_ratings` still has 1,068 rows with the same ratings, `fact_ratings` has 1,053,
and `silver_review` is empty. `silver_rating_changes` is there with nothing in it, waiting for
the first change in prod.

Then I went through my own ratings in prod and changed seven of them, two with new favorite songs
too. The first one hit the job permission error from earlier. Once that was fixed, all seven came
through in `silver_rating_changes` with my name and save times, and `silver_review` stayed empty,
so every `from` matched what prod had recorded.

After the permission fix I started a refresh by hand for the first change, and while it ran I saved
the other six. Each of those saves started a refresh of its own, and since prod runs the job one at
a time, all six queued up behind the one that was running. That run picked up all seven changes,
which left six full refreshes of about 11 minutes each with nothing new to do. Now the app checks for a run that's already queued before it starts one:

```python
runs = jobs.list_runs(job_id=job_id, active_only=True)
if any(r.state.life_cycle_state == RunLifeCycleState.QUEUED for r in runs):
    st.success(f"Saved W{week:03d}. A refresh is already waiting to start, and it will pick this up.")
    return
jobs.run_now(job_id=job_id)
```

A run that's already going might have read the volume before the save, so that case still starts
a new one, and it waits its turn. The same `try` catches a refresh that can't start, like the
permission error, and tells the member their rating is saved but will show up after the next
refresh, instead of showing them a traceback.

## Next: running things on a schedule

On Free Edition the app stops 24 hours after it starts, so I've been starting it by hand before
each meeting. Part 9 is about running things on a schedule, and starting the app is one of them.
