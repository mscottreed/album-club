# Album Club on Databricks, Part 3: From OneNote to Bronze

Every album club meeting since 2021 has a page in one OneNote section called Music: the album,
a table of everyone's ratings and favorite tracks, and whatever notes I pasted in about the band.
This post gets those pages into Databricks. By the end there's a bronze table with one row per
OneNote page, holding the page exactly as OneNote wrote it, and every piece of Databricks setup
behind it is in the bundle except for one grant.

The code for this post is tagged `post-03` in
[mscottreed/album-club](https://github.com/mscottreed/album-club).

## A one-time export

I'll keep running meetings in OneNote, but from post 7 on, the week's ratings go into a
Databricks app. So OneNote only matters for history, and the history only has to come in once.
There's no need for the Microsoft Graph API, an app registration or a scheduled sync when a
manual export does the job.

Unfortunately, there are many different versions of OneNote, and they differ in export functionality.  OneNote for Mac only saves pages as PDF.  A PDF turns
a ratings table into loosely positioned text. Windows OneNote has more options under
**File > Export**:

| Format | Verdict |
|---|---|
| OneNote Section (`.one`), OneNote Package (`.onepkg`) | Binary formats with only unofficial parsers |
| PDF | Tables become positioned text |
| Word (`.docx`) | Readable, but harder to tell where one page ends and the next starts |
| Single File Web Page (`.mht`) | HTML, with real tables and links |

Luckily, I have both Mac and Windows laptops, and so I exported the whole Music section from Windows as a Single File Web Page. It came out as one 103 MB `.mht` file.

An `.mht` file is a MIME archive, the same format as an email with attachments. This one has a
single HTML document (12 MB) holding every page, plus 623 images: album covers, band photos and
anything else I pasted in. In the HTML, each OneNote page is a top-level `<div>`, and its header
holds the page title, then the date and time the page was created.

### Raw data stays out of git

The export has everyone's full names and ratings, and the repo is public. The rule from part 1
is that everything in Databricks comes from git, but that's about code and configuration. The
data itself goes in the other direction. The export lives in two places only: a gitignored
`data/` folder on my laptop, and a Unity Catalog volume.

```gitignore
# Raw album club data (names, ratings) never goes in git
data/
```

## Bronze, silver and gold

Databricks recommends a layout it calls the medallion architecture, and this series follows it.
Data moves through three layers of tables, and each layer is built from the one before:

- Bronze is the data as it arrived, with nothing fixed or interpreted.
- Silver is cleaned up and typed, with one row per meeting, per rating and per favorite song, and
  the rules about what the pages mean applied.
- Gold is shaped for whatever uses it, which for the club is a dashboard.

Databricks doesn't enforce any of this. The layers are just tables, and here they all live in one
schema, with `bronze_` and `silver_` in the names of the first two. Some projects give each layer
its own schema, but one schema is plenty for an album club.

The rule for bronze is to keep it raw. This post's bronze table holds each page's HTML exactly as
OneNote wrote it. Even the created date stays as the text on the page, because turning it into a
date is a decision, and decisions belong in silver.

That's what lets everything after bronze change. If I decide a favorites cell means something
different, the fix is new code and another pipeline run over bronze, not another export from
Windows OneNote. The export only has to happen once because bronze keeps all of it.

## Planning before building

Before writing the pipeline, I profiled the export to see what parsing it would take. There are
272 pages, 163 of them meeting pages, and the data has the usual surprises. OneNote records when
a page was created, which isn't the meeting date. Some meetings covered three albums, some
people rated a playlist instead of the album, and favorite songs are shorthand like
`2, 3, 1, ... (x5, 9, 10)`, which means "these three in order, I liked everything else except
5, 9 and 10."

None of that changes the bronze table, which keeps every page as it is. But it decides what
bronze has to keep, and it's what post 4 has to parse, so the rules are written down in
[docs/data-decisions.md](../docs/data-decisions.md) before any parsing code exists.

## The bundle resources

Three new resources go in `resources/album_club.yml`: a schema, a volume for the exports, and a
pipeline.

```yaml
resources:
  schemas:
    album_club:
      catalog_name: ${var.catalog}
      name: album_club
      comment: Album club data, from raw OneNote exports to gold tables.

  volumes:
    onenote_exports:
      catalog_name: ${var.catalog}
      schema_name: ${resources.schemas.album_club.name}
      name: onenote_exports
      comment: OneNote sections exported from Windows OneNote as Single File Web Page (.mht).

  pipelines:
    album_club_pipeline:
      name: album_club
      catalog: ${var.catalog}
      schema: ${resources.schemas.album_club.name}
      serverless: true
      # src/ is on the pipeline's Python path, so transformations can import album_club.
      root_path: ../src
      libraries:
        - glob:
            include: ../src/pipeline/**
      environment:
        dependencies:
          - beautifulsoup4>=4.15.0
      configuration:
        album_club.onenote_exports_path: /Volumes/${var.catalog}/${resources.schemas.album_club.name}/${resources.volumes.onenote_exports.name}
```

A **volume** is Unity Catalog's place for files rather than tables. It's governed like a table,
and you address it with a path like `/Volumes/<catalog>/<schema>/<volume>`.

The pipeline's key is `album_club_pipeline`, not `album_club`. I first gave the schema and the
pipeline the same key, and validate refused:

```text
Error: multiple resources or scripts have been defined with the same key: album_club
  at resources.pipelines.album_club
     resources.schemas.album_club
```

Resource keys have to be unique across all resource types, not just within one. The pipeline's
`name` can still be `album_club`.

### Dev and prod schemas

In part 1, `bundle init` added a `schema` variable to `databricks.yml`: my username in dev,
`prod` in prod. I removed it, because the schema is now a resource and development mode already
keeps dev and prod apart. In dev, the bundle prefixes Unity Catalog resources the same way it
prefixes jobs, so the schema is `dev_scott_album_club` in dev and `album_club` in prod. Both live
in the `workspace` catalog, the only one Free Edition has.

The volume and the pipeline refer to the schema as `${resources.schemas.album_club.name}`
rather than repeating `album_club`, so they pick up the prefix too.

## Splitting the file into pages

The splitting is plain Python in `src/album_club/onenote_export.py`, so it can be tested on my
laptop without Databricks:

```python
PAGE_DIV_STYLE = "direction:ltr;border-width:100%"


def split_mht(content: bytes) -> list[dict]:
    """Return one dict per page: page_number (1-based), title, created (as OneNote wrote it), html."""
    message = email.message_from_bytes(content, policy=policy.default)
    html = next(part for part in message.walk() if part.get_content_type() == "text/html").get_content()
    soup = BeautifulSoup(html, "html.parser")

    pages = []
    for div in soup.body.find_all("div", recursive=False):
        if div.get("style") != PAGE_DIV_STYLE:
            continue
        title_div, created_div = div.div.find_all("div", recursive=False)[:2]
        pages.append(
            {
                "page_number": len(pages) + 1,
                "title": _text(title_div),
                "created": _text(created_div),
                "html": str(div),
            }
        )
    return pages
```

Python's standard `email` module unpacks the MIME archive, and BeautifulSoup finds the page
`<div>`s. The style check skips the "Created with OneNote." footer, which is also a top-level
`<div>`.

The tests in `tests/test_onenote_export.py` build a small fake `.mht` in code, so no real names
or ratings end up in the repo. One test came from the real export: OneNote wraps long lines in
its HTML source, so a date could come out as `Monday,\r\nAugust 15, 2016`. `_text` collapses
the whitespace:

```python
def _text(element) -> str:
    """Visible text with whitespace collapsed. OneNote wraps long lines in its HTML source."""
    return " ".join(element.get_text(" ").split())
```

`created` stays as the text OneNote wrote, like `Sunday, November 21, 2021 11:08 AM`. Silver
turns it into a timestamp.

BeautifulSoup is now a dependency in two places. It's in `pyproject.toml` so the code and tests
run locally, and in the pipeline's `environment` so serverless installs it too. The comment the
template left in `pyproject.toml` says the same: pipeline dependencies go in the pipeline's
`environment` section.

Two other template leftovers in `pyproject.toml` changed. `packages` now points at
`src/album_club`, so `uv sync` installs the package and the tests can import it, and the
`main` script entry is gone because the template's `main.py` no longer exists.

## The pipeline

Lakeflow Declarative Pipelines (formerly Delta Live Tables) let you declare a table as a
function that returns a DataFrame, and the pipeline works out how to keep it up to date.
`src/pipeline/bronze_onenote_pages.py` declares the bronze table:

```python
split_mht_udf = F.udf(split_mht, PAGES)


@dp.table(comment="One row per OneNote page, split out of the .mht section exports as-is.")
def bronze_onenote_pages():
    exports_path = spark.conf.get("album_club.onenote_exports_path")  # noqa: F821 (spark is provided by the pipeline)
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.mht")
        .load(exports_path)
        .select(
            F.col("_metadata.file_path").alias("source_file"),
            F.col("modificationTime").alias("source_file_modified_at"),
            F.explode(split_mht_udf("content")).alias("page"),
        )
        .select("source_file", "source_file_modified_at", "page.*", F.current_timestamp().alias("ingested_at"))
    )
```

`PAGES` is the schema of what `split_mht` returns, an array of structs. The `noqa` comments are
there because the pipeline provides `spark` as a global, which ruff can't see.

`cloudFiles` is Auto Loader. It watches the volume and reads each new file exactly once,
remembering in a checkpoint which files it has already processed. The `binaryFile` format hands
each `.mht` over as one row with the whole file in `content`. The UDF splits it into pages, and
`explode` turns that one row into one row per page.

Because the function returns a stream, `bronze_onenote_pages` is a streaming table. Each
pipeline update adds rows for the files Auto Loader hasn't seen and leaves the rows already
there alone, which is what you want from a layer that holds things as they arrived.

The volume path comes from the pipeline's `configuration` in the bundle, so the same code reads
from the dev volume in dev and the prod volume in prod.

The import of `album_club` works because `root_path: ../src` puts `src/` on the pipeline's
Python path, including on the workers that run the UDF.

The bronze table keeps the page HTML and drops the images. The images are most of the file's
103 MB, and nothing planned needs them. They're still in
the original file in the volume if that changes.

## Running it

Deploying to dev creates the schema, the volume and the pipeline:

```bash
databricks bundle deploy
```

```text
Created schemas.album_club
Created volumes.onenote_exports
Created pipelines.album_club_pipeline
Resources: 3 created, 0 changed, 0 deleted, 1 unchanged
```

Then I uploaded the export to the dev volume, with the export date in the file name:

```bash
databricks fs cp data/onenote-export/Music.mht \
  dbfs:/Volumes/workspace/dev_scott_album_club/onenote_exports/music-2026-11-07.mht
```

The date matters if I ever export again. Auto Loader only reads files it hasn't seen, so a new
export needs a new name. It then lands as a second set of 272 rows with a different
`source_file`, and silver can take the newest copy of each page.

Running the pipeline:

```bash
databricks bundle run album_club_pipeline
```

```text
Flow 'workspace.dev_scott_album_club.bronze_onenote_pages' is RUNNING.
Completed a streaming update of 'workspace.dev_scott_album_club.bronze_onenote_pages'.
Update c58b76 is COMPLETED.
```

The whole update took about a minute, most of it waiting for serverless compute. The table has
what the local run of `split_mht` found:

```sql
SELECT count(*) AS pages, count(DISTINCT source_file) AS files,
       count_if(title = '' OR created = '') AS missing
FROM workspace.dev_scott_album_club.bronze_onenote_pages
```

```text
+-----+-----+-------+
|pages|files|missing|
+-----+-----+-------+
|272  |1    |0      |
+-----+-----+-------+
```

| page_number | title | created |
|---|---|---|
| 1 | Albums (Crafting Bytes) | Monday, August 15, 2016 4:20 PM |
| 6 | W001 - #1 - Frank Sinatra - In the Wee Small Hours (1955) | Sunday, November 21, 2021 11:08 AM |
| 7 | W002 - #201 - The Stooges - Fun House (1970) | Thursday, December 16, 2021 4:07 PM |

That's 272 pages, all with a title and a created date. Not all of them are meetings: page 1 is a
list of albums from 2016, years before the club started. Sorting that out is post 4's job.

## Prod

Fast-forwarding `v2-from-scratch` ran the checks and then the deploy, and the deploy failed:

```text
Error: cannot create resources.schemas.album_club: Principal 'album-club-ci' does not have CREATE SCHEMA on Catalog 'workspace'. (403 PERMISSION_DENIED)
```

In dev, the bundle deploys as me, and I'm a workspace admin. In prod it deploys as the service
principal, which only had `USE CATALOG`, the privilege every workspace user gets on `workspace`.
The catalog isn't a bundle resource, so this grant is a one-time command, like creating the
service principal in part 2:

```bash
databricks grants update catalog workspace --json '{"changes": [{"principal": "fc6dd84e-5cda-40d2-81db-865f13b5b731", "add": ["CREATE_SCHEMA"]}]}'
```

Re-running the failed job created everything:

```text
Created schemas.album_club
Created volumes.onenote_exports
Created pipelines.album_club_pipeline
Created pipelines.album_club_pipeline.permissions
Resources: 4 created, 0 changed, 0 deleted, 2 unchanged
```

### Access to the prod schema

The service principal created the prod schema, so it owns it, and my upload to the prod volume
was refused:

```text
Error: User does not have USE SCHEMA privilege on SCHEMA 'workspace.album_club'.
```

The fix is a `grants` block on the schema in `resources/album_club.yml`. Grants on a schema
apply to its volumes and tables, so one block covers uploading exports and querying tables:

```yaml
  schemas:
    album_club:
      ...
      grants:
        - principal: account users
          privileges: [USE_SCHEMA, SELECT, READ_VOLUME, WRITE_VOLUME]
```

The grant goes to `account users` rather than to me by name, which keeps my email out of the
repo. A narrower group wasn't available. Unity Catalog only accepts account-level groups, so the
workspace's `admins` and `users` groups are out. It refused the system-generated workspace admins
group outright, and Free Edition refuses the account-level identity APIs (part 2), so I can't
create an account group. On Free Edition, `account users` is just me and the service principal.

With the grant deployed, I uploaded the same export to the prod volume and ran the prod
pipeline:

```bash
databricks fs cp data/onenote-export/Music.mht \
  dbfs:/Volumes/workspace/album_club/onenote_exports/music-2026-11-07.mht
databricks bundle run -t prod album_club_pipeline
```

I started the run, but the pipeline runs as the service principal, like the `hello` job in
part 2, and the service principal owns the bronze table. Prod has the same 272 pages as dev.

The upload is the one step in prod that doesn't go through CI. The bundle defines where the
export goes, but the file itself has names and ratings in it, so it goes from my laptop straight
to the volume.

## Next: parsing into silver

Bronze has every page, but a page is still a blob of HTML. Post 4 parses it into silver tables
(meetings, what was played, ratings, favorites and people), following the rules in
`docs/data-decisions.md`, with unit tests for each piece of shorthand.
