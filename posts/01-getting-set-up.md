# Album Club on Databricks, Part 1: Getting Set Up

Since November 2021, a small group of us has met most weeks to listen to an album together.
Everyone gives it a score and names their favorite songs, and I write it all down in OneNote,
one page per meeting. After a few years that's a lot of pages, and I want to answer questions
OneNote can't: who rates the highest, which decades we like best, which albums split the room.

This series builds a data pipeline for those pages on Databricks, starting from an empty folder.
I'm writing it as I go, with two rules:

1. Nothing lives in Databricks that isn't in git. Jobs, pipelines, schemas, volumes and
   dashboards are all defined in files and deployed from the command line. If I click something
   into existence in the UI, I've broken the rule.
2. Every post matches a git tag. The code for this post is tagged `post-01` in
   [mscottreed/album-club](https://github.com/mscottreed/album-club), so you can check out exactly
   what's described here.

This first post covers why the data is harder than it looks, what Databricks Free Edition gives
you, the tools to install, and a first "hello world" job deployed from git.

## Music data is messy

An album club sounds like an easy data set: a date, an album, an artist, some scores. Then you
try to join it to anything else.

- The same artist goes by several names. The Rolling Stones, Rolling Stones, and The Stones are
  one band, and depending on who typed the notes that night, any of them might show up.
- Artists change their names. John Cougar became John Cougar Mellencamp, then John Mellencamp.
  An album released under one name gets reissued under another.
- Rights change hands. Catalogs get sold, and sometimes a lawsuit decides who owns what. The
  label on a record can depend on which year you look it up.
- "The album" might be several albums. Is it the original 1960s release, the deluxe edition
  with bonus tracks, or the 2022 reissue? Streaming services usually serve the latest remaster.
  Look up Frank Sinatra's *In the Wee Small Hours* on Spotify and track 1 comes back as "In The Wee
  Small Hours Of The Morning - Remastered 1998".

None of this matters in a notebook page. It matters a lot when you want to count how many Stones
albums we've rated, or match our notes to a track listing. Post 4 deals with the name problems and
post 5 with editions, but this is the reason the series doesn't start by writing SQL.

## What Databricks is

If you haven't used it, Databricks is a data platform built around Apache Spark, started by the
people who created Spark. Databricks calls the result a lakehouse: data is stored as files, as in a
data lake, but you query it as tables, as in a data warehouse. Three pieces come up in every post:

- Spark runs the work. It splits a job across several machines, and you write it in Python or
  SQL.
- Delta Lake is the table format. A Delta table is a folder of Parquet files plus a
  transaction log, so a write either finishes completely or doesn't happen at all, and you can
  look at a table as it was before the last change.
- Unity Catalog organizes and secures everything. Tables live in schemas, and schemas live in
  catalogs, so every table has a three-part name like `workspace.album_club.silver_ratings`.
  Files get the same treatment in volumes. Who can see what is a set of grants on those objects.

You reach all of it through a workspace, the website you log in to, which also holds notebooks,
jobs and dashboards.

## Databricks Free Edition

[Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) is a
free, permanent workspace intended for learning and personal projects. It isn't a time-limited
trial. The trade-off is a set of
[limits](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations). The ones
that shape this project are:

- Serverless only. You can't create your own clusters. Jobs, pipelines and notebooks all run
  on serverless compute, which suits a small weekly workload.
- Daily usage quotas. Go over and compute shuts off until the quota resets. Your data stays.
- One SQL warehouse, 2X-Small. Enough for a dashboard over a few thousand rows.
- One active pipeline per pipeline type, and at most 5 concurrent job tasks.
- Outbound internet access is limited to trusted domains. This one will matter when we call
  the Spotify API in post 5.
- No commercial use, which is fine for an album club.

Free Edition also gives you a single catalog, `workspace`, so instead of separate dev and prod
catalogs this project uses separate schemas inside it.

## Installing the tools

I'm on a Mac and use Homebrew. Each tool's docs cover Windows and Linux.

### GitHub CLI

The [GitHub CLI](https://cli.github.com/) (`gh`) handles creating the repo, opening pull requests,
and later, setting up GitHub Actions secrets.

```bash
brew install gh
gh auth login
```

`gh auth login` asks a few questions. I picked GitHub.com, HTTPS, and logging in through the
browser. When it finishes, `gh auth status` shows which account you're using.

### Databricks CLI

The [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) deploys everything
in this series.

```bash
brew trust databricks/tap
brew tap databricks/tap
brew install databricks
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile personal-free
```

The host is the URL you see in the browser when you're logged in to your workspace. The profile
name is a label, and I named mine after the account rather than the project, since every project
in this workspace can share it. `auth login` opens a browser to finish an OAuth login and saves the
profile in `~/.databrickscfg`. To check it worked:

```bash
databricks auth profiles
```

### uv

[uv](https://docs.astral.sh/uv/) manages Python versions and dependencies.

```bash
brew install uv
```

#### Why uv instead of venv and pip

The standard way to set up a Python project is `python -m venv .venv`, activate it, and
`pip install -r requirements.txt`. For years that's what I did.  But there are a number of problems with that approach.

First, venv uses whatever Python you already have. My Mac has Python 3.15. Databricks Connect
has to match the Python version on the Databricks side, so the project pins it:

```toml
requires-python = ">=3.12,<3.13"
```

With venv, I'd have to install 3.12 myself (Homebrew, pyenv, or the python.org installer) and
remember to use it. uv reads that line, downloads Python 3.12 if it isn't installed, and builds
the environment with it. I never installed 3.12. `uv sync` fetched it:

```bash
$ python3 --version
Python 3.15.2
$ .venv/bin/python --version
Python 3.12.15
```

Second, `requirements.txt` doesn't lock anything unless you make it. If it lists `pytest`, you
get whatever version of pytest is newest on the day you install, plus whatever versions of its
dependencies are newest. `pip freeze` pins everything, but then you can't tell what you asked
for from what came along with it. uv keeps the two apart: `pyproject.toml` lists what the project
needs, and `uv.lock` records the exact version of every package that was resolved, 62 of them
here. Both go into git. When GitHub Actions builds this project in post 2, it will install the same
62 packages I have on my laptop, and `uv sync --locked` fails the build if the lock file is out of
date instead of quietly resolving something new.  In my opinion that is the main reason to use uv.
I switched to `uv` for everything almost three years ago, and frankly it is a godsend.

Third, it's one tool instead of several. uv replaces venv, pip, pip-tools and pyenv, and
`uv run` runs a command inside the environment without activating anything:

```bash
uv run pytest
```

It's also much faster than pip. [Astral](https://docs.astral.sh/uv/), the company behind it,
puts it at 10 to 100 times faster.

The Databricks bundle template assumes uv too. The `README.md` it generates tells you to run
`uv sync --dev`, so going with uv also means following the path Databricks expects.

## Creating the bundle

Databricks calls a project defined in files a **Declarative Automation Bundle** (it used to be
called a Databricks Asset Bundle, and you'll still see that name around). A bundle is a
`databricks.yml` file plus whatever YAML and code it points to. The CLI can generate a starting
point:

```bash
databricks bundle init default-minimal
```

I answered the prompts with:

| Prompt | Answer |
|---|---|
| Project name | `album_club` |
| Default catalog | `workspace` |
| Use a personal schema for each user in dev | `yes` |
| Language | `python` |

`init` creates a folder named after the project. I moved its contents up to the root of my repo.
Here's what you get:

| File | What it's for |
|---|---|
| `databricks.yml` | The bundle definition: its name, variables, and the `dev` and `prod` targets. |
| `pyproject.toml` | Python project settings for uv, including dev tools like pytest and Databricks Connect. |
| `tests/conftest.py` | pytest setup that connects to serverless compute. We'll use it in post 2. |
| `fixtures/` | A place for test data files. |
| `.vscode/` | Editor settings for VS Code and Cursor. |
| `AGENTS.md` | Notes for AI coding assistants. |
| `README.md`, `.gitignore` | What you'd expect. |

### Dev and prod targets

The interesting part of `databricks.yml` is `targets`:

```yaml
targets:
  dev:
    mode: development
    default: true
    ...
  prod:
    mode: production
    ...
```

`dev` is the default. That means if you don't pick a target with `-t`, you get dev. In development mode, everything you deploy goes into your own user folder
and gets a `[dev yourname]` prefix, and any schedules are paused. You can deploy as often as you
like without touching anything real. `prod` is the copy that runs on a schedule. From post 2 on,
only GitHub Actions deploys to it.

I made one change to what `init` generated. The `prod` target had my email address written out
in two places. The repo is public, so I replaced it with a variable the CLI fills in at deploy
time:

```yaml
  prod:
    mode: production
    workspace:
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    permissions:
      - user_name: ${workspace.current_user.userName}
        level: CAN_MANAGE
```

### A hello world job

A job is how Databricks runs work without someone sitting at a notebook. Databricks now calls
them Lakeflow Jobs, though the CLI and the bundle files still just say `jobs`. A job is made of
one or more tasks, and each task runs one thing: a notebook, a Python file, a SQL query, a
pipeline, and so on. Tasks can depend on each other, so one runs only after another succeeds.
The job also holds the settings around the work: what compute to use, when to run (on a
schedule, when files arrive, or only when started by hand), retries, and who gets notified when
something fails. Every run is recorded with its output and logs, so you can see what happened
last Tuesday.

Later in this series a job will run the whole pipeline every week. For now, the job has a single
task that runs one Python file.

The minimal template has no code in it, so here's the smallest job that proves the whole chain
works. A Python file, `src/hello.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

row = spark.sql("SELECT current_user() AS user, current_catalog() AS catalog, current_timestamp() AS ts").first()
print(f"Hello, Album Club! Running as {row.user} in catalog '{row.catalog}' at {row.ts}")
```

And a job definition, `resources/hello.job.yml`. `databricks.yml` includes everything matching
`resources/*.yml`, so new resources are picked up as soon as you add the file.

```yaml
resources:
  jobs:
    hello:
      name: hello
      tasks:
        - task_key: hello
          spark_python_task:
            python_file: ../src/hello.py
          environment_key: serverless
      environments:
        - environment_key: serverless
          spec:
            # Serverless environment version: pins Python, preinstalled libraries and Databricks Connect.
            # See https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/
            environment_version: "6"
```

There's no cluster in that definition. On Free Edition every job runs on serverless compute, and
the `environments` block says which serverless environment to run in.

The word "environments" in the job has nothing to do with the dev and prod we defined in the `targets` section of `databricks.yml`.
It's the serverless Python environment the task runs in, and `serverless` is just the name I gave it so 
the task can refer to it. The key is an arbitrary label: any name works, as long as the task's
`environment_key` matches one of the keys under `environments`.


#### Where are dev and prod?

The job doesn't mention dev or prod anywhere, and that's on purpose. A resource is defined once,
and you pick the target when you deploy:

```bash
databricks bundle deploy            # no -t, so the target marked default: true (dev) is used
databricks bundle deploy -t prod    # the same job definition, deployed to prod
```

Remember the `default: true` in the `targets` section only decides which target you get when you leave off `-t`.
The target's `mode` decides what the CLI does to the job on the way out: in development mode the name gets the
`[dev scott]` prefix and schedules are paused, and in production mode the name is left alone and
schedules run. If a target ever needs something different, like a schedule that only exists in
prod, you override that one setting under `targets.prod.resources` and the rest stays shared.

#### What environment_version means

On a classic cluster you pick a Databricks Runtime version. On serverless you pick an
[environment version](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/)
instead, and each one fixes the Python version, the preinstalled libraries, and the Databricks
Connect release that goes with it:

| Version | Python | Databricks Connect | End of support |
|---|---|---|---|
| 6 | 3.12.3 | 19 | Sep 2029 |
| 5 | 3.12.3 | 18 | Feb 2029 |
| 4 | 3.12.3 | 17.3 | Aug 2028 |

I'm using 6, the newest. It matters for local development too: Databricks Connect on my laptop
should match the one on the server, so `pyproject.toml` pins the same release. The template
originally pinned 16.4, which goes with the older version 3, so I changed it:

```toml
"databricks-connect>=19.1,<19.2",  # matches serverless environment version 6
```

## Deploying it

There are three commands to take it from local files to a finished run. `validate` checks the configuration without changing anything:

```bash
databricks bundle validate
```

```text
Name: album_club
Target: dev
Workspace:
  Host: https://dbc-eebf3c2f-dab5.cloud.databricks.com
  User: scott@craftingbytes.com
  Path: /Workspace/Users/scott@craftingbytes.com/.bundle/album_club/dev

Validation OK!
```

`deploy` uploads the files and creates or updates the job:

```bash
databricks bundle deploy
```

```text
Uploading bundle files to /Workspace/Users/scott@craftingbytes.com/.bundle/album_club/dev/files...
Created jobs.hello
Resources: 1 created, 0 changed, 0 deleted, 0 unchanged
```

`run` starts the job and waits for it to finish:

```bash
databricks bundle run hello
```

```text
"[dev scott] hello" RUNNING
"[dev scott] hello" TERMINATED SUCCESS
Hello, Album Club! Running as scott@craftingbytes.com in catalog 'workspace' at 2026-11-07 01:54:04
```

About 25 seconds from start to finish, most of it waiting for serverless compute. The job shows
up in the workspace's Jobs list as `[dev scott] hello`, but it came from a file in git, and the
next deploy will put it back the way the file says if anyone edits it in the UI.

For local development, `uv sync` creates a virtual environment with PySpark, Databricks Connect
and pytest, ready for when there's code worth testing.

## Next: CI/CD

Right now I deploy from my laptop, which works but depends on me remembering to do it. In post 2,
GitHub Actions takes over: every pull request gets validated and tested, and merging deploys to
prod. After that, the rest of the series ships through the CI/CD pipeline instead of from my machine.
