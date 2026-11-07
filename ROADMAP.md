# Album Club on Databricks — Blog Series Roadmap

Building a data pipeline from scratch on Databricks Free Edition, starting from the
OneNote pages used to run a weekly album club, and documenting each step as it's built.

## Ground rules

- **Nothing lives in Databricks that isn't in git.** Every job, pipeline, schema, volume
  and dashboard is a Declarative Automation Bundle resource (formerly Databricks Asset Bundles), deployed with `databricks bundle deploy`.
  No clicking things into existence in the UI.
- **Dev and prod are separate schemas** in the single `workspace` catalog (Free Edition limit).
- **One post = one tag.** Each post ships with a git tag (`post-01`, `post-02`, …) so readers
  can check out the code exactly as described.
- **The one exception:** secret *values* (API tokens). The scope and the steps to populate it
  are documented in the repo; the values are not.

## Posts

| # | Post | What gets checked in |
|---|------|----------------------|
| 1 | **Getting set up** | Why music data is messy, which motivates the whole series: artist names vary (The Stones / Rolling Stones / The Rolling Stones), artists change names (John Cougar → John Cougar Mellencamp → John Mellencamp) and rights change hands, sometimes through lawsuits, and one "album" can mean the original release, a deluxe edition, or a decades-later reissue. What Databricks Free Edition gives you (and its limits). Installing the GitHub CLI and `gh auth login`, installing the Databricks CLI and `databricks auth login`. Git repo, Python project (uv), `databricks bundle init`, a hello-world job deployed to `dev`. |
| 2 | **CI/CD with GitHub Actions** | Service principal for deployments. Tests + `bundle validate` on every PR; deploy to prod on merge. From here on, every post's changes ship through this pipeline. |
| 3 | **From OneNote to bronze** | Script that pulls pages via the Microsoft Graph OneNote API and saves raw HTML + metadata (idempotent, only new/changed pages). Schema and Volume as bundle resources, upload to the Volume, Auto Loader into a bronze table. |
| 4 | **Parsing and cleaning into silver** | Pure-Python HTML parser with local unit tests, run inside a Lakeflow Declarative Pipeline with expectations. Silver tables: `meetings`, `albums`, `ratings`, `members`. Member alias map as a seed file in the repo, stable IDs, handling non-numeric ratings. |
| 5 | **Enrichment** | Album catalog seed data. Track listings and release dates from Spotify or MusicBrainz, running unattended, with ambiguous matches routed to a review table. |
| 6 | **Gold tables and a dashboard** | Per-member stats, top-rated albums, trends over time. AI/BI dashboard as a `.lvdash.json` bundle resource. |
| 7 | **An app for entering this week's ratings** | Databricks App (bundle resource) for the host to record a meeting: album, date, each member's score and favorite songs. Where app writes land (Delta table vs. Lakebase) and how they flow into silver alongside OneNote history. |
| 8 | **An app for members to edit their own ratings** | Second app (or a second mode of the first) where each member sees and corrects only their own scores, using the app's knowledge of who is signed in. |
| 9 | **Running it weekly** | Scheduled job that runs the whole chain, so a new meeting reaches the dashboard without manual steps. |
| — | **Possible extras** | Genie space ("who rates 70s prog highest?"), recommendations, a look back at the series. |

## Decisions

- **Repo:** this series lives on the `v2-from-scratch` branch of `mscottreed/album-club`,
  branched from `main`. When the series is done, `v2-from-scratch` merges back into `main`.
- **Member names in public posts and data:** first name + last initial (e.g. "Scott R").
  No dates of birth.
- **OneNote extraction** is written from scratch in post 3.
- **Branching:** one feature branch per post, PR'd into `v2-from-scratch`. CI deploys prod
  from `v2-from-scratch` until it merges into `main`.

## Housekeeping (not blog posts)

- Once the new pipeline is running, remove the earlier exploratory objects from the
  workspace (the `Album Review Pipeline`, the `Get Songs From Album` job, the
  `workspace.music` schema and its volumes, the `album-club` Git folder).

## Open questions

- **Apps on Free Edition:** apps stop 24 hours after starting and must be restarted (maybe
  by a scheduled job before each meeting). Max 3 apps per account.
- **Who can use the apps:** members need to sign in to the workspace to use an app. Can
  Free Edition add other users, and does everyone want a Databricks login?
- **Source of truth going forward:** once ratings are entered in the app, does OneNote become
  history only (one-time import), or do both keep running?
