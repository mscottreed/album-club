# Album Club on Databricks — Blog Series Roadmap

Building a data pipeline from scratch on Databricks (Free Edition), starting from the
OneNote pages used to run a weekly album club.

## Ground rules

- **Nothing lives in Databricks that isn't in git.** Every job, pipeline, schema, volume
  and dashboard is a Databricks Asset Bundle resource, deployed with `databricks bundle deploy`.
  No clicking things into existence in the UI.
- **Dev and prod are separate schemas** in the single `workspace` catalog (Free Edition limit).
- **One post = one tag.** Each post ships with a git tag (`post-01`, `post-02`, …) so readers
  can check out the code exactly as described.
- **The one exception:** secret *values* (API tokens). The scope and the steps to populate it
  are documented in the repo; the values are not.

## Posts

| # | Post | What gets checked in |
|---|------|----------------------|
| 1 | **What went wrong last time, and the setup that fixes it** | Story of the first attempt (pipeline pointing at a deleted folder, hand-made volumes and tables, parsing code that never made it to git) and the rules above. Then: git repo, Python project (uv), Databricks CLI auth, `databricks bundle init`, a hello-world job deployed to `dev`. |
| 2 | **From OneNote to bronze** | Script that pulls pages via the Microsoft Graph OneNote API and saves raw HTML + metadata (idempotent, only new/changed pages). Schema and Volume as bundle resources, upload to the Volume, Auto Loader into a bronze table. |
| 3 | **Parsing and cleaning into silver** | Pure-Python HTML parser with local unit tests, run inside a Lakeflow Declarative Pipeline with expectations. Silver tables: `meetings`, `albums`, `ratings`, `members`. Member alias map as a seed file in the repo, stable IDs instead of `row_number()`, handling non-numeric ratings. |
| 4 | **Enrichment** | Album catalog seed data (replacing the +2000/+3000 ID offsets). Track listings and release dates from Spotify or MusicBrainz, running unattended, with ambiguous matches routed to a review table. |
| 5 | **Gold tables and a dashboard** | Per-member stats, top-rated albums, trends over time. AI/BI dashboard as a `.lvdash.json` bundle resource. |
| 6 | **CI/CD with GitHub Actions** | Tests + `bundle validate` on PRs, deploy to prod on merge to `main`. |
| 7 | **Running it weekly** | Scheduled job that runs the whole chain, so a new meeting page reaches the dashboard without manual steps. |
| 8 | **Retiring the old build** | Compare new vs. old outputs, then delete the old pipeline, job, volumes and tables. |
| — | **Possible extras** | Genie space ("who rates 70s prog highest?"), recommendations, a look back at the series. |

## Decisions

- **Repo:** this series lives on the `v2-from-scratch` branch of `mscottreed/album-club`,
  branched from `main` (the first attempt). The branch's first commit removes the v1 code;
  when the series is done, `v2-from-scratch` merges back into `main`.
- **Member names in public posts and data:** first name + last initial (e.g. "Scott R").
  No dates of birth.
- **OneNote extraction:** the original OneNote → CSV script is presumed lost, so post 2
  writes it from scratch (and this time it's in git).

## Open questions

- Should CI/CD (post 6) move up right after post 1, so every later post deploys through it?
