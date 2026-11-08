# Album Club on Databricks, Part 2: CI/CD with GitHub Actions

At the end of [part 1](01-getting-set-up.md), I could deploy the bundle from my laptop with
`databricks bundle deploy`. That works, but prod then depends on me remembering to deploy, and
on my laptop having whatever I last checked out. In this post, GitHub Actions takes over: every
push gets checked, and a push to the main branch deploys to prod. From here on, nothing reaches
prod except through this pipeline.

As a side note I always try to build my projects like this, a hello world and a deployment
as the first few steps. It has two benefits: 1) If you are paying for the infrastructure it puts
the pressure on to get something usable out the door, and 2) deploying manually can become a
habit that takes time away from working on the project, so it is better to stop it before it
starts.

The code for this post is tagged `post-02` in
[mscottreed/album-club](https://github.com/mscottreed/album-club).

## How the branches work

I'm the only person on this project, so I'm not using pull requests. Each post gets its own
branch (`post-02-cicd` for this one), and when it's done I fast-forward merge it into
`v2-from-scratch`, the branch this series is built on. The pipeline follows that:

| Push to | What runs |
|---|---|
| a `post-*` branch | checks: install, lint, validate the bundle, run the tests |
| `v2-from-scratch` | the same checks, then deploy to prod |

If I were working with other people, I'd add pull requests and make the checks required before
a merge. The workflow wouldn't need to change much.

## Who CI deploys as

GitHub Actions needs a Databricks identity to deploy with. It shouldn't be me, so I created a
**service principal**: an identity for automation rather than a person.

```bash
databricks service-principals create --display-name album-club-ci
```

This is the one piece of Databricks setup that isn't in the bundle. A bundle can't create the
identity that deploys it, so the command lives here in the post instead.

### Why not OIDC?

Databricks recommends
[workload identity federation](https://docs.databricks.com/aws/en/dev-tools/auth/provider-github)
for GitHub Actions. GitHub proves who it is on each run with a short-lived token, and Databricks
trusts tokens from your repo, so there's no secret stored anywhere. I wanted to use it.

The catch is that the trust is set up with a
[federation policy](https://docs.databricks.com/aws/dev-tools/auth/oauth-federation-policy), and
federation policies are managed at the account level, not in the workspace.

Databricks has two levels: an account, which holds one or more workspaces, and the workspaces
themselves. Each has its own API endpoint. The `personal-free` profile from part 1 points at my
workspace URL. Account-level calls go to `https://accounts.cloud.databricks.com` with an account
ID instead, so they need a second CLI profile. It's the same login, just a different endpoint:

```bash
databricks auth login --host https://accounts.cloud.databricks.com --account-id <account-id> --profile personal-free-account
```

The account ID is under your name in the top right of the account console. On Free Edition, I
could log in this way and the CLI could list my workspace, but every identity call was refused,
including the one for federation policies:

```bash
databricks account service-principal-federation-policy list 78851659483579 -p personal-free-account
```

```text
Error: Cannot complete request; user is unauthenticated
```

As far as I can tell, Free Edition doesn't make you an account admin, so OIDC is out. If you're on a paid workspace
with account admin access, use it.

### An OAuth secret instead

The fallback is an OAuth secret for the service principal, which a workspace admin can create.
The secret goes straight from the Databricks CLI into a GitHub Actions secret through a pipe, so
it's never printed or saved to a file:

```bash
J=$(databricks service-principal-secrets-proxy create 78851659483579 -o json)
printf '%s' "$J" | jq -r .secret | gh secret set DATABRICKS_CLIENT_SECRET
printf '%s' "$J" | jq '{id, expire_time}'
unset J
```

`78851659483579` is the service principal's ID from the `create` command. The secret expires
after two years, and when it does, CI stops deploying until I create a new one.
That's the price of not having OIDC.

The workspace URL and the service principal's application ID aren't secrets, so they go in as
GitHub variables:

```bash
gh variable set DATABRICKS_HOST --body https://dbc-eebf3c2f-dab5.cloud.databricks.com
gh variable set DATABRICKS_CLIENT_ID --body fc6dd84e-5cda-40d2-81db-865f13b5b731
```

The Databricks CLI, the Python SDK and Databricks Connect all read `DATABRICKS_HOST`,
`DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` from the environment, so setting those
three is all the authentication the workflow needs.

## Changing the prod target

In part 1, prod deployed into the folder of whoever ran the deploy. With CI deploying, that
becomes a trap: if I ever deployed prod from my laptop, I'd get a second copy of everything in my
own folder. So prod now always lives in the service principal's folder and runs as the service
principal, whoever deploys it:

```yaml
variables:
  ci_service_principal:
    description: Application ID of the album-club-ci service principal that GitHub Actions deploys prod as
    default: fc6dd84e-5cda-40d2-81db-865f13b5b731

targets:
  prod:
    mode: production
    workspace:
      root_path: /Workspace/Users/${var.ci_service_principal}/.bundle/${bundle.name}/${bundle.target}
    run_as:
      service_principal_name: ${var.ci_service_principal}
    permissions:
      - service_principal_name: ${var.ci_service_principal}
        level: CAN_MANAGE
      - group_name: admins
        level: CAN_MANAGE
```

`run_as` means prod jobs run as the service principal, not as whoever deployed them.
Permissions go to the `admins` group instead of to me by name, which also keeps my email address
out of the repo.

I only added the service principal to `permissions` because `bundle validate -t prod` warned me
about it. The prod folder sits in the service principal's home folder, so the service principal
already has `CAN_MANAGE` on it, and the CLI wants that written down in the bundle:

```text
Warning: workspace folder has permissions not configured in bundle
```

## A first test

CI should run tests, and there's no real code to test yet. The parser in post 4 will bring real
tests. For now, one test proves the thing most likely to break: that whatever is running the
tests can reach Databricks serverless compute.

```python
def test_serverless_connection(spark):
    """Proves the environment running the tests can reach Databricks serverless compute."""
    assert spark.sql("SELECT 1 AS one").first().one == 1
```

The `spark` fixture comes from the `tests/conftest.py` that `bundle init` generated in part 1. It
uses Databricks Connect, and when no cluster is configured it falls back to serverless. Locally
it uses my `personal-free` profile, and in CI it uses the service principal.

One more fix came out of this: the generated `conftest.py` didn't pass ruff's import-order check,
so `uv run ruff check --fix` sorted its imports.

## The workflow

One file, `.github/workflows/ci.yml`, with two jobs:

```yaml
name: CI

on:
  push:
    branches:
      - "post-*"
      - v2-from-scratch
  workflow_dispatch:

env:
  DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
  DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID }}
  DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: astral-sh/setup-uv@v10.3.3
      - uses: databricks/setup-cli@v1.23.0
        with:
          version: 1.24.0

      - name: Install dependencies
        run: uv sync --locked
      - name: Lint
        run: uv run ruff check
      - name: Check formatting
        run: uv run ruff format --check
      - name: Validate bundle
        run: databricks bundle validate -t prod
      - name: Test
        run: uv run pytest

  deploy:
    needs: check
    if: github.ref == 'refs/heads/v2-from-scratch'
    runs-on: ubuntu-latest
    concurrency: prod-deploy
    steps:
      - uses: actions/checkout@v7
      - uses: databricks/setup-cli@v1.23.0
        with:
          version: 1.24.0

      - name: Deploy to prod
        run: databricks bundle deploy -t prod
```

A few details:

- `uv sync --locked` is where the lock file from part 1 pays off. CI installs exactly the
  packages in `uv.lock`, and fails if `pyproject.toml` has changed without the lock file being
  updated.
- The Databricks CLI version is pinned to 1.24.0, the same version I have locally, so CI and my
  laptop validate the bundle the same way.
- `deploy` only runs after `check` passes, and only on `v2-from-scratch`.
- `concurrency: prod-deploy` makes two quick pushes queue their deploys instead of running them
  at the same time.

### The first run failed

My first push failed before any step ran:

```text
Unable to resolve action `astral-sh/setup-uv@v10`, unable to find version `v10`
```

Many actions publish a moving major-version tag, so `actions/checkout@v7` always means the latest
7.x. `setup-uv` doesn't: it only has full version tags like `v10.3.3`. Pinning the exact version
fixed it.

## It works

On a push to `post-02-cicd`, the checks run as the service principal. From the log:

```text
User: fc6dd84e-5cda-40d2-81db-865f13b5b731
Path: /Workspace/Users/fc6dd84e-5cda-40d2-81db-865f13b5b731/.bundle/album_club/prod
Validation OK!
============================== 1 passed in 1.75s ===============================
```

The check job took 22 seconds, and the deploy job was skipped because this isn't
`v2-from-scratch`. Fast-forwarding `v2-from-scratch` ran the checks again and then deployed:

```text
Uploading bundle files to /Workspace/Users/fc6dd84e-5cda-40d2-81db-865f13b5b731/.bundle/album_club/prod/files...
Created jobs.hello
Created jobs.hello.permissions
Resources: 2 created, 0 changed, 0 deleted, 0 unchanged
```

Running the prod job shows who it runs as now:

```bash
databricks bundle run -t prod hello
```

```text
"hello" TERMINATED SUCCESS
Hello, Album Club! Running as fc6dd84e-5cda-40d2-81db-865f13b5b731 in catalog 'workspace' at 2026-11-07 18:38:52
```

The workspace now has two copies of the job: `[dev scott] hello`, which I deploy from my laptop
while I work, and `hello`, which only CI deploys.

## Next: getting the data out of OneNote

The pipeline has nothing to carry yet. Post 3 pulls the album club pages out of OneNote and into
Databricks as a bronze table, and every step of it will ship through this workflow.
