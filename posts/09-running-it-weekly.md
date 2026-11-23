# Album Club on Databricks, Part 9: Running It Every Week

The club meets on Sundays, and since part 7 the ratings go into an app instead of OneNote. On Free
Edition an app stops 24 hours after it starts, so before each meeting I had to remember to open
the workspace and start it. Now a job starts it on Sunday morning and another one stops it that
night.

That leaves one manual step from part 8. When a save can't start the refresh, the app tells you
the rating will be on the dashboard after the next refresh, but nothing made sure there'd be a
next one. Now the refresh also runs every Monday morning.

The code for this post is tagged `post-09` in
[mscottreed/album-club](https://github.com/mscottreed/album-club).

## A job that starts the app

A job task can run a Python file, and the Databricks SDK can start and stop an app, so the whole
thing is a short script, `src/app_power.py`:

```python
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["start", "stop"])
    parser.add_argument("--app", required=True)
    args = parser.parse_args()
    apps = WorkspaceClient().apps  # as the job's run-as identity
    state = apps.get(args.app).compute_status.state
    # Someone may have started it by hand already, or it may have stopped on its own.
    if args.action == "start" and state != ComputeState.ACTIVE:
        apps.start_and_wait(args.app)
    elif args.action == "stop" and state != ComputeState.STOPPED:
        apps.stop_and_wait(args.app)
    print(f"{args.app}: {apps.get(args.app).compute_status.state.value}")
```

`WorkspaceClient()` with no arguments picks up the job's own credentials, so there's no token to
store. In prod the jobs run as the CI service principal (the bundle's `run_as` from part 2), and
the prod target gives that service principal `CAN_MANAGE` on everything it deploys, the app
included. In dev they run as me, and I own my dev app.

The two jobs live in their own file, `resources/album_club_app_power.yml`. They're the same job
twice with a different schedule and argument:

```yaml
album_club_app_start:
  name: album_club_app_start
  schedule:
    quartz_cron_expression: "0 0 9 ? * SUN"
    timezone_id: America/Los_Angeles
  tasks:
    - task_key: start_app
      spark_python_task:
        python_file: ../src/app_power.py
        parameters: [start, "--app=${resources.apps.ratings_app.name}"]
      environment_key: serverless
```

The stop job is the same at `0 0 21 ? * SUN`, 9 PM. Quartz cron has a seconds field first, so
`0 0 9 ? * SUN` is 9:00:00 AM on Sundays, and the `?` says "no particular day of the month", since
the day of the week is set. The app name comes from `${resources.apps.ratings_app.name}`, so the
dev jobs start the dev app and the prod jobs start the prod one.

Members can still change their own ratings from part 8, but only while the app is up, which is
now Sundays from 9 to 9.

## What starting an app brings back

Starting a stopped app creates a new deployment from the bundle's source folder in the
workspace, the same folder `databricks bundle deploy` uploads to, so it runs the latest code. When I deployed this post's
code to dev, the app was stopped, and the deploy didn't create a deployment. When the start job
ran a few minutes later, the app got a new one, built from the files that deploy had just
uploaded. So if CI deploys new app code to prod midweek while the app is stopped, the Sunday
start picks it up.

Both apps were stopped when I got to this, with this message:

```text
App compute was stopped due to workspace or account status.
```

The day before, the workspace had hit Free Edition's daily compute limit, and that stopped the
apps along with everything else. This is the "it may have stopped on its own" case in the script's
comment. The start job doesn't care how the app stopped, so next Sunday it comes back either way.

## The refresh runs on Mondays

Every save in the app starts `album_club_refresh`, so most weeks the dashboard is up to date a
few minutes after the meeting. But a save doesn't always get its refresh. In part 8 the first
prod save hit a permission error, and on a day when the workspace is over its compute limit, the
refresh can't run at all. Either way the change record is safe in the volume, and the app says:

```text
Saved W155, but the refresh didn't start. It will be on the dashboard after the next one.
```

The schedule makes sure there is a next one:

```yaml
album_club_refresh:
  name: album_club_refresh
  # Each save in the app starts this job. The weekly run picks up any save whose refresh
  # didn't start, the morning after the meeting, when the free daily compute limit has reset.
  schedule:
    quartz_cron_expression: "0 0 6 ? * MON"
    timezone_id: America/Los_Angeles
```

I put it on Monday morning instead of right after the app stops on Sunday night. If Sunday is
the day the compute limit runs out, a run at 9:30 that night would fail too, and Monday is a new
day either way. The refresh reads every change record in the volume, so it doesn't matter how many
saves missed their refresh or when they happened.

Nothing in the chain needs OneNote anymore, so there's nothing else to schedule. The meeting goes
into the app, the app's save starts the refresh, and the Monday run catches whatever that missed.

## Schedules in dev

The dev target uses `mode: development`, and development mode deploys every schedule paused. After
`databricks bundle deploy`, all three dev jobs show their schedules with `pause_status: PAUSED`,
so my dev app doesn't start itself every Sunday and the dev refresh doesn't run every Monday. In
prod they're live.

To try the jobs in dev, I ran each one by hand:

```bash
databricks bundle run album_club_app_start
databricks bundle run album_club_app_stop
```

## Trying it

In dev, the start job took about two and a half minutes. The app came up with a fresh deployment
and was running by the time I checked again. The stop job took about a minute, and the app went
back to `STOPPED`.

<!-- TODO: prod, after the first Sunday (2026-11-29): did album_club_app_start bring the prod app
up at 9, did the stop job stop it at 9 PM, and did the Monday refresh run? -->

## What's left

That's the last of the posts on the roadmap. It also lists a few possible extras, like a Genie
space for questions such as "who rates 70s prog highest?"
