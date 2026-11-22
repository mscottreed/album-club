"""Start or stop the ratings app.

Free Edition stops an app 24 hours after it starts, so one job starts it on Sunday morning, before
the meeting, and another stops it that evening. Starting an app brings back its last deployment.
"""

import argparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import ComputeState


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


if __name__ == "__main__":
    main()
