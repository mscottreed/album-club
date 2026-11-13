"""Work out when each meeting happened. OneNote only records when a page was created.

The rules are in docs/data-decisions.md: an override, else the date entered in the app, else a
date written in the page body within 3 weeks of creation, else the first Sunday after creation
that's after the previous meeting.
"""

import datetime as dt


def next_sunday(day: dt.date) -> dt.date:
    """The first Sunday strictly after day."""
    return day + dt.timedelta(days=(6 - day.weekday()) or 7)


def assign_meeting_dates(meetings: list[dict], overrides: dict[int, dt.date]) -> list[dict]:
    """Add meeting_date, date_source and review_reason to each meeting.

    meetings: dicts with week, created (date) and body_date (date or None), one per week.
    Works in week order, so each Sunday-rule date lands after the previous meeting's date.
    """
    dated = []
    previous = None
    for m in sorted(meetings, key=lambda m: m["week"]):
        review = None
        if m["week"] in overrides:
            date, source = overrides[m["week"]], "override"
        elif m.get("app_date") is not None:  # entered in the app, which asks for the date
            date, source = m["app_date"], "app"
        elif m["body_date"] is not None:
            date, source = m["body_date"], "page"
        else:
            date, source = next_sunday(m["created"]), "sunday_rule"
            while previous is not None and date <= previous:
                date += dt.timedelta(days=7)
        if previous is not None and date <= previous:
            review = f"not after the previous meeting ({previous})"
        dated.append({**m, "meeting_date": date, "date_source": source, "review_reason": review})
        previous = date
    return dated
