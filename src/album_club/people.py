"""Match a name as written on a page to a person in the people seed."""


def match_person(name: str, people: list[dict]) -> str | None:
    """Match by first name, plus the last initial when one is written.

    "Linda Lackey", "Linda L" and "Linda" all match Linda L, the only Linda. "Bill" alone matches
    no one, because there are two. Returns the person, or None if no one or more than one matches.
    """
    first, *rest = name.split()
    initial = rest[0][0].upper() if rest else None
    matches = [
        p["person"]
        for p in people
        if p["first_name"].lower() == first.lower() and (initial is None or p["last_initial"] == initial)
    ]
    return matches[0] if len(matches) == 1 else None
