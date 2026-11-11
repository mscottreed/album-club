"""Match each album listen to Spotify, MusicBrainz and the 1001 Albums list.

crawl() gathers the API responses one listen needs. The fetch job runs it against the live APIs
and caches every response; the pipeline runs it again against the cache, so both see exactly the
same requests. Everything else here is a pure function of those responses. The rules are in
docs/data-decisions.md.
"""

import difflib
import re
import unicodedata
from collections import Counter
from collections.abc import Callable

# Import names, not the module: a UDF that references a module object pickles the module itself,
# and a workspace-file module carries Databricks' import hook, which workers can't load.
from album_club.apis import (
    MUSICBRAINZ,
    SPOTIFY,
    musicbrainz_release,
    musicbrainz_release_group,
    musicbrainz_release_group_loose_search,
    musicbrainz_release_group_search,
    musicbrainz_releases,
    spotify_album,
    spotify_album_tracks,
    spotify_search,
    spotify_track,
)

# (source, request) -> parsed JSON, or None when there's no usable response.
Get = Callable[[str, str], dict | None]

MATCH = 0.85  # minimum similarity for two names to count as the same


def crawl(listen: dict, override: dict, get: Get) -> dict:
    """Every API response enrichment needs for one album listen.

    listen has artist, album and spotify_track_ids (links on the page). override is the listen's
    row in the album_overrides seed, or {}.
    """
    artist, album = listen["artist"], override.get("album") or listen["album"]
    linked = []
    for track_id in listen["spotify_track_ids"]:
        if track := get(SPOTIFY, spotify_track(track_id)):
            linked.append(track["album"]["id"])
    found = []
    for search_artist in dict.fromkeys([artist, None]):  # without the artist too, in case it's misspelled
        search = get(SPOTIFY, spotify_search(search_artist, album)) or {}
        found += [
            a["id"]
            for a in (search.get("albums") or {}).get("items") or []
            if a and names_match(artist, album, [x["name"] for x in a["artists"]], a["name"])
        ]
        if found:
            break
    albums = {}
    for album_id in dict.fromkeys([override.get("spotify_album_id")] + linked + found):
        if album_id and (a := get(SPOTIFY, spotify_album(album_id))):
            tracks = list(a["tracks"]["items"])
            while len(tracks) < a["tracks"]["total"]:
                page = get(SPOTIFY, spotify_album_tracks(album_id, len(tracks)))
                if not page or not page["items"]:
                    break
                tracks += page["items"]
            albums[album_id] = {**a, "all_tracks": tracks}

    release_group = original = None
    group_id = override.get("musicbrainz_release_group_id")
    if not group_id:  # exact phrases, with and without a suffix like "(White Album)"
        titles = dict.fromkeys([album, without_suffix(album)])
        found = [get(MUSICBRAINZ, musicbrainz_release_group_search(artist, t)) for t in titles]
        groups = [g for f in found for g in (f or {}).get("release-groups", [])]
        if not any(_is_original_album(g) and _matches(g, artist, album) for g in groups):  # then a looser search
            loose = get(MUSICBRAINZ, musicbrainz_release_group_loose_search(artist, album))
            groups += (loose or {}).get("release-groups", [])
        group_id = pick_release_group({"release-groups": groups}, artist, album)
    if group_id:
        release_group = get(MUSICBRAINZ, musicbrainz_release_group(group_id))
        releases = []
        while page := get(MUSICBRAINZ, musicbrainz_releases(group_id, len(releases))):
            releases += page["releases"]
            if not page["releases"] or len(releases) >= page["release-count"]:
                break
        if release_id := pick_original_release(releases):
            original = get(MUSICBRAINZ, musicbrainz_release(release_id))
    return {"linked_album_ids": linked, "albums": albums, "release_group": release_group, "original": original}


# --- names


def normalize(text: str) -> str:
    """Lowercase, no accents or punctuation, '&' as 'and', no leading 'the'."""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    text = re.sub(r"['.’]", "", text)  # R.E.M. -> rem, Day's -> days
    text = re.sub(r"[^a-z0-9]+", " ", text.replace("&", " and ")).strip()
    return re.sub(r"^the ", "", text)


def without_suffix(text: str) -> str:
    """'In The Wee Small Hours (Remastered 1998)' -> 'In The Wee Small Hours'; also '[...]' and ' - ...'."""
    return re.sub(r"\s*(\([^)]*\)|\[[^\]]*\]|\s-\s.*)$", "", text) or text


def _without_suffixes(a: str, b: str) -> tuple[str, str]:
    return without_suffix(a), without_suffix(b)


def similar(a: str, b: str, contained_words: int = 0) -> float:
    """Similarity of two names from 0 to 1, ignoring case, punctuation and edition suffixes.

    With contained_words, a name of at least that many words that appears whole inside the other
    counts as a match: "James Brown" in "James Brown & The Famous Flames".
    """
    pairs = [(normalize(x), normalize(y)) for x, y in [(a, b), _without_suffixes(a, b)]]
    score = max(difflib.SequenceMatcher(None, x, y).ratio() for x, y in pairs)
    if contained_words:
        for x, y in pairs:
            short, long = sorted([x, y], key=len)
            if len(short.split()) >= contained_words and f" {short} " in f" {long} ":
                score = max(score, MATCH)
    return score


def numbers(name: str) -> set[str]:
    """Roman numerals and volume numbers in a name: 'Led Zeppelin II' -> {'ii'}, 'Story Volume 2' -> {'vol 2'}."""
    text = normalize(name)
    return set(re.findall(r"\b(?:ii|iii|iv|v|vi|vii|viii|ix)\b", text)) | {
        f"vol {n}" for n in re.findall(r"\bvol(?:ume)? ?(\d+)\b", text)
    }


def names_match(artist: str | None, album: str, candidate_artists: list[str], candidate_album: str) -> bool:
    """Whether a candidate (from Spotify or MusicBrainz) is the album on the page. Names with
    different numbers are different albums, however similar: 'Van Halen' isn't 'Van Halen II'. A
    self-titled album's name is in many of the artist's titles, so it has to be close, not just
    contained: 'Ray Charles' isn't 'The Genius of Ray Charles'."""
    artists = {normalize(a) for a in [artist or "", *candidate_artists]}
    self_titled = any(normalize(without_suffix(a)) in artists for a in (album, candidate_album))
    if similar(album, candidate_album, contained_words=0 if self_titled else 2) < MATCH:
        return False
    if all(numbers(a) != numbers(b) for a, b in [(album, candidate_album), _without_suffixes(album, candidate_album)]):
        return False
    if artist is None:
        return True
    names = candidate_artists + [" & ".join(candidate_artists)]
    return any(similar(artist, name, contained_words=1) >= MATCH for name in names)


# --- MusicBrainz


NOT_ORIGINAL_ALBUMS = {"Compilation", "Live", "Remix", "Demo", "DJ-mix", "Mixtape/Street"}


def pick_release_group(search: dict | None, artist: str | None, album: str) -> str | None:
    """The search result that matches the page, preferring an album that isn't a compilation, live
    album or the like (soundtracks are fine), then the closest title, then the earliest (so
    reissues lose)."""
    matches = [g for g in (search or {}).get("release-groups", []) if _matches(g, artist, album)]
    plain = [g for g in matches if _is_original_album(g)]
    ranked = sorted(
        plain or matches, key=lambda g: (-similar(album, g["title"]), g.get("first-release-date") or "9999")
    )
    return ranked[0]["id"] if ranked else None


def pick_original_release(releases: list[dict]) -> str | None:
    """The original edition of the album: the earliest official release with the track count most
    releases share. The earliest release alone is often an odd one: Rubber Soul's is a 12-track
    Canadian LP, where most releases have the UK's 14 tracks."""
    if not releases:
        return None
    counts = Counter(_track_count(r) for r in releases)
    usual = [r for r in releases if _track_count(r) == counts.most_common(1)[0][0]]
    # "1969" sorts after "1969-11-28": a release with a full date is the better-documented one
    return min(usual, key=lambda r: (r.get("date") or "9999").ljust(10, "~"))["id"]


def _track_count(release: dict) -> int:
    return sum(m.get("track-count") or 0 for m in release.get("media") or [])


def _matches(group: dict, artist: str | None, album: str) -> bool:
    return names_match(artist, album, [c["name"] for c in group.get("artist-credit", [])], group["title"])


def _is_original_album(group: dict) -> bool:
    return group.get("primary-type") == "Album" and not NOT_ORIGINAL_ALBUMS & set(group.get("secondary-types") or [])


def original_tracks(original: dict | None) -> list[dict]:
    """The original release's tracks, in order: disc, track (number on the disc) and title."""
    return [
        {"disc": medium["position"], "track": i + 1, "title": t["title"]}
        for medium in (original or {}).get("media", [])
        for i, t in enumerate(medium.get("tracks") or [])
    ]


def genres(release_group: dict | None, top: int = 5) -> list[str]:
    """MusicBrainz genres for the album, most-voted first."""
    ranked = sorted((release_group or {}).get("genres", []), key=lambda g: (-g["count"], g["name"]))
    return [g["name"] for g in ranked[:top]]


# --- Spotify


def clean_title(title: str) -> str:
    """A track title without remaster and mono/stereo notes: 'Dreams - 2004 Remaster' -> 'dreams'."""
    return normalize(_without_remaster(title))


def _without_remaster(title: str) -> str:
    title = re.sub(r"\s-\s[^-]*\b(remaster(ed)?|mono|stereo)\b.*$", "", title, flags=re.IGNORECASE)
    return re.sub(r"\s*[(\[][^)\]]*\b(remaster(ed)?|mono|stereo)\b[^)\]]*[)\]]", "", title, flags=re.IGNORECASE)


def _track(album: dict, disc: int | None, track: int) -> dict | None:
    """A track by number. Without a disc, the number counts through the whole album."""
    tracks = album["all_tracks"]
    if disc is None:
        return tracks[track - 1] if 1 <= track <= len(tracks) else None
    return next((t for t in tracks if t["disc_number"] == disc and t["track_number"] == track), None)


def choose_edition(
    listen: dict, crawled: dict, favorites: list[tuple[int | None, int]], override: dict
) -> tuple[str | None, str | None, str | None]:
    """(Spotify album ID, how it was chosen, review reason).

    An override wins. Then a track link on the page, when that track's album is this album. Then
    the search results where the most of the written track numbers exist, preferring the track
    list most like the original release's (shared titles over all titles, so a deluxe edition
    loses to a plain one), then the track count nearest the original's, then Spotify's order. Numbers that don't exist on the chosen edition are flagged in
    silver_review.
    """
    albums = crawled["albums"]
    if override.get("spotify_album_id") in albums:
        return override["spotify_album_id"], "override", None
    candidates = [
        album_id
        for album_id, a in albums.items()
        if names_match(listen["artist"], listen["album"], [x["name"] for x in a["artists"]], a["name"])
    ]
    for album_id in crawled["linked_album_ids"]:
        if album_id in candidates:
            return album_id, "track_link", None
    if not candidates:
        return None, None, "no Spotify match"
    fit = {i: sum(_track(albums[i], disc, track) is not None for disc, track in favorites) for i in candidates}
    fitting = [i for i in candidates if fit[i] == max(fit.values())]
    original = [clean_title(t["title"]) for t in original_tracks(crawled["original"])]

    def likeness(album_id):
        titles = [clean_title(t["name"]) for t in albums[album_id]["all_tracks"]]
        shared = len(set(original) & set(titles))
        nearest = -abs(len(titles) - len(original)) if original else 0
        return shared / (len(titles) + len(set(original)) - shared), nearest

    return max(fitting, key=likeness), "search", None


def edition_tracks(album: dict, original: dict | None) -> list[dict]:
    """The chosen edition's tracks, each marked whether it's on the original release (None if unknown)."""
    on_original = _on_original(album["all_tracks"], original_tracks(original))
    return [
        {
            "position": i + 1,
            "disc": t["disc_number"],
            "track": t["track_number"],
            "title": t["name"],
            "artists": [a["name"] for a in t["artists"]],
            "duration_ms": t["duration_ms"],
            "spotify_track_id": t["id"],
            "on_original_album": on_original[i],
        }
        for i, t in enumerate(album["all_tracks"])
    ]


def _on_original(tracks: list[dict], original: list[dict]) -> list[bool | None]:
    """Whether each track is one of the original's: 85% similar, or one title inside the other,
    ignoring suffixes. So 'To Zion (feat. Carlos Santana)' is 'To Zion' and 'Steamroller Blues' is
    'Steamroller'. Each original track matches once, in order, so a later 'Dreams - Live' on a
    deluxe edition is a bonus track."""
    if not original:
        return [None] * len(tracks)
    unmatched = [t["title"] for t in original]
    result = []
    for t in tracks:
        title = _without_remaster(t["name"])
        titles = dict.fromkeys([title, without_suffix(without_suffix(title))])  # "Scenario (feat. ...) - LP Mix"
        scores = [max(similar(x, o, contained_words=1) for x in titles) for o in unmatched]
        best = max(range(len(scores)), key=scores.__getitem__, default=None)
        result.append(best is not None and scores[best] >= MATCH)
        if result[-1]:
            unmatched.pop(best)
    return result


def match_listen(
    listen: dict, crawled: dict, favorites: list[tuple[int | None, int]], override: dict, albums_1001: list[dict]
) -> dict:
    """Everything enrichment knows about one album listen: its Spotify edition and tracks, the
    MusicBrainz album, genres, the 1001 list entry, and anything to review."""
    if override.get("album"):  # the page's shorthand, like "IV", spelled out
        listen = {**listen, "album": override["album"]}
    album_id, method, review = choose_edition(listen, crawled, favorites, override)
    edition = crawled["albums"].get(album_id)
    group, original = crawled["release_group"], crawled["original"]
    reviews = [review] if review else []
    if group is None:
        reviews.append("no MusicBrainz match")
    original_year = int(group["first-release-date"][:4]) if group and group.get("first-release-date") else None
    if override.get("year"):  # e.g. a 78 album reissued as the LP the club played
        original_year = int(override["year"])
    if original_year and listen["year"] and abs(original_year - listen["year"]) > 1:
        reviews.append(f"MusicBrainz says {original_year}, the page says {listen['year']}")

    names = [(listen["artist"], listen["album"])]
    if edition:
        names.append((" & ".join(a["name"] for a in edition["artists"]), edition["name"]))
    if group:
        names.append((artist_credit(group), group["title"]))
    entry = match_1001(names, albums_1001)
    return {
        "spotify_album_id": album_id,
        "spotify_album": edition and edition["name"],
        "spotify_artists": [a["name"] for a in edition["artists"]] if edition else [],
        "spotify_release_date": edition and edition["release_date"],
        "spotify_match": method,
        "musicbrainz_release_group_id": group and group["id"],
        "original_title": group and group["title"],
        "original_artist": group and artist_credit(group),
        "original_year": original_year,
        "original_track_count": len(original_tracks(original)) or None,
        "genres": genres(group),
        "list_number": int(entry["list_number"]) if entry else None,
        "tracks": edition_tracks(edition, original) if edition else [],
        "review_reasons": reviews,
    }


def artist_credit(release_group: dict) -> str:
    """MusicBrainz's artist credit as written: 'Simon & Garfunkel'."""
    return "".join(c["name"] + c.get("joinphrase", "") for c in release_group.get("artist-credit", []))


# --- the 1001 Albums list


def match_1001(names: list[tuple[str | None, str]], albums_1001: list[dict]) -> dict | None:
    """The list entry matching any of an album's names (page, Spotify, MusicBrainz), or None.

    names are (artist, album) pairs; albums_1001 rows have list_number, artist and album.
    """
    best, best_score = None, 0.0
    for row in albums_1001:
        for artist, album in names:
            if not names_match(artist, album, [row["artist"]], row["album"]):
                continue
            score = min(similar(album, row["album"]), similar(artist, row["artist"]) if artist else 1.0)
            if score >= best_score:
                best, best_score = row, score
    return best
