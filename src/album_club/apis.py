"""Small clients for the Spotify Web API and the MusicBrainz API, using only the standard library.

Requests are identified by (source, request), where request is the path and query string. That
pair is the key in the bronze_api_responses cache, so the pipeline can look up exactly what the
fetch job asked for.
"""

import base64
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request

SPOTIFY = "spotify"
MUSICBRAINZ = "musicbrainz"
BASES = {SPOTIFY: "https://api.spotify.com", MUSICBRAINZ: "https://musicbrainz.org"}
# MusicBrainz asks every client to identify itself with a name and a way to get in touch.
USER_AGENT = "album-club/0.1 ( https://github.com/mscottreed/album-club )"


def spotify_search(artist: str | None, album: str) -> str:
    """A plain-words album search; Spotify's field filters find nothing when the words have punctuation.
    It returns at most 10 albums per request (since February 2026)."""
    q = re.sub(r"[^\w]+", " ", f"{album} {artist or ''}").strip()
    return "/v1/search?" + urllib.parse.urlencode({"q": q, "type": "album", "limit": 10})


def spotify_track(track_id: str) -> str:
    return f"/v1/tracks/{track_id}"


def spotify_album(album_id: str) -> str:
    return f"/v1/albums/{album_id}"


def spotify_album_tracks(album_id: str, offset: int) -> str:
    """Albums return their first 50 tracks; longer albums need this for the rest."""
    return f"/v1/albums/{album_id}/tracks?limit=50&offset={offset}"


def musicbrainz_release_group_search(artist: str | None, album: str) -> str:
    query = f'releasegroup:"{_lucene(album)}"' + (f' AND artist:"{_lucene(artist)}"' if artist else "")
    return "/ws/2/release-group?" + urllib.parse.urlencode({"query": query, "limit": 10, "fmt": "json"})


def musicbrainz_release_group_loose_search(artist: str | None, album: str) -> str:
    """Without exact phrases, so misspellings on the page ("Alanis Morrisette") still find something."""
    query = _lucene(f"{album} {artist or ''}").strip()
    return "/ws/2/release-group?" + urllib.parse.urlencode({"query": query, "limit": 25, "fmt": "json"})


def musicbrainz_release_group(release_group_id: str) -> str:
    return f"/ws/2/release-group/{release_group_id}?inc=genres+artist-credits&fmt=json"


def musicbrainz_releases(release_group_id: str, offset: int) -> str:
    """Official releases in a release group (every edition of the album) with their track counts, 100 at a time."""
    return (
        f"/ws/2/release?release-group={release_group_id}&status=official&inc=media&limit=100&offset={offset}&fmt=json"
    )


def musicbrainz_release(release_id: str) -> str:
    return f"/ws/2/release/{release_id}?inc=recordings&fmt=json"


def _lucene(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"').replace("/", "\\/")


class Client:
    """Makes requests politely: one per second to MusicBrainz, retrying when told to slow down."""

    def __init__(self, spotify_client_id: str, spotify_client_secret: str):
        self._spotify_credentials = (spotify_client_id, spotify_client_secret)
        self._spotify_token = None
        self._last_musicbrainz = 0.0

    def get(self, source: str, request: str) -> tuple[int, str]:
        """(HTTP status, response body) for one request."""
        for attempt in range(6):
            headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
            if source == SPOTIFY:
                headers["Authorization"] = f"Bearer {self._token()}"
            else:
                time.sleep(max(0.0, self._last_musicbrainz + 1.1 - time.monotonic()))
                self._last_musicbrainz = time.monotonic()
            try:
                request_obj = urllib.request.Request(BASES[source] + request, headers=headers)
                with urllib.request.urlopen(request_obj, timeout=30) as r:
                    return r.status, r.read().decode()
            except urllib.error.HTTPError as e:
                if e.code == 401 and source == SPOTIFY:
                    self._spotify_token = None  # tokens last an hour
                elif e.code in (429, 500, 502, 503, 504):
                    time.sleep(int(e.headers.get("Retry-After") or 2 ** (attempt + 1)))
                else:
                    return e.code, e.read().decode()
            except (urllib.error.URLError, ConnectionError, TimeoutError):  # connection reset, timeout
                time.sleep(2 ** (attempt + 1))
        raise RuntimeError(f"{source} {request} still failing after retries")

    def _token(self) -> str:
        """A Client Credentials token: enough for search and catalog lookups, no user involved."""
        if self._spotify_token is None:
            basic = base64.b64encode(":".join(self._spotify_credentials).encode()).decode()
            request = urllib.request.Request(
                "https://accounts.spotify.com/api/token",
                data=b"grant_type=client_credentials",
                headers={"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"},
            )
            with urllib.request.urlopen(request) as r:
                self._spotify_token = json.load(r)["access_token"]
        return self._spotify_token
