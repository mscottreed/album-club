# Secrets

The bundle creates one secret scope per target, named after the schema: `dev_<user>_album_club`
in dev and `album_club` in prod. The values never go in git. Put them in by hand after the first
deploy of each target.

## Spotify

The `fetch_enrichment` task uses a Spotify app's Client Credentials (search and catalog lookups,
no user sign-in).

1. At [developer.spotify.com/dashboard](https://developer.spotify.com/dashboard), create an app
   (or reuse one) with the Web API enabled. In Development Mode, the app owner needs Spotify
   Premium.
2. Put its client ID and client secret in the scope. `put-secret` prompts for the value, so it
   doesn't end up in your shell history:

```bash
SCOPE=album_club   # or dev_<user>_album_club for dev
databricks secrets put-secret $SCOPE spotify-client-id
databricks secrets put-secret $SCOPE spotify-client-secret
```

MusicBrainz needs no key. Its client identifies itself with the repo URL, as MusicBrainz asks.
