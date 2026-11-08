# Data decisions

How the album club's OneNote history is interpreted. Decided while planning post 3, from a
profile of the full export (272 pages, 163 of them meeting pages). The parser in post 4 and
the enrichment in post 5 implement these rules.

## Source

- The history comes from one Windows OneNote export of the **Music** section, saved as a Single
  File Web Page (`.mht`). OneNote for Mac can only save PDF, so the export needs Windows.
- OneNote is history only. It's still used to run meetings, but new ratings are entered in the
  app (post 7), so the export is imported once rather than on a schedule.
- The raw export holds full names and lives only in the private Unity Catalog volume and a
  gitignored local `data/` folder, never in git.

## Pages

- Each OneNote page is a top-level `<div>` in the export, with the title, then the created date
  and time, at the top.
- **Meeting pages** have titles starting `W###`, in two styles:
  - early: `W001 - #1 - Frank Sinatra - In the Wee Small Hours (1955)`, where `#1` is the album's
    number in the *1001 Albums* list
  - later: `W159 - 1959 - Miles Davis - Kind of Blue`
- Everything else (planning lists, cycles, Top 3s, concerts, `W17 - 1987 (602)`-style planning
  pages) is not a meeting.
- `W165` (created between W155 and W157) is treated as `W156`.
- On W056 and W059 the export turned the whole page into images, and the text survives only in
  each image's `alt` attribute, one table cell per line. When a meeting page has no ratings table,
  the parser reads the table from the image whose alt text starts with the table header.
- Each album has its own page. A meeting with several albums has one page per album, all with the
  same week number (W003, W109, W133).

## Meetings and dates

- A meeting can cover more than one album (W003, W109 and W133 did, when albums were short or
  related). A rating belongs to a person and a listen (see Albums and playlists).
- OneNote records when a page was created, not when the meeting happened. The meeting date is:
  1. a date written in the page body that falls within 3 weeks after the page was created, if
     there is one; otherwise
  2. the first Sunday after the page was created that's also after the previous meeting
     (meetings were almost always on Sundays), working in week-number order and counting from
     the last known date so one early-created page can't push later weeks off; and
  3. a `meeting_dates` override file in git for corrections.
- Each meeting records which of those its date came from. The old dataset's dates are used as a
  cross-check only: they're "first Sunday after creation" without the one-meeting-per-Sunday
  constraint, and go backwards in week order in five places.

## Ratings

- The ratings table on a meeting page is the one whose header has a `Name` column and a column
  containing "Rating" (usually `Name | Album Rating | Favorite songs`).
- Ratings are 0–10 with decimals. The raw text is always kept next to the parsed number.
- A **blank rating means the person wasn't there**, and they're marked absent, which gives
  attendance. Favorites can still be filled in: at W056 Bill R wasn't at the meeting and sent his
  favorite (13) afterwards. Those favorites are kept, with no rating.
- Question marks are dropped and an `x` after the decimal point counts as `.0`:
  `8?` → 8, `?8.X?` → 8, `3.x` → 3.
- Two meetings had a playlist alongside the album (see Albums and playlists), and each person
  rated whichever they listened to, sometimes both. W107 has separate `Album Rating` and
  `Playlist Rating` columns. W122 has one rating column with values marked `(A)` or `(P)`
  (`8.4 (A), 9.8 (P)` is both) and separate album and playlist favorites columns. On W107's single
  favorites column, favorites go with whichever listen the person rated.

## Favorite songs

- Favorites are **track numbers, in ranked order** (first favorite first).
- Track numbers refer to the version actually listened to, usually on Spotify, which can differ
  from the original album. Numbers beyond the original album's track count are flagged as not on
  the original album, not mapped to the wrong song.
- Parentheses usually mark a tie and are ignored (numbers keep the order written). Sometimes
  (W134) they mark bonus tracks from the playlist, which the rule above catches.
- `...` with `x`-prefixed numbers means "liked all except": `2, 3, 1, ... (x5, 9, 10)` is ranked
  favorites 2, 3, 1, every other track liked, and 5, 9, 10 excluded. Written-out forms (`all
  except 8`, `all but (10, 6)`, `6, all others`) mean the same, and a note starting `(all` means
  every track was liked.
- Each rating stores ranked favorites, a "likes the rest" flag, and excluded tracks. Expanding
  "the rest" into tracks needs the album's track count, which comes from enrichment (post 5).
- Marks on a track number are dropped and the track still counts: movements (`1ii-v`), letters
  (`10b`), names (`4-Dirt`) and asterisks (`3*`, which at W046 meant a song Scott finds
  overplayed).
- W146 (*The White Album*) labels tracks by disc: `Disc 1: 4, 7 … Disc 2: 1, 3`.
- W010 (*69 Love Songs*, three volumes) has two unlabeled columns after "Favorite songs". The
  three columns are the volumes, so they're read as discs 1, 2 and 3. Nobody picked favorites
  from Volume 3.
- **Blank favorites with a rating means no favorites.** The raw text is always kept, and rows
  with notes the parser can't interpret (`(lyrics)`, `(bass)`, `no standouts`) are flagged.

## People

- Names are shown as first name + last initial, and full names never go in git.
- A `people` seed in git lists each person and their role, nothing else:
  - members: Bill L, Bill R, Chantal R, Charlotte R, Julia R, Linda L, Scott R
  - guests: Claudia D, Craig M, Danielle L, Greg J, Roland R
- The parser matches a name as written by first name, plus the last initial when one is written:
  `Linda Lackey`, `Linda L` and `Linda` all match Linda L (the only Linda), and `Bill Lackey` or
  `Bill L` matches Bill L. A name that matches no one, or more than one person, is flagged.
- Album averages and rankings include guests. Per-person stats and comparisons default to
  members only, with guests available as a filter.

## Albums and playlists

- A meeting has one or more **listens**. A listen is an album or a playlist. Most meetings are
  one album, and W003, W109 and W133 were several albums.
- A listen can be part of an album. At W133 the club played only some tracks of each of its three
  albums, and the line of track numbers under each ratings table (`1, 4, 7, 12`) lists the tracks
  played. Favorites are still album track numbers. Each listen records the tracks played when the
  page lists them, and "likes the rest" means the rest of those tracks, not the whole album.
- W107 (*Dirt*) and W122 (*Superunknown*) also had a playlist, an alternative for members who
  don't like grunge. Each person chose the album, the playlist or both, so these meetings have
  two listens and each rating says which one it's for. No meeting was a playlist only.
- Artist, title, year and (early on) the *1001 Albums* number come from the page title. The year
  in the title is the album's original year.
- Each album records its **selection method** by era, with boundaries taken from where the cycle
  pages sit in the section:

  | Weeks | Selection method |
  |---|---|
  | W001–W038 | *1001 Albums* list, worked through in order |
  | W039–W069 | Democratic (voted from members' Top 3 Albums) |
  | W070–W085 | Democratic to Autocratic (remaining voted albums plus Scott's supplements) |
  | W086 onward | Autocratic (chosen by Scott, a few by Julia) |

  A picker is recorded only where it's known. No stats are planned on picks.
- Being **on the *1001 Albums* list** is separate from the selection method: almost every album
  is on it, in every era. The exceptions are mostly post-2000 albums, which the list covers
  thinly. The list (2021 edition, 1,089 albums) is a seed in git. Matching albums to it waits for
  enrichment (post 5): plain name matching misses many albums over spellings ("Alanis Morrisette",
  "Red Hot Chilli Peppers"), and matching on Spotify and MusicBrainz names will be more reliable.
- The `#N` in early titles is kept as written (`list_number_in_title`). It comes from a different
  edition of the list than the seed: `#201` *Fun House* is 200 in the seed.
- **Spotify is the source** (post 5), because the club listens on Spotify and favorite track
  numbers refer to the edition actually played. An album is identified by the Spotify album ID
  of that edition, found by:
  1. a Spotify track link on the page (64 of 163 meeting pages have one), when that track's
     album matches the page's artist and title;
  2. otherwise a Spotify search on artist, title and year, preferring an edition whose track
     count fits the favorites; and
  3. an `album_overrides` file in git.
- A playlist's ratings and favorites refer to the playlist (favorite numbers are playlist
  positions) and are excluded from album stats. W122's page lists its 16 tracks; W107's page has
  no track list or link, so its Spotify playlist ID goes in a `listens` override file.
- Other mentions of "playlist" aren't listens: W051's Spotify playlist link is part of the band
  notes, and W093 mentions the film *Nick and Norah's Infinite Playlist*.
- MusicBrainz will probably be needed alongside Spotify (post 5): its release group ties every
  edition of an album together, and from it the original release's track list can be found. Where
  possible, pick the Spotify edition whose track list matches the original release. MusicBrainz
  also has genre tags, which could matter for gold stats if Spotify's genre data is too thin.
