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
- A ratings table filled in on a page after the export can be copied from OneNote as
  tab-separated text into `ratings_tables/W###.tsv` in the same volume (W153 and W156). The
  pasted table replaces that week's table from the page and goes through the same rules. It's
  for meetings from before this project started; later ones go in the app (post 7).

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
- **Blank favorites (or `0`) with a rating means no favorites.** The raw text is always kept,
  and rows with notes the parser can't interpret (`(lyrics)`, `(bass)`, `no standouts`) are
  flagged.

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
  thinly. The list (2021 edition, 1,089 albums) is a seed in git. An album matches a list entry
  when any of its names (the page's, Spotify's or MusicBrainz's) is at least 85% similar to the
  entry in both artist and title, ignoring case, punctuation, a leading "The", "&" vs "and", and
  edition suffixes like "(Remastered)". Several names are compared because plain page names miss
  over spellings ("Alanis Morrisette", "Red Hot Chilli Peppers").
- The `#N` in early titles is kept as written (`list_number_in_title`). It comes from a different
  edition of the list than the seed: `#201` *Fun House* is 200 in the seed.
- **Spotify is the source** (post 5), because the club listens on Spotify and favorite track
  numbers refer to the edition actually played. An album is identified by the Spotify album ID
  of that edition, chosen in this order:
  1. an `album_overrides` file in git;
  2. a Spotify track link on the page (64 of 163 meeting pages have one), when that track's
     album matches the page's artist and title; and
  3. a Spotify search on artist and title, keeping the editions where the most favorite and
     excluded track numbers exist. Among those, the track list most like the original release's
     (below) wins: shared titles divided by all titles across both lists, so a 60-track super
     deluxe edition loses to the plain remaster. Then the track count nearest the original's,
     then Spotify's order.

  A Spotify, MusicBrainz or 1001 list name matches the page's at 85% similarity, as above, with
  three more rules: a name found whole inside the other counts ("James Brown" in "James Brown &
  The Famous Flames"; an album title needs at least two words for this); a self-titled album
  has to be close, not just contained ("Ray Charles" isn't "The Genius of Ray Charles"); and
  names with different Roman numerals or volume numbers never match ("Van Halen" isn't "Van
  Halen II").

  Some pages use shorthand the matching can't read (Led Zeppelin's "I", "II" and "IV", "NFR").
  `album_overrides` gives the name to search for, and optionally the Spotify album and the
  MusicBrainz release group. The page's name stays as written in `silver_listens`. It can also
  give the original year, when MusicBrainz's isn't the album the club means: W099's Billie
  Holiday is 1957, the Verve LP the club played, not 1947, the Commodore 78 album it reissued
  with four more tracks.

  An album with no match, or a track number that isn't on the chosen edition, is flagged for
  review.
- **Track numbers** without a disc count through the whole edition (track 15 of a two-disc album
  is the fifteenth track). With a disc (`Disc 2: 3`), they count within that disc.
- **MusicBrainz** finds the original release. Its release group ties every edition of an album
  together: the search picks the group matching the page's artist and title, preferring a plain
  album over compilations, live albums and the like. The original is the earliest official
  release with the track count most of the group's releases share: the earliest release alone is
  often an odd one, like Rubber Soul's 12-track Canadian LP. A year-only date sorts after full
  dates in that year. The rule goes wrong when expanded reissues outnumber the original (most
  *Night Visions* releases have 20 tracks), so `album_overrides` pins those editions.
- A track on the Spotify edition is **on the original album** when its title, minus remaster and
  mono/stereo notes, is at least 85% similar to one of that release's titles, or one title is
  found whole inside the other, ignoring suffixes like "(feat. ...)" and "- LP Mix". So "To Zion
  (feat. Carlos Santana)" is "To Zion" and "Steamroller Blues" is "Steamroller". Each original
  track matches once, in edition order, so a later "Dreams - Live" on a deluxe edition is a bonus
  track. MusicBrainz's own titles occasionally defeat this: two "[untitled]" tracks on *(What's
  the Story) Morning Glory?*, and censored titles on *Norman Fucking Rockwell!*.
- An album whose MusicBrainz year is more than a year off the page's is flagged.
- **Genres** are MusicBrainz's genre tags for the release group, the five most-voted. Spotify's
  album genres are almost always empty.
- **"Likes the rest"** becomes a liked row for every track on the edition that the person didn't
  rank or exclude.
- A playlist's ratings and favorites refer to the playlist (favorite numbers are playlist
  positions) and are excluded from album stats. Spotify only returns a playlist's tracks to its
  owner, so both playlists' track lists are in a `playlist_tracks` seed: W122's copied from its
  page, W107's ("Mellower Chains", 11 tracks) from the playlist's public Spotify embed page.
- Other mentions of "playlist" aren't listens: W051's Spotify playlist link is part of the band
  notes, and W093 mentions the film *Nick and Norah's Infinite Playlist*.
- API responses are kept raw: the fetch task writes them as JSON-lines files to the
  `api_responses` volume, and the pipeline loads them into `bronze_api_responses`, one row per
  request. Matching runs only on those responses, so re-running it never calls the APIs, and a
  later fetch only asks for what's missing. Errors other than 404 aren't kept, so they're retried.

## Gold and the dashboard

- Gold is a star schema of SQL materialized views (`src/pipeline/gold.sql`), albums only:
  `dim_person`, `dim_album`, `dim_track`, `fact_ratings` (person × album) and
  `fact_track_picks` (person × track). Playlist ratings and favorites stay in silver.
- `dim_album` shows names as the club wrote them (`artist`, `album`, `label`), with
  MusicBrainz's names alongside. `dim_track` titles drop Spotify's remaster and mono/stereo
  notes ("Dreams - 2004 Remaster" is "Dreams"); `spotify_title` keeps them.
- Aggregates are precomputed so the dashboard's queries only select: album averages
  (`agg_album_ratings`, guests included), rankings, overall and per-decade averages, and each
  member's unrated albums. Rankings and averages have a row for each person and one for
  `Club`, everyone together. The club's overall average is over all ratings, so albums more
  people rated count more.
- A rank is by rating, highest first, and tied ratings share a rank, so a top 10 includes every
  album tied at the cutoff.
- The dashboard picks a person with a parameter rather than a filter, so there's no "All"
  mixing everyone's rows; `Club` is the everyone view. The Album page starts with no album.
