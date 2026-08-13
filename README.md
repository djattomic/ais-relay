# Air history service

The dashboard's 7-day aircraft chart used to be recorded in the browser, which
only samples while a tab is open — a day covered for three hours ended up plotted
beside a day covered for twelve. This service samples continuously instead.

Deployed on Render at `https://ais-relay-ooxw.onrender.com`, from
github.com/djattomic/ais-relay (`server.js` at the repo root).

```
npm start          # or: node server.js
```

No API key and no dependencies — Node's built-in fetch and http only.

## Endpoints

- `GET /milhistory` — `{days, samples}`: per-UTC-day mean, peak, sample count,
  distinct airframes seen, and which mirrors answered. Sampled every 5 minutes
  from the first mirror that responds (adsb.fi, adsb.lol, airplanes.live), using
  the same large-airframe filter as the globe.
- `GET /` — one-line status.

History is kept in `mil-history.json` beside the process, so a restart resumes
with it intact. Render wipes it on redeploy, so each deploy restarts the record.

## Wire it to the dashboard

The page already points at the deployed URL. To aim it elsewhere, in the
dashboard's console, once, then reload:

```
localStorage.setItem('relayUrl', 'https://your-relay.onrender.com')
```

If the service is unreachable the page falls back to the counts it records itself
while open, and the note under the chart says so.

## Two things to know

A free Render instance sleeps after 15 minutes without an HTTP request and stops
sampling until woken. Point a free uptime pinger at `/` every 10 minutes if you
want unbroken days.

`BIG_TYPES` is duplicated in the dashboard's `news-events.js`. Change both
together or the served history stops matching what the globe draws.

## Removed: aisstream tanker socket

This service began as an AIS relay holding an aisstream key server-side, because
aisstream refuses browser origins. The socket connected and the subscription was
accepted, but no messages ever arrived — with both `APIKey` and `Apikey`
spellings and a whole-world bounding box — and latterly the service answered the
socket with 429s. The code was removed rather than left retrying in the logs; it
is in git history if aisstream ever replies. The dashboard's tankers come from
Digitraffic directly (Baltic and Gulf of Finland coverage).
