# AIS relay

aisstream.io refuses browser origins, so the dashboard cannot call it directly.
This small service holds the key, keeps one socket open to aisstream, and
republishes tanker positions as CORS-open JSON at `GET /tankers`.

## Run locally

```
cd ais-relay
npm install
AIS_KEY=your-key npm start
```

Then open http://localhost:8080/tankers — it fills within a minute.

## Host it free

Any of these work. All read `AIS_KEY` from an environment variable and run
`npm start`.

**Render** — New → Web Service → connect repo or upload → Environment: add
`AIS_KEY` → deploy. Free tier sleeps when idle; first request wakes it.

**Railway** — New Project → Deploy from repo → Variables → add `AIS_KEY`.

**Fly.io** — `fly launch`, then `fly secrets set AIS_KEY=your-key`, `fly deploy`.

## Wire it to the dashboard

Once deployed you get a URL like `https://ais-relay.onrender.com`.
Send it over and it replaces the Digitraffic call in `live-data.js` —
one line. Global tanker coverage replaces the Baltic-only feed.

## Notes

- Bounding boxes in `server.js` match the lanes the globe draws. Edit `BOXES`
  to widen or narrow coverage.
- Vessels unheard from for 40 minutes are dropped.
- Ship types 80-89 are tankers; vessels whose static data has not arrived yet
  are included until their type is known.
