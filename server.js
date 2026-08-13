/* AIS relay — holds the aisstream key server-side and republishes tanker
   positions as plain CORS-open JSON the dashboard can poll.

   aisstream refuses browser origins, so the page cannot talk to it directly.
   This process keeps one long-lived socket open, remembers the latest position
   per vessel, and serves them at GET /tankers.

   Run:  AIS_KEY=your-key node server.js
*/

import http from 'http';
import fs from 'fs';
import WebSocket from 'ws';

const KEY = process.env.AIS_KEY;
const PORT = process.env.PORT || 8080;
if (!KEY) { console.error('Set AIS_KEY'); process.exit(1); }

// One box while we confirm the stream feeds at all — aisstream appears to accept
// only a small number, and eight produced silence. Narrow once data flows.
const BOXES = [
  [[-90, -180], [90, 180]]
];

/* The lanes the globe draws, for when per-region boxes work again:
  [[10, 45], [32, 62]],     // Gulf, Hormuz, Gulf of Oman
  [[10, 32], [32, 44]],     // Red Sea, Suez, Bab el-Mandeb
  [[-8, 95], [10, 110]],    // Malacca, Singapore
  [[18, 105], [42, 132]],   // South China Sea to Japan
  [[30, -10], [46, 37]],    // Mediterranean, Adriatic, Black Sea
  [[48, -12], [62, 30]],    // North Sea, Baltic
  [[18, -98], [32, -60]],   // US Gulf, Caribbean
  [[-40, 10], [10, 25]]     // West Africa, Cape route
*/

const SHIP_TYPE_TANKER = t => t >= 80 && t <= 89;
const STALE_MS = 40 * 60 * 1000;

const vessels = new Map();   // mmsi -> { lat, lon, cog, name, type, at }

let backoff = 5000;                 // aisstream allows one socket per key; a tight
let seenMsgs = 0;                   // retry loop earns a 429, so back off on failure
let seenTotal = 0;

function connect() {
  const ws = new WebSocket('wss://stream.aisstream.io/v0/stream');

  ws.on('open', () => {
    console.log('aisstream connected');
    // their own example uses `Apikey`; docs elsewhere say `APIKey`. Send both —
    // whichever the server reads, the other is ignored.
    const sub = {
      Apikey: KEY,
      APIKey: KEY,
      BoundingBoxes: BOXES,
      FilterMessageTypes: ['PositionReport', 'ShipStaticData']
    };
    console.log('subscribing:', JSON.stringify(sub).replace(KEY, 'KEY'));
    ws.send(JSON.stringify(sub));
  });

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch (e) { console.log('non-JSON:', String(raw).slice(0, 200)); return; }
    if (seenMsgs < 3) { console.log('msg', seenMsgs, JSON.stringify(m).slice(0, 300)); seenMsgs++; }
    seenTotal++;
    if (m.error || m.Error) { console.log('aisstream error:', m.error || m.Error); return; }
    backoff = 5000;                 // a real message means the key is good
    const meta = m.MetaData || {};
    const mmsi = meta.MMSI || meta.MMSI_String;
    if (!mmsi) return;
    const prev = vessels.get(mmsi) || {};

    if (m.MessageType === 'PositionReport') {
      const p = m.Message.PositionReport;
      vessels.set(mmsi, {
        ...prev,
        lat: p.Latitude, lon: p.Longitude,
        cog: p.TrueHeading < 360 ? p.TrueHeading : p.Cog,
        sog: p.Sog,
        name: (meta.ShipName || prev.name || '').trim(),
        at: Date.now()
      });
    } else if (m.MessageType === 'ShipStaticData') {
      const s = m.Message.ShipStaticData;
      vessels.set(mmsi, {
        ...prev,
        type: s.Type,
        dest: (s.Destination || '').trim(),
        draught: s.MaximumStaticDraught,
        name: (s.Name || prev.name || '').trim()
      });
    }
  });

  ws.on('close', () => {
    console.log('closed, retrying in ' + Math.round(backoff / 1000) + 's');
    setTimeout(connect, backoff);
    backoff = Math.min(backoff * 2, 120000);
  });
  ws.on('error', (e) => { console.log('error', e.message); try { ws.close(); } catch (x) {} });
}
connect();

setInterval(() => {
  const cut = Date.now() - STALE_MS;
  for (const [k, v] of vessels) if ((v.at || 0) < cut) vessels.delete(k);
}, 60000);

// heartbeat so silence is distinguishable from a stalled process
setInterval(() => console.log('heartbeat · ' + seenTotal + ' msgs · ' + vessels.size + ' vessels'), 30000);

/* ── Military air sampler ───────────────────────────────────────────────────
   The dashboard used to record airborne counts itself, which meant it only
   sampled while a browser tab was open — a day covered for three hours was
   plotted beside a day covered for twelve. This process runs continuously, so
   it samples on a fixed cadence from one source and the history is comparable
   across days. Counts use the same airframe filter as the page.

   GET /milhistory returns per-UTC-day figures plus the raw samples.
*/
const MIL_FILE = process.env.MIL_FILE || './mil-history.json';
const MIL_EVERY = 5 * 60 * 1000;
const MIL_KEEP = 21 * 24 * 3600 * 1000;

// same list the page walks, minus the text proxy: from a server the mirrors answer directly
const MIL_SOURCES = [
  { name: 'adsb.fi', url: 'https://opendata.adsb.fi/api/v2/mil' },
  { name: 'adsb.lol', url: 'https://api.adsb.lol/v2/mil' },
  { name: 'airplanes.live', url: 'https://api.airplanes.live/v2/mil' }
];

/* Large airframes only — tankers, ISR and electronic warfare, transports. Keep in
   step with BIG_TYPES in news-events.js or the served history won't match the globe. */
const BIG_TYPES = /^(C135|K35R|R135|KC30|A332|A333|A339|A310|B703|E3TF|E3CF|E3|E6|E8|P8|P3|U2|RC12|E4|VC25|C17|C5M|C5|C30J|C130|C160|A400|C40|C32|C37|B752|B763|B77|IL76|IL78|AN12|AN24|AN26|AN30|AN72|AN124|A124|CL60|GLF|G5)/i;

let milSamples = [];                 // { t, n, src }
let milSeen = new Map();             // utc day -> Set of hex, for distinct-airframe counts
let milSrc = 0;

const utcDay = (t) => new Date(t).toISOString().slice(0, 10);

function milLoad() {
  try {
    const j = JSON.parse(fs.readFileSync(MIL_FILE, 'utf8'));
    const cut = Date.now() - MIL_KEEP;
    milSamples = (j.samples || []).filter(p => p && p.t > cut);
    for (const d in (j.seen || {})) milSeen.set(d, new Set(j.seen[d]));
    console.log('mil history loaded · ' + milSamples.length + ' samples');
  } catch (e) { console.log('mil history: starting empty'); }
}

function milSave() {
  const seen = {};
  for (const [d, s] of milSeen) seen[d] = [...s];
  try { fs.writeFileSync(MIL_FILE, JSON.stringify({ samples: milSamples, seen })); }
  catch (e) { console.log('mil history write failed:', e.message); }
}

async function milFetch(src) {
  const ctl = new AbortController();
  const bail = setTimeout(() => ctl.abort(), 15000);
  try {
    const r = await fetch(src.url, { signal: ctl.signal, headers: { 'accept': 'application/json' } });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const j = await r.json();
    return j.ac || j.aircraft || [];
  } finally { clearTimeout(bail); }
}

async function milSample() {
  const order = [milSrc, ...MIL_SOURCES.map((_, i) => i).filter(i => i !== milSrc)];
  for (const i of order) {
    try {
      const list = await milFetch(MIL_SOURCES[i]);
      const air = list.filter(a =>
        typeof a.lat === 'number' && a.alt_baro !== 'ground' && BIG_TYPES.test(a.t || ''));
      if (!list.length) throw new Error('empty');
      milSrc = i;
      const t = Date.now();
      milSamples.push({ t, n: air.length, src: MIL_SOURCES[i].name });
      const day = utcDay(t);
      const set = milSeen.get(day) || new Set();
      for (const a of air) if (a.hex) set.add(a.hex);
      milSeen.set(day, set);

      const cut = t - MIL_KEEP, cutDay = utcDay(cut);
      milSamples = milSamples.filter(p => p.t > cut);
      for (const d of [...milSeen.keys()]) if (d < cutDay) milSeen.delete(d);
      milSave();
      console.log('mil sample · ' + air.length + ' airborne · ' + MIL_SOURCES[i].name);
      return;
    } catch (e) {
      console.log('mil source ' + MIL_SOURCES[i].name + ' failed: ' + e.message);
    }
  }
  console.log('mil sample skipped — every source refused');
}

milLoad();
milSample();
setInterval(milSample, MIL_EVERY);

/** Per-day figures. `samples` is the day's coverage — a low count means a weak
    estimate, and the page greys those days out rather than pretending. */
function milDays() {
  const by = new Map();
  for (const p of milSamples) {
    const d = utcDay(p.t);
    const b = by.get(d) || { sum: 0, n: 0, peak: 0, srcs: new Set() };
    b.sum += p.n; b.n++; b.srcs.add(p.src);
    if (p.n > b.peak) b.peak = p.n;
    by.set(d, b);
  }
  const out = [];
  for (const [day, b] of [...by].sort((a, z) => a[0] < z[0] ? -1 : 1)) {
    out.push({
      day, samples: b.n, mean: b.sum / b.n, peak: b.peak,
      distinct: (milSeen.get(day) || new Set()).size,
      sources: [...b.srcs]
    });
  }
  return out;
}

http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.url.startsWith('/milhistory')) {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({
      every: MIL_EVERY, now: Date.now(),
      days: milDays(), samples: milSamples
    }));
    return;
  }
  if (req.url.startsWith('/tankers')) {
    const out = [];
    for (const [mmsi, v] of vessels) {
      if (v.lat == null) continue;
      if (v.type != null && !SHIP_TYPE_TANKER(v.type)) continue;
      out.push({ id: mmsi, lat: v.lat, lon: v.lon, cog: v.cog || 0, sog: v.sog || 0,
                 name: v.name || '', dest: v.dest || '', at: v.at });
    }
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(out));
    return;
  }
  res.end('relay up · ' + vessels.size + ' vessels held · '
    + milSamples.length + ' air samples');
}).listen(PORT, () => console.log('listening on ' + PORT));
