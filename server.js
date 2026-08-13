/* Military air history service.

   The dashboard used to record airborne counts in the browser, which meant it
   only sampled while a tab was open — a day covered for three hours was plotted
   beside a day covered for twelve. This process runs continuously, samples on a
   fixed cadence from one source, and serves a history that is comparable across
   days. Counts use the same airframe filter as the page.

     GET /milhistory   per-UTC-day mean, peak, sample count, distinct airframes
     GET /             one-line status

   Run:  node server.js

   The aisstream tanker socket that used to live here was removed: the service
   accepted the subscription and then sent nothing for weeks, and latterly refused
   the socket with 429s. Global AIS is on hold until they answer; the dashboard's
   tankers come from Digitraffic directly. Previous version is in git history.
*/

import http from 'http';
import fs from 'fs';

const PORT = process.env.PORT || 8080;
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
      if (!list.length) throw new Error('empty');
      const air = list.filter(a =>
        typeof a.lat === 'number' && a.alt_baro !== 'ground' && BIG_TYPES.test(a.t || ''));
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
    estimate, and the page draws those days faint rather than pretending. */
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
  const last = milSamples[milSamples.length - 1];
  res.end('air history up · ' + milSamples.length + ' samples'
    + (last ? ' · last ' + last.n + ' airborne from ' + last.src : ''));
}).listen(PORT, () => console.log('listening on ' + PORT));
