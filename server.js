/* Military air history service.

   The dashboard used to record airborne counts in the browser, which meant it
   only sampled while a tab was open — a day covered for three hours was plotted
   beside a day covered for twelve. This process runs continuously, samples on a
   fixed cadence from one source, and serves a history that is comparable across
   days. Counts use the same airframe filter as the page.

     GET /milhistory   per-UTC-day mean, peak, sample count, distinct airframes
     GET /flow         gate-crossing record: tanker level, cargo throughput, per gate
     GET /quotes       spot + 24h change for indices, metals and oil (Stooq)
     GET /             one-line status

   Run:  node server.js

   The aisstream tanker socket that used to live here was removed: the service
   accepted the subscription and then sent nothing for weeks, and latterly refused
   the socket with 429s. Global AIS is on hold until they answer; the dashboard's
   tankers come from Digitraffic directly. Previous version is in git history.
*/

import http from 'http';
import fs from 'fs';
import zlib from 'zlib';
import crypto from 'crypto';

const PORT = process.env.PORT || 8080;
const MIL_FILE = process.env.MIL_FILE || './mil-history.json';
const FLOW_FILE = process.env.FLOW_FILE || './flow-history.json';
const MIL_EVERY = 5 * 60 * 1000;
const MIL_KEEP = 21 * 24 * 3600 * 1000;
const DAY = 24 * 3600 * 1000;
const FLOW_KEEP = 45 * DAY;          // the 30-day cargo baseline needs support behind it
const VISIT_GAP = 90 * 60 * 1000;    // same airframe inside the same gate: one crossing, not one per sample
/* A point-in-box test only catches an airframe that is inside the box at the instant of
   a sample; on a 5-minute cadence a transport covers 40nm between samples, so a transit
   across a gate corner can fall between two of them. The last position of each airframe
   is kept and the segment to the current one is tested against every gate, so the
   crossing registers even when no sample landed inside. Only across a plausible gap. */
const TRANSIT_GAP_MAX = 15 * 60 * 1000;
const TRACK_KEEP = 30 * 60 * 1000;
/* Instantaneous heading says what the airframe is doing this second, not where it is
   going — a tanker in a racetrack reads E on one sample and W on the next. Direction is
   net longitude across the track window instead; below the threshold the crossing is
   provisional and gets corrected in place once the displacement is decisive. */
const DIR_MIN_DLON = 0.15;
const DIR_WINDOW = 20 * 60 * 1000;

// same list the page walks, minus the text proxy: from a server the mirrors answer directly
const MIL_SOURCES = [
  { name: 'adsb.fi', url: 'https://opendata.adsb.fi/api/v2/mil' },
  { name: 'adsb.lol', url: 'https://api.adsb.lol/v2/mil' },
  { name: 'airplanes.live', url: 'https://api.airplanes.live/v2/mil' }
];

/* Large airframes only — tankers, ISR and electronic warfare, transports. Keep in
   step with BIG_TYPES in news-events.js or the served history won't match the globe. */
const BIG_TYPES = /^(C135|K35R|R135|KC30|A332|A333|A339|A310|B703|E3TF|E3CF|E3|E6|E8|P8|P3|U2|RC12|E4|VC25|C17|C5M|C5|C30J|C130|C160|A400|C40|C32|C37|B752|B763|B77|IL76|IL78|AN12|AN24|AN26|AN30|AN72|AN124|A124|CL60|GLF|G5)/i;

/* ---- air bridge flow ------------------------------------------------------
   Two measurement gates. A crossing is registered the first time an airframe is
   seen inside a gate; it is not counted again until it has been out of that gate
   for VISIT_GAP, so a jet loitering across four samples is one crossing.

   Tankers are scarce and mission-critical, so their level accumulates (net
   eastbound, never reset). Cargo always comes home, so it is read as throughput:
   a 3-day rate against the 30-day baseline. Weights follow the same idea —
   a costly airframe is a stronger signal than a routine hauler. */
const GATES = [
  { id: 'central_europe', route: 'Middle East Northern route', leg: 'Central Europe',
    via: 'Ramstein · Balkans · E Med', bbox: [45.5, 14.0, 50.5, 23.0] },
  { id: 'mediterranean', route: 'Middle East Southern route', leg: 'Mediterranean',
    via: 'Lajes · Morón · Sigonella · Souda', bbox: [34.0, 2.0, 42.0, 20.0] }
];

/* VIP and command transports (VC25, C32, C37, GLF, CL60, G5) ride the same corridors
   without carrying capability or supply, so they are in neither metric. */
const CLASSES = [
  [/^(K35R|C135|KC30|K46|A332|A333|A339|A310|IL78)/i, 'tanker', 4],
  [/^(R135|E3TF|E3CF|E3|E6|E8|P8|P3|U2|RC12|E4)/i, 'isr', 3],
  [/^(C5M|C5|AN124|A124|IL76|B77|B763|B752)/i, 'cargo_hi', 2],
  [/^(C17|C30J|C130|C160|A400|AN12|AN24|AN26|AN30|AN72|C40)/i, 'cargo', 1]
];
const classOf = (t) => {
  for (const [re, cls, w] of CLASSES) if (re.test(t)) return { cls, w };
  return null;
};
const isCargo = (c) => c.cls === 'cargo' || c.cls === 'cargo_hi';
/* The C-5M is the only airframe worth calling out by type: a handful exist, they move
   outsized loads nothing else can, and a run of them eastbound is its own signal. */
const C5 = /^C5/i;
const c5Of = (list) => {
  const m = list.filter(c => C5.test(c.type || ''));
  const e = m.filter(c => c.dir === 'E').length;
  return { count: m.length, east: e, west: m.length - e };
};

let flow = { epoch: Date.now(), crossings: [], tankerEast: 0, tankerWest: 0 };
let flowInside = new Map();          // hex|gate -> last timestamp seen inside
const flowTracks = new Map();        // hex -> [{t, lat, lon}], memory only
const flowProv = new Map();          // hex|gate -> crossing awaiting a firm direction

function flowLoad() {
  try {
    const j = JSON.parse(fs.readFileSync(FLOW_FILE, 'utf8'));
    const cut = Date.now() - FLOW_KEEP;
    flow = {
      epoch: j.epoch || Date.now(),
      crossings: (j.crossings || []).filter(c => c && c.t > cut),
      tankerEast: j.tankerEast || 0,
      tankerWest: j.tankerWest || 0
    };
    for (const [k, v] of (j.inside || [])) flowInside.set(k, v);
    console.log('flow record loaded · ' + flow.crossings.length + ' crossings');
  } catch (e) { console.log('flow record: starting empty'); }
}

function flowSave() {
  try {
    fs.writeFileSync(FLOW_FILE, JSON.stringify({
      epoch: flow.epoch, crossings: flow.crossings,
      tankerEast: flow.tankerEast, tankerWest: flow.tankerWest,
      inside: [...flowInside]
    }));
  } catch (e) { console.log('flow write failed:', e.message); }
}

function gateFor(lat, lon) {
  for (const g of GATES) {
    const [s, w, n, e] = g.bbox;
    if (lat >= s && lat <= n && lon >= w && lon <= e) return g.id;
  }
  return null;
}

/* Liang-Barsky: does the segment touch the box at all, endpoints inside or not. */
function segHitsBox(lat0, lon0, lat1, lon1, box) {
  const [s, w, n, e] = box;
  const dx = lon1 - lon0, dy = lat1 - lat0;
  const p = [-dx, dx, -dy, dy];
  const q = [lon0 - w, e - lon0, lat0 - s, n - lat0];
  let t0 = 0, t1 = 1;
  for (let i = 0; i < 4; i++) {
    if (p[i] === 0) { if (q[i] < 0) return false; continue; }
    const r = q[i] / p[i];
    if (p[i] < 0) { if (r > t1) return false; if (r > t0) t0 = r; }
    else { if (r < t0) return false; if (r < t1) t1 = r; }
  }
  return true;
}
function gatesOnSegment(lat0, lon0, lat1, lon1) {
  const out = [];
  for (const g of GATES) if (segHitsBox(lat0, lon0, lat1, lon1, g.bbox)) out.push(g.id);
  return out;
}
function dirFrom(pts, trk) {
  if (pts && pts.length >= 2) {
    const last = pts[pts.length - 1];
    let first = pts[0];
    for (const p of pts) { if (last.t - p.t <= DIR_WINDOW) { first = p; break; } }
    const dlon = last.lon - first.lon;
    if (Math.abs(dlon) >= DIR_MIN_DLON) return { dir: dlon > 0 ? 'E' : 'W', firm: true };
  }
  return { dir: (typeof trk === 'number' && trk > 0 && trk < 180) ? 'E' : 'W', firm: false };
}
function tallyTanker(cls, dir, step) {
  if (cls !== 'tanker') return;
  if (dir === 'E') flow.tankerEast += step; else flow.tankerWest += step;
}

function flowIngest(air, t) {
  let added = 0;
  for (const a of air) {
    const spec = classOf(a.t || '');
    if (!spec) continue;
    if (!(typeof a.lat === 'number' && typeof a.lon === 'number')) continue;

    const pts = (flowTracks.get(a.hex) || []).filter(p => t - p.t <= TRACK_KEEP);
    const before = pts[pts.length - 1];
    pts.push({ t, lat: a.lat, lon: a.lon });
    flowTracks.set(a.hex, pts);

    const hit = new Set();
    const here = gateFor(a.lat, a.lon);
    if (here) hit.add(here);
    if (before && t - before.t <= TRANSIT_GAP_MAX) {
      for (const g of gatesOnSegment(before.lat, before.lon, a.lat, a.lon)) hit.add(g);
    }
    if (!hit.size) continue;

    const trk = typeof a.track === 'number' ? a.track
      : (typeof a.true_heading === 'number' ? a.true_heading : null);
    const d = dirFrom(pts, trk);

    for (const gate of hit) {
      const key = a.hex + '|' + gate;
      const last = flowInside.get(key);
      flowInside.set(key, t);
      if (last && t - last < VISIT_GAP) {
        const open = flowProv.get(key);
        if (open && d.firm) {
          if (open.dir !== d.dir) {
            tallyTanker(open.cls, open.dir, -1);
            open.dir = d.dir;
            tallyTanker(open.cls, open.dir, 1);
          }
          open.prov = false;
          flowProv.delete(key);
        }
        continue;
      }
      const c = {
        t, hex: a.hex, reg: (a.r || '').trim(), type: a.t || '',
        cls: spec.cls, w: spec.w, gate, dir: d.dir, prov: !d.firm
      };
      flow.crossings.push(c);
      added++;
      tallyTanker(spec.cls, d.dir, 1);
      if (d.firm) flowProv.delete(key); else flowProv.set(key, c);
    }
  }
  const cut = t - FLOW_KEEP;
  flow.crossings = flow.crossings.filter(c => c.t > cut);
  for (const [k, v] of flowInside) if (t - v > VISIT_GAP * 2) flowInside.delete(k);
  for (const [k, pts] of flowTracks) if (t - pts[pts.length - 1].t > TRACK_KEEP) flowTracks.delete(k);
  flowSave();
  return added;
}

const utcDayStart = (t) => Date.parse(new Date(t).toISOString().slice(0, 10) + 'T00:00:00Z');

/** Everything the panel draws. Windows are clipped to the age of the record, so a
    young record reports a short baseline rather than dividing by days it never saw. */
function flowSnapshot() {
  const now = Date.now();
  const ageDays = (now - flow.epoch) / DAY;
  const since = (d) => flow.crossings.filter(c => c.t > now - d * DAY);
  const win = (d) => Math.max(0.25, Math.min(d, ageDays));

  const recentDays = win(3), baseDays = win(30);
  const recentPerDay = since(3).filter(isCargo).length / recentDays;
  const basePerDay = since(30).filter(isCargo).length / baseDays;

  const t7 = since(7).filter(c => c.cls === 'tanker');
  const east7 = t7.filter(c => c.dir === 'E').length;

  const gatesFor = (d) => GATES.map(g => {
    const mine = since(d).filter(c => c.gate === g.id);
    const tk = mine.filter(c => c.cls === 'tanker');
    return {
      id: g.id, name: g.leg, route: g.route, leg: g.leg, via: g.via,
      tankerE: tk.filter(c => c.dir === 'E').length,
      tankerW: tk.filter(c => c.dir === 'W').length,
      cargo: mine.filter(isCargo).length,
      isr: mine.filter(c => c.cls === 'isr').length
    };
  });
  /* The panel switches window without refetching, so every window is served at once.
     `all` is the whole retained record, which FLOW_KEEP caps at 45 days. */
  const gatesByWindow = {
    all: gatesFor(FLOW_KEEP / DAY), '30': gatesFor(30), '7': gatesFor(7), '1': gatesFor(1)
  };
  const per = gatesByWindow['30'];

  /* Per-gate buckets behind each route's sparkline: 45 daily and 48 hourly, so the
     panel can slice any window it offers without another request. */
  const bucketSeries = (span, size, count) => {
    const out = {};
    for (const g of GATES) {
      const mine = flow.crossings.filter(c => c.gate === g.id && c.t > now - span);
      const arr = [];
      for (let i = count - 1; i >= 0; i--) {
        const end = now - i * size, start = end - size;
        const b = mine.filter(c => c.t > start && c.t <= end);
        arr.push({
          t: end,
          cargo: b.filter(isCargo).length,
          tanker: b.filter(c => c.cls === 'tanker').length
        });
      }
      out[g.id] = arr;
    }
    return out;
  };
  const gateDaily = bucketSeries(FLOW_KEEP, DAY, Math.round(FLOW_KEEP / DAY));
  const gateHourly = bucketSeries(48 * 3600 * 1000, 3600 * 1000, 48);

  /* The two headline figures on any window the panel offers. `all` is the whole
     retained record and keeps the running tanker counters, which are never reset;
     the cargo baseline stays 30 days whatever the recent window is, or the ratio
     would be measured against itself. */
  const headFor = (w) => {
    const span = w === 'all' ? null : Number(w);
    const list = span ? since(span) : flow.crossings;
    const tk = list.filter(c => c.cls === 'tanker');
    const e = tk.filter(c => c.dir === 'E').length, wv = tk.length - e;
    const days = span ? win(span) : Math.max(0.25, ageDays);
    const recent = list.filter(isCargo).length / days;
    return {
      east: span ? e : flow.tankerEast,
      west: span ? wv : flow.tankerWest,
      tankerNet: span ? e - wv : flow.tankerEast - flow.tankerWest,
      tankerCount: tk.length,
      cargoCount: list.filter(isCargo).length,
      c5: c5Of(list),
      recentPerDay: recent, basePerDay, baseDays,
      ratio: basePerDay > 0 ? recent / basePerDay : 0,
      days
    };
  };
  const headByWindow = { all: headFor('all'), '30': headFor('30'), '7': headFor('7'), '1': headFor('1') };

  const sampleDays = new Map();
  for (const p of milSamples) {
    const d = utcDay(p.t);
    sampleDays.set(d, (sampleDays.get(d) || 0) + 1);
  }
  const series = [];
  for (let i = 6; i >= 0; i--) {
    const start = utcDayStart(now - i * DAY), end = start + DAY;
    const mine = flow.crossings.filter(c => c.t >= start && c.t < end);
    const tk = mine.filter(c => c.cls === 'tanker');
    const day = new Date(start).toISOString().slice(0, 10);
    series.push({
      day, samples: sampleDays.get(day) || 0,
      tankerNet: tk.filter(c => c.dir === 'E').length - tk.filter(c => c.dir === 'W').length,
      cargo: mine.filter(isCargo).length,
      isr: mine.filter(c => c.cls === 'isr').length
    });
  }

  return {
    epoch: flow.epoch, now, ageDays, samples: milSamples.length,
    every: MIL_EVERY,
    tankerNet: flow.tankerEast - flow.tankerWest,
    audit: { east: flow.tankerEast, west: flow.tankerWest },
    flow7: { east: east7, west: t7.length - east7, net: east7 - (t7.length - east7) },
    cargo: { recentPerDay, basePerDay, ratio: basePerDay > 0 ? recentPerDay / basePerDay : 0, baseDays },
    gates: per, gatesByWindow, headByWindow, gateDaily, gateHourly, series, window: { gates: 30 }
  };
}

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
      const crossed = flowIngest(air, t);
      console.log('mil sample · ' + air.length + ' airborne · ' + MIL_SOURCES[i].name
        + (crossed ? ' · ' + crossed + ' gate crossings' : ''));
      return;
    } catch (e) {
      console.log('mil source ' + MIL_SOURCES[i].name + ' failed: ' + e.message);
    }
  }
  console.log('mil sample skipped — every source refused');
}

milLoad();
flowLoad();
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

/* ---- market quotes -------------------------------------------------------
   Yahoo's chart endpoint is keyless and covers indices, metals and oil, but it
   sends no CORS headers, so a browser cannot read it; from here it is a plain
   fetch. Change is against the previous session close the feed reports, not an
   open-to-now move. Nothing is invented: a symbol that fails is simply absent.
   Stooq was tried first and 404s to any datacentre address. */

const SYMS = {
  'S&P 500': '^GSPC', 'Nasdaq': '^IXIC', 'Gold': 'GC=F',
  'Silver': 'SI=F', 'Copper': 'HG=F', 'Oil (WTI)': 'CL=F',
  // Treasury yields quote as the yield itself; 2Y has no index, so the future stands in
  'US 3M': '^IRX', 'US 2Y': '2YY=F', 'US 5Y': '^FVX',
  'US 10Y': '^TNX', 'US 30Y': '^TYX', 'VIX': '^VIX', 'Crude vol': '^OVX'
};
/* 15s: one Yahoo batch per 15s serves every reader, whatever the visitor count —
   the cache and the CDN headers below collapse the crowd into that one request.
   The futures in this batch are ~10min delayed at source; the indices are not, and
   they are what a faster clock buys. */
const Q_TTL = 15 * 1000;
const UA = 'Mozilla/5.0 (compatible; atomic-news-relay/1.0)';
let qCache = { at: 0, data: null }, qInflight = null;

async function yahoo(sym) {
  const ctl = new AbortController();
  const bail = setTimeout(() => ctl.abort(), 12000);
  try {
    const url = 'https://query1.finance.yahoo.com/v8/finance/chart/'
      + encodeURIComponent(sym) + '?interval=1d&range=5d';
    const r = await fetch(url, { signal: ctl.signal, headers: { 'user-agent': UA, accept: 'application/json' } });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const j = await r.json();
    const meta = j && j.chart && j.chart.result && j.chart.result[0] && j.chart.result[0].meta;
    if (!meta) throw new Error('no meta');
    const px = Number(meta.regularMarketPrice);
    const prev = Number(meta.chartPreviousClose || meta.previousClose);
    if (!isFinite(px) || px <= 0) throw new Error('no price');
    return { px, ch: isFinite(prev) && prev ? ((px - prev) / prev) * 100 : 0 };
  } finally { clearTimeout(bail); }
}

async function quotes() {
  if (qCache.data && Date.now() - qCache.at < Q_TTL) return qCache.data;
  if (qInflight) return qInflight;
  qInflight = (async () => {
    const out = {};
    await Promise.all(Object.entries(SYMS).map(async ([name, sym]) => {
      try { out[name] = await yahoo(sym); }
      catch (e) { console.log('quote ' + name + ' failed: ' + e.message); }
    }));
    if (!Object.keys(out).length) throw new Error('every symbol failed');
    qCache = { at: Date.now(), data: out };
    return out;
  })().finally(() => { qInflight = null; });
  return qInflight;
}

/* Pass-through for Yahoo price history, cached so a panel left open does not
   hammer the endpoint. Symbols are limited to the ones the page charts. */
const SERIES_OK = new Set([...Object.values(SYMS), 'BTC-USD', 'ETH-USD']);
const INTERVALS = new Set(['1m', '5m', '15m', '30m', '60m', '1h', '1d', '1wk', '1mo', '3mo']);
const RANGES = new Set(['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'max']);
const sCache = new Map();   // key -> { at, body }

async function series(sym, interval, range) {
  if (!SERIES_OK.has(sym)) throw new Error('symbol not allowed');
  if (!INTERVALS.has(interval) || !RANGES.has(range)) throw new Error('bad frame');
  const key = sym + '|' + interval + '|' + range;
  const ttl = /m$/.test(interval) && interval !== '1mo' && interval !== '3mo' ? 60e3 : 10 * 60e3;
  const hit = sCache.get(key);
  if (hit && Date.now() - hit.at < ttl) return hit.body;
  const ctl = new AbortController();
  const bail = setTimeout(() => ctl.abort(), 15000);
  try {
    const url = 'https://query1.finance.yahoo.com/v8/finance/chart/'
      + encodeURIComponent(sym) + '?interval=' + interval + '&range=' + range;
    const r = await fetch(url, { signal: ctl.signal, headers: { 'user-agent': UA, accept: 'application/json' } });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const body = await r.text();
    sCache.set(key, { at: Date.now(), body });
    if (sCache.size > 120) sCache.delete(sCache.keys().next().value);
    return body;
  } finally { clearTimeout(bail); }
}

/* ---- Strait of Hormuz status ---------------------------------------------
   Nothing publishes the state of the strait as data, and no keyless AIS source
   covers the Gulf, so there is nothing to observe directly. It is inferred from
   reporting instead: GDELT is queried for Hormuz stories and each headline is
   read as leaning shut or reopened. Distinct DOMAINS are counted, never
   articles — one wire story syndicated forty times is one source. A flip needs
   a side to hold a dominant lean across two consecutive polls, so a single
   speculative headline cannot reopen the strait on the dashboard. Silence never
   moves the state: absence of reporting is not evidence of change.
   The three regexes below are duplicated in live-data.js for the browser's
   standby detector — change them together. */
const HORMUZ_FILE = process.env.HORMUZ_FILE || './hormuz-state.json';
const HZ_EVERY = 10 * 60 * 1000;
const HZ_WINDOW = '1d';           // GDELT timespan for each poll
const HZ_MIN_SOURCES = 3;         // distinct domains needed to constitute a lean
const HZ_RATIO = 2;               // and it must outweigh the other side this far
const HZ_CONFIRM = 2;             // consecutive polls before the state flips
const HZ_KEEP = 7 * DAY;
const HZ_PIN = ['open', 'closed', 'contested'].includes((process.env.HORMUZ_PIN || '').toLowerCase())
  ? process.env.HORMUZ_PIN.toLowerCase() : null;

const HZ_OPEN = /\b(re-?open(s|ed|ing)?|resum(e|es|ed|ing|ption)|lift(s|ed)?\s+(the\s+)?(blockade|closure|ban|restrictions)|back open|open(ed)?\s+(again|to shipping|to traffic)|first tanker since|traffic (returns|resumed|normal)|all[- ]clear|eas(ed|ing)\s+(of\s+)?restrictions|normalis(ed)?|normaliz(ed)?)\b/i;
const HZ_SHUT = /\b(clos(e|es|ed|ure|ing)|shut(s|ting|down)?|blockad(e|ed|ing)|block(s|ed|ing)|mined|mining|halt(s|ed|ing)|suspend(s|ed|ing)|seiz(e|es|ed|ure)|impassable|no transits|denied passage|barred)\b/i;
/* "Iran rules out reopening" reads as a reopening to a keyword test. A headline
   carrying both a reopen term and a negation is discarded rather than guessed. */
const HZ_NEG = /\b(not|no|never|won'?t|will not|refus\w+|reject\w+|rules? out|deny|denies|denied|doubt\w*|unlikely|fails? to|despite|without)\b/i;

let hz = { state: 'closed', since: null, prev: null, streak: 0, streakFor: null,
           last: null, flips: [], polls: [], error: null };

function hzLoad() {
  try {
    const j = JSON.parse(fs.readFileSync(HORMUZ_FILE, 'utf8'));
    if (j && j.state) hz = { ...hz, ...j };
  } catch (e) { /* first run */ }
}
function hzSave() {
  try { fs.writeFileSync(HORMUZ_FILE, JSON.stringify(hz)); }
  catch (e) { console.warn('hormuz save failed', e.message); }
}

async function hzFetch() {
  const q = '"strait of hormuz" sourcelang:eng';
  const url = 'https://api.gdeltproject.org/api/v2/doc/doc?query=' + encodeURIComponent(q)
    + '&format=json&mode=artlist&maxrecords=75&timespan=' + HZ_WINDOW + '&sort=datedesc';
  const ctl = new AbortController();
  const bail = setTimeout(() => ctl.abort(), 15000);
  try {
    const r = await fetch(url, { signal: ctl.signal, headers: { 'user-agent': UA } });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const j = await r.json();
    return Array.isArray(j.articles) ? j.articles : [];
  } finally { clearTimeout(bail); }
}

/** shut | open | null for one headline. Titles that say both are dropped. */
function hzRead(title) {
  const t = String(title || '');
  if (!/hormuz/i.test(t)) return null;
  const open = HZ_OPEN.test(t), shut = HZ_SHUT.test(t);
  if (open && (shut || HZ_NEG.test(t))) return null;
  if (open) return 'open';
  if (shut) return 'shut';
  return null;
}

function hzLean(o, s) {
  if (o >= HZ_MIN_SOURCES && o >= s * HZ_RATIO) return 'open';
  if (s >= HZ_MIN_SOURCES && s >= o * HZ_RATIO) return 'closed';
  if (o >= 2 && s >= 2) return 'contested';
  return null;
}

async function hzPoll(retry = 0) {
  let arts;
  try { arts = await hzFetch(); }
  catch (e) {
    // a failed poll is not a reading: leave the streak and the state untouched
    hz.error = e.message;
    hzSave();
    /* GDELT answers the first request from a cold datacentre IP with a 429 often
       enough that waiting out the full interval would leave the panel blank for
       ten minutes after every deploy. Two short retries, then wait for the tick. */
    if (retry < 2) setTimeout(() => hzPoll(retry + 1), 90000 * (retry + 1));
    return;
  }
  hz.error = null;
  const now = Date.now();
  const openD = new Set(), shutD = new Set(), cites = { open: [], shut: [] };
  for (const a of arts) {
    const side = hzRead(a.title);
    if (!side) continue;
    const dom = a.domain || '';
    (side === 'open' ? openD : shutD).add(dom);
    const bucket = cites[side];
    if (bucket.length < 3 && !bucket.some(c => c.domain === dom)) {
      bucket.push({ head: a.title, url: a.url, domain: dom, at: a.seendate || null });
    }
  }
  const lean = hzLean(openD.size, shutD.size);
  hz.last = { at: now, articles: arts.length, openSources: openD.size,
              shutSources: shutD.size, lean, cites };
  if (lean && lean !== hz.state) {
    hz.streak = hz.streakFor === lean ? hz.streak + 1 : 1;
    hz.streakFor = lean;
    if (hz.streak >= HZ_CONFIRM) {
      hz.flips.push({ at: now, from: hz.state, to: lean,
                      openSources: openD.size, shutSources: shutD.size });
      if (hz.flips.length > 20) hz.flips.shift();
      hz.prev = hz.state;
      hz.state = lean;
      hz.since = now;
      hz.streak = 0;
      hz.streakFor = null;
      console.log('hormuz -> ' + lean);
    }
  } else {
    hz.streak = 0;
    hz.streakFor = null;
  }
  hz.polls.push({ t: now, o: openD.size, s: shutD.size, lean });
  hz.polls = hz.polls.filter(p => now - p.t < HZ_KEEP);
  hzSave();
}

function hzSnapshot() {
  return {
    state: HZ_PIN || hz.state,
    pinned: !!HZ_PIN,
    since: hz.since,
    prev: hz.prev,
    updated: hz.last ? hz.last.at : null,
    lean: hz.last ? hz.last.lean : null,
    openSources: hz.last ? hz.last.openSources : 0,
    shutSources: hz.last ? hz.last.shutSources : 0,
    articles: hz.last ? hz.last.articles : 0,
    cites: hz.last ? hz.last.cites : { open: [], shut: [] },
    streak: hz.streak, streakFor: hz.streakFor, confirm: HZ_CONFIRM,
    minSources: HZ_MIN_SOURCES, window: HZ_WINDOW, every: HZ_EVERY,
    polls: hz.polls, flips: hz.flips.slice(-5),
    error: hz.error, src: 'relay', now: Date.now()
  };
}

hzLoad();
hzPoll();
setInterval(hzPoll, HZ_EVERY);

/* ---- export and restore ---------------------------------------------------
   Render's free tier has no persistent disk, so every deploy or recycle starts
   these files empty. `GET /export` dumps the raw record so a browser can keep a
   copy, and `POST /restore` merges one back in after a redeploy — the record
   survives on the client instead of on a disk we don't have.
   Restore is additive and idempotent: crossings are keyed hex|gate|t and mil
   samples t|src, so re-posting the same archive twice changes nothing. */
const RESTORE_TOKEN = process.env.RESTORE_TOKEN || '';
const EXPORT_MAX = Number(process.env.EXPORT_MAX) || 2000;

/* Every endpoint answers the same body to every caller, so the work is done once and
   handed to whatever cache sits in front. Each response carries an ETag (so a poller
   gets 304 and no body), Cache-Control with s-maxage (so a CDN serves the crowd from
   the edge and this box sees one request per TTL) and gzip. The gzipped body is kept
   against its ETag, so a thousand identical requests compress once. */
const gzCache = new Map();   // etag -> Buffer
function send(req, res, body, ttl, { type = 'application/json' } = {}) {
  const text = typeof body === 'string' ? body : JSON.stringify(body);
  const etag = 'W/"' + crypto.createHash('sha1').update(text).digest('base64').slice(0, 22) + '"';
  res.setHeader('Content-Type', type);
  res.setHeader('ETag', etag);
  res.setHeader('Vary', 'Accept-Encoding');
  res.setHeader('Access-Control-Expose-Headers', 'ETag');
  res.setHeader('Cache-Control', ttl > 0
    ? 'public, max-age=' + Math.round(ttl / 2) + ', s-maxage=' + ttl
      + ', stale-while-revalidate=' + ttl
    : 'no-store');
  if ((req.headers['if-none-match'] || '').split(/,\s*/).includes(etag)) {
    res.statusCode = 304;
    res.end();
    return;
  }
  const wantsGzip = /\bgzip\b/.test(req.headers['accept-encoding'] || '');
  if (wantsGzip && text.length > 1024) {
    let buf = gzCache.get(etag);
    if (!buf) {
      buf = zlib.gzipSync(Buffer.from(text), { level: 6 });
      if (gzCache.size > 64) gzCache.delete(gzCache.keys().next().value);
      gzCache.set(etag, buf);
    }
    res.setHeader('Content-Encoding', 'gzip');
    res.setHeader('Content-Length', buf.length);
    res.end(buf);
    return;
  }
  res.end(text);
}

function exportDoc(since = 0, limit = EXPORT_MAX) {
  const seen = {};
  for (const [d, s] of milSeen) seen[d] = [...s];
  const cut = Number(since) || 0;
  /* Bounded on purpose: a thousand tabs each asking for the whole tail every five
     minutes is the one thing on this box that costs real bandwidth. Each response
     carries at most `limit` of each stream, oldest first, with `more` and `nextSince`
     so the client walks forward across several pulls. */
  const allSamples = milSamples.filter(p => p.t > cut);
  const allCross = flow.crossings.filter(c => c.t > cut);
  const samples = allSamples.slice(0, limit);
  const crossings = allCross.slice(0, limit);
  const more = samples.length < allSamples.length || crossings.length < allCross.length;
  /* The two streams advance separately, so the resume mark is the lower of the two
     ends — the same rule the page uses when it asks. */
  const ends = [];
  if (samples.length) ends.push(samples[samples.length - 1].t);
  if (crossings.length) ends.push(crossings[crossings.length - 1].t);
  const nextSince = more && ends.length ? Math.min(...ends) - 1 : null;
  return {
    v: 1, at: Date.now(), since: cut, more, nextSince,
    /* The client keeps the record; `since` lets it ask only for what it has not
       merged yet, so the pull stays small enough to run every few minutes. */
    mil: { samples, seen },
    flow: { epoch: flow.epoch, crossings,
            oldest: flow.crossings.length ? flow.crossings[0].t : null,
            tankerEast: flow.tankerEast, tankerWest: flow.tankerWest },
    hormuz: { state: hz.state, since: hz.since, prev: hz.prev, flips: hz.flips, polls: hz.polls }
  };
}

function restoreDoc(doc) {
  if (!doc || doc.v !== 1) throw new Error('unrecognised archive');
  const now = Date.now();
  const report = { milAdded: 0, crossingsAdded: 0, epochMovedBack: false, hormuz: false };

  const up = doc.mil || {};
  if (Array.isArray(up.samples)) {
    const have = new Set(milSamples.map(p => p.t + '|' + (p.src || '')));
    for (const p of up.samples) {
      if (!p || typeof p.t !== 'number' || typeof p.n !== 'number' || !p.src) continue;
      if (now - p.t > MIL_KEEP) continue;
      const k = p.t + '|' + p.src;
      if (have.has(k)) continue;
      have.add(k);
      milSamples.push({ t: p.t, n: p.n, src: p.src });
      report.milAdded++;
    }
    milSamples.sort((a, b) => a.t - b.t);
  }
  if (up.seen && typeof up.seen === 'object') {
    for (const [d, list] of Object.entries(up.seen)) {
      if (!Array.isArray(list)) continue;
      const set = milSeen.get(d) || new Set();
      for (const h of list) set.add(h);
      milSeen.set(d, set);
    }
  }
  if (report.milAdded) milSave();

  const uf = doc.flow || {};
  if (Array.isArray(uf.crossings)) {
    /* The running tanker counters include crossings that have since aged out of the
       45-day window, so they cannot be recomputed from the list. The archive's
       counters are adopted wholesale and this instance's own crossings — the ones
       after the archive's last observation — are added on top. */
    const upTo = uf.crossings.reduce((m, c) => Math.max(m, c && c.t || 0), 0);
    const older = typeof uf.epoch === 'number' && uf.epoch < flow.epoch;
    const have = new Set(flow.crossings.map(c => c.hex + '|' + c.gate + '|' + c.t));
    const cut = now - FLOW_KEEP;
    for (const c of uf.crossings) {
      if (!c || typeof c.t !== 'number' || !c.hex || !c.gate) continue;
      if (c.t <= cut) continue;
      const k = c.hex + '|' + c.gate + '|' + c.t;
      if (have.has(k)) continue;
      have.add(k);
      flow.crossings.push(c);
      report.crossingsAdded++;
    }
    flow.crossings.sort((a, b) => a.t - b.t);
    if (older && report.crossingsAdded) {
      const mineAfter = flow.crossings.filter(c => c.t > upTo && c.cls === 'tanker');
      flow.tankerEast = (Number(uf.tankerEast) || 0) + mineAfter.filter(c => c.dir === 'E').length;
      flow.tankerWest = (Number(uf.tankerWest) || 0) + mineAfter.filter(c => c.dir === 'W').length;
      flow.epoch = uf.epoch;
      report.epochMovedBack = true;
    }
    if (report.crossingsAdded) flowSave();
  }

  /* The strait's state is only adopted if this instance has never read one — a
     stale archived state must not override a live poll. */
  const uh = doc.hormuz || {};
  if (uh.state && !hz.last) {
    hz.state = uh.state;
    hz.since = uh.since || null;
    hz.prev = uh.prev || null;
    if (Array.isArray(uh.flips)) hz.flips = uh.flips.slice(-20);
    if (Array.isArray(uh.polls)) hz.polls = uh.polls.filter(p => now - p.t < HZ_KEEP);
    hzSave();
    report.hormuz = true;
  }
  return report;
}

function readBody(req, cap = 24 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let n = 0;
    const parts = [];
    req.on('data', (d) => {
      n += d.length;
      if (n > cap) { reject(new Error('archive too large')); req.destroy(); return; }
      parts.push(d);
    });
    req.on('end', () => resolve(Buffer.concat(parts).toString('utf8')));
    req.on('error', reject);
  });
}

http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'content-type, if-none-match, x-restore-token');
  if (req.method === 'OPTIONS') { res.statusCode = 204; res.end(); return; }

  if (req.url.startsWith('/export')) {
    let since = 0, limit = EXPORT_MAX;
    try {
      const q = new URL(req.url, 'http://x').searchParams;
      since = Number(q.get('since')) || 0;
      limit = Math.max(100, Math.min(EXPORT_MAX, Number(q.get('limit')) || EXPORT_MAX));
    } catch (e) { /* full dump */ }
    send(req, res, exportDoc(since, limit), 60);
    return;
  }
  if (req.url.startsWith('/restore')) {
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Cache-Control', 'no-store');
    if (req.method !== 'POST') {
      res.statusCode = 405;
      res.end(JSON.stringify({ error: 'POST an archive' }));
      return;
    }
    if (!RESTORE_TOKEN) {
      res.statusCode = 503;
      res.end(JSON.stringify({ error: 'RESTORE_TOKEN is not set on the relay' }));
      return;
    }
    if ((req.headers['x-restore-token'] || '') !== RESTORE_TOKEN) {
      res.statusCode = 403;
      res.end(JSON.stringify({ error: 'bad token' }));
      return;
    }
    try {
      const report = restoreDoc(JSON.parse(await readBody(req)));
      console.log('restored', JSON.stringify(report));
      res.end(JSON.stringify({ ok: true, ...report }));
    } catch (e) {
      res.statusCode = 400;
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }
  if (req.url.startsWith('/hormuz')) {
    send(req, res, hzSnapshot(), 120);      // the detector polls every 10 min
    return;
  }
  if (req.url.startsWith('/series')) {
    try {
      const u = new URL(req.url, 'http://x');
      const interval = u.searchParams.get('interval') || '1d';
      const body = await series(u.searchParams.get('sym'), interval, u.searchParams.get('range') || '1y');
      const intraday = /m$/.test(interval) && interval !== '1mo' && interval !== '3mo';
      send(req, res, body, intraday ? 60 : 600);
    } catch (e) {
      res.statusCode = 502;
      send(req, res, { error: e.message }, 0);
    }
    return;
  }

  if (req.url.startsWith('/quotes')) {
    try {
      send(req, res, { at: qCache.at, quotes: await quotes() }, 15);
    } catch (e) {
      res.statusCode = 502;
      send(req, res, { error: e.message, quotes: {} }, 0);
    }
    return;
  }
  if (req.url.startsWith('/milhistory')) {
    send(req, res, {
      every: MIL_EVERY, now: Date.now(),
      days: milDays(), samples: milSamples
    }, 120);
    return;
  }
  if (req.url.startsWith('/flow')) {
    send(req, res, flowSnapshot(), 60);
    return;
  }
  const last = milSamples[milSamples.length - 1];
  send(req, res, 'air history up · ' + milSamples.length + ' samples'
    + ' · ' + flow.crossings.length + ' gate crossings'
    + ' · hormuz ' + (HZ_PIN || hz.state)
    + (last ? ' · last ' + last.n + ' airborne from ' + last.src : ''), 30, { type: 'text/plain' });
}).listen(PORT, () => console.log('listening on ' + PORT));
