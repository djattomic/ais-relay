/* AIS relay — holds the aisstream key server-side and republishes tanker
   positions as plain CORS-open JSON the dashboard can poll.

   aisstream refuses browser origins, so the page cannot talk to it directly.
   This process keeps one long-lived socket open, remembers the latest position
   per vessel, and serves them at GET /tankers.

   Run:  AIS_KEY=your-key node server.js
*/

import http from 'http';
import WebSocket from 'ws';

const KEY = process.env.AIS_KEY;
const PORT = process.env.PORT || 8080;
if (!KEY) { console.error('Set AIS_KEY'); process.exit(1); }

// The lanes the globe draws. Each box is [[lat,lon],[lat,lon]].
const BOXES = [
  [[10, 45], [32, 62]],     // Gulf, Hormuz, Gulf of Oman
  [[10, 32], [32, 44]],     // Red Sea, Suez, Bab el-Mandeb
  [[-8, 95], [10, 110]],    // Malacca, Singapore
  [[18, 105], [42, 132]],   // South China Sea to Japan
  [[30, -10], [46, 37]],    // Mediterranean, Adriatic, Black Sea
  [[48, -12], [62, 30]],    // North Sea, Baltic
  [[18, -98], [32, -60]],   // US Gulf, Caribbean
  [[-40, 10], [10, 25]]     // West Africa, Cape route
];

const SHIP_TYPE_TANKER = t => t >= 80 && t <= 89;
const STALE_MS = 40 * 60 * 1000;

const vessels = new Map();   // mmsi -> { lat, lon, cog, name, type, at }

function connect() {
  const ws = new WebSocket('wss://stream.aisstream.io/v0/stream');

  ws.on('open', () => {
    console.log('aisstream connected');
    ws.send(JSON.stringify({
      APIKey: KEY,
      BoundingBoxes: BOXES,
      FilterMessageTypes: ['PositionReport', 'ShipStaticData']
    }));
  });

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
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

  ws.on('close', () => { console.log('closed, retrying in 5s'); setTimeout(connect, 5000); });
  ws.on('error', (e) => { console.log('error', e.message); try { ws.close(); } catch (x) {} });
}
connect();

setInterval(() => {
  const cut = Date.now() - STALE_MS;
  for (const [k, v] of vessels) if ((v.at || 0) < cut) vessels.delete(k);
}, 60000);

http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
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
  res.end('AIS relay up · ' + vessels.size + ' vessels held');
}).listen(PORT, () => console.log('listening on ' + PORT));
