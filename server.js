'use strict';
/* Servidor de salas para el modo en línea de 8-Bits.
   Empareja a 2 jugadores con un código de 4 letras y reenvía sus mensajes.
   El servidor solo hace de relé: en Bloques y Aleteo cada móvil simula su partida; en el resto
   (juego remoto) el anfitrión simula y transmite, y por aquí pasan la señalización WebRTC y,
   si la conexión directa no es posible, las imágenes de respaldo. */

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

/* =========================================================
   VALIDACIÓN DE COMPRAS (POST /validate)
   La app (cordova-plugin-purchase) envía cada compra aprobada; aquí se comprueba con la API de Google Play
   (purchases.products.get) que el token es real, está pagado y, si son monedas, que no se usó ya.
   Variables de entorno en Render:
     GOOGLE_SERVICE_ACCOUNT  → el JSON completo de la cuenta de servicio con acceso a Play Console
     PACKAGE_NAME            → com.gem.ochobits (valor por defecto)
   Sin GOOGLE_SERVICE_ACCOUNT la compra se acepta con un aviso (modo de configuración), para no bloquear
   compras mientras se termina de configurar. Las compras de iPhone aún se aceptan sin comprobar.
   ========================================================= */
const PACKAGE_NAME = process.env.PACKAGE_NAME || 'com.gem.ochobits';
const ERR_VERIFICATION = 6777017, ERR_COMMUNICATION = 6777012;
let SERVICE = null;
try { if (process.env.GOOGLE_SERVICE_ACCOUNT) SERVICE = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT); }
catch (e) { console.error('GOOGLE_SERVICE_ACCOUNT no es un JSON válido'); }
console.log(SERVICE ? 'Validación de compras de Google Play: ACTIVA' : 'Validación de compras: SIN CONFIGURAR (se aceptan con aviso)');

const b64url = (b) => Buffer.from(b).toString('base64').replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');
let gToken = null, gTokenExp = 0;
// token de acceso OAuth2 de la cuenta de servicio (JWT firmado con su clave privada), con caché de ~55 min
async function googleToken() {
  if (gToken && Date.now() < gTokenExp) return gToken;
  const now = Math.floor(Date.now() / 1000);
  const head = b64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const claim = b64url(JSON.stringify({ iss: SERVICE.client_email, scope: 'https://www.googleapis.com/auth/androidpublisher', aud: 'https://oauth2.googleapis.com/token', iat: now, exp: now + 3600 }));
  const sig = b64url(crypto.createSign('RSA-SHA256').update(head + '.' + claim).sign(SERVICE.private_key));
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: 'grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=' + head + '.' + claim + '.' + sig,
  });
  if (!res.ok) throw Object.assign(new Error('OAuth ' + res.status), { comm: true });
  const j = await res.json();
  gToken = j.access_token; gTokenExp = Date.now() + (j.expires_in - 300) * 1000;
  return gToken;
}
const ok = (id, transaction, extra) => ({ ok: true, data: Object.assign({ id, latest_receipt: true, transaction, collection: [{ id, purchaseDate: transaction.purchaseTimeMillis ? Number(transaction.purchaseTimeMillis) : undefined, isExpired: false }], date: new Date().toISOString() }, extra || {}) });
const fail = (code, message, status) => ({ ok: false, code, message, status: status || 200 });

async function validate(body) {
  const tr = body && body.transaction;
  if (!tr || typeof body.id !== 'string') return fail(ERR_VERIFICATION, 'Petición incompleta');
  if (tr.type === 'android-playstore') {
    if (typeof tr.purchaseToken !== 'string' || tr.purchaseToken.length > 4096) return fail(ERR_VERIFICATION, 'Falta el token de compra');
    if (!SERVICE) return ok(body.id, { type: 'android-playstore' }, { warning: 'sin verificar: falta GOOGLE_SERVICE_ACCOUNT' });
    let res;
    try {
      const url = 'https://androidpublisher.googleapis.com/androidpublisher/v3/applications/' + encodeURIComponent(PACKAGE_NAME) +
        '/purchases/products/' + encodeURIComponent(body.id) + '/tokens/' + encodeURIComponent(tr.purchaseToken);
      res = await fetch(url, { headers: { Authorization: 'Bearer ' + (await googleToken()) } });
    } catch (e) { return fail(ERR_COMMUNICATION, 'No se pudo consultar a Google', 503); }
    if (res.status === 400 || res.status === 404 || res.status === 410) return fail(ERR_VERIFICATION, 'Google no reconoce esta compra');
    if (!res.ok) return fail(ERR_COMMUNICATION, 'Google respondió ' + res.status, 503);
    const p = await res.json();
    // purchaseState: 0 pagada, 1 cancelada, 2 pendiente · consumptionState: 1 = monedas ya entregadas
    if (p.purchaseState !== 0) return fail(ERR_VERIFICATION, p.purchaseState === 2 ? 'Pago pendiente' : 'Compra cancelada');
    if (body.type === 'consumable' && p.consumptionState === 1) return fail(ERR_VERIFICATION, 'Esta compra ya se usó');
    return ok(body.id, Object.assign({ type: 'android-playstore' }, p));
  }
  if (tr.type === 'ios-appstore' || tr.type === 'apple-sk2') return ok(body.id, { type: tr.type }, { warning: 'sin verificar: validación de Apple pendiente' });
  return fail(ERR_VERIFICATION, 'Tienda no admitida');
}
// límite por IP para /validate (30 por minuto)
const hits = new Map();
setInterval(() => hits.clear(), 60000);

const PORT = process.env.PORT || 8790;
const GAME_ID = /^[a-z]{3,16}$/;                 // id de juego válido (todos los de 2 jugadores)
const MAX_MSG = 128 * 1024;                       // bytes por mensaje (imágenes de respaldo)
const MAX_MSGS_PER_SEC = 150;                    // límite anti-abuso por conexión
const MAX_ROOMS = 5000;                          // tope de salas abiertas a la vez

const CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type', 'Cache-Control': 'no-store' };
const server = http.createServer((req, res) => {
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); return res.end(); }
  if (req.method === 'POST' && req.url.split('?')[0] === '/validate') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    const n = (hits.get(ip) || 0) + 1; hits.set(ip, n);
    const reply = (obj) => { res.writeHead(obj.ok ? 200 : (obj.status || 200), Object.assign({ 'Content-Type': 'application/json' }, CORS)); res.end(JSON.stringify(obj)); };
    if (n > 30) return reply(fail(ERR_COMMUNICATION, 'Demasiadas peticiones', 429));
    let raw = '';
    req.on('data', (c) => { raw += c; if (raw.length > 64 * 1024) req.destroy(); });
    req.on('end', async () => {
      let body;
      try { body = JSON.parse(raw); } catch (e) { return reply(fail(ERR_VERIFICATION, 'JSON no válido', 400)); }
      try {
        const r = await validate(body);
        console.log('validate', body.id, r.ok ? 'OK' + (r.data.warning ? ' (' + r.data.warning + ')' : '') : 'RECHAZADA: ' + r.message);
        reply(r);
      } catch (e) { console.error('validate', e.message); reply(fail(ERR_COMMUNICATION, 'Error del servidor', 500)); }
    });
    return;
  }
  // Ruta de salud (/healthz) para Render y para "despertar" el servidor desde la app
  res.writeHead(200, Object.assign({ 'Content-Type': 'text/plain' }, CORS));
  res.end('8-Bits online: ' + rooms.size + ' salas activas · compras: ' + (SERVICE ? 'verificadas' : 'sin configurar') + '\n');
});
const wss = new WebSocketServer({ server, maxPayload: MAX_MSG });
const rooms = new Map(); // code -> { game, players: [ws], rematch: Set }

const send = (ws, m) => { if (ws.readyState === 1) ws.send(JSON.stringify(m)); };

function newCode() {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
  let c;
  do { c = Array.from({ length: 4 }, () => A[Math.floor(Math.random() * A.length)]).join(''); } while (rooms.has(c));
  return c;
}

function start(room) {
  const seed = Math.floor(Math.random() * 2 ** 31);
  room.rematch.clear();
  room.players.forEach((p, i) => send(p, { t: 'start', seed, role: i === 0 ? 'host' : 'guest', game: room.game }));
}

function leave(ws) {
  const room = rooms.get(ws.room);
  ws.room = null;
  if (!room) return;
  room.players = room.players.filter((p) => p !== ws);
  room.rematch.delete(ws);
  room.players.forEach((p) => send(p, { t: 'left' }));
  if (!room.players.length) rooms.forEach((r, c) => { if (r === room) rooms.delete(c); });
}

wss.on('connection', (ws) => {
  ws.room = null;
  ws.isAlive = true;
  ws.budget = MAX_MSGS_PER_SEC;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    if (--ws.budget < 0) return;
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'create') {
      if (typeof m.game !== 'string' || !GAME_ID.test(m.game)) return send(ws, { t: 'error', msg: 'Ese juego no tiene modo en línea.' });
      if (rooms.size >= MAX_ROOMS) return send(ws, { t: 'error', msg: 'El servidor está lleno. Inténtalo en un momento.' });
      leave(ws);
      const code = newCode();
      rooms.set(code, { game: m.game, players: [ws], rematch: new Set() });
      ws.room = code;
      send(ws, { t: 'created', code });
    } else if (m.t === 'join') {
      const code = String(m.code || '').toUpperCase().slice(0, 4);
      const room = rooms.get(code);
      if (!room) return send(ws, { t: 'error', msg: 'No existe ninguna sala con ese código.' });
      if (room.players.length >= 2) return send(ws, { t: 'error', msg: 'La sala ya está llena.' });
      if (room.game !== m.game) return send(ws, { t: 'error', msg: 'Esa sala es de otro juego.' });
      leave(ws);
      room.players.push(ws);
      ws.room = code;
      start(room);
    } else if (m.t === 'relay') {
      const room = rooms.get(ws.room);
      if (room) room.players.forEach((p) => { if (p !== ws) send(p, { t: 'peer', d: m.d }); });
    } else if (m.t === 'rematch') {
      const room = rooms.get(ws.room);
      if (!room) return;
      room.rematch.add(ws);
      if (room.players.length === 2 && room.rematch.size === 2) start(room);
      else room.players.forEach((p) => { if (p !== ws) send(p, { t: 'rematch' }); });
    } else if (m.t === 'leave') {
      leave(ws);
    }
  });

  ws.on('close', () => leave(ws));
});

// Latido: cierra conexiones muertas y recarga el límite de mensajes
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 15000);
setInterval(() => { wss.clients.forEach((ws) => { ws.budget = MAX_MSGS_PER_SEC; }); }, 1000);

server.listen(PORT, () => console.log('8-Bits online escuchando en el puerto ' + PORT));
