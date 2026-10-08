'use strict';
/* Servidor de salas para el modo en línea de 8-Bits.
   Empareja a 2 jugadores con un código de 4 letras y reenvía sus mensajes.
   El servidor solo hace de relé: en Bloques y Aleteo cada móvil simula su partida; en el resto
   (juego remoto) el anfitrión simula y transmite, y por aquí pasan la señalización WebRTC y,
   si la conexión directa no es posible, las imágenes de respaldo. */

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8790;
const GAME_ID = /^[a-z]{3,16}$/;                 // id de juego válido (todos los de 2 jugadores)
const MAX_MSG = 128 * 1024;                       // bytes por mensaje (imágenes de respaldo)
const MAX_MSGS_PER_SEC = 150;                    // límite anti-abuso por conexión
const MAX_ROOMS = 5000;                          // tope de salas abiertas a la vez

const server = http.createServer((req, res) => {
  // Ruta de salud (/healthz) para Render y para "despertar" el servidor desde la app
  res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' });
  res.end('8-Bits online: ' + rooms.size + ' salas activas\n');
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
