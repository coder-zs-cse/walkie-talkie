/**
 * TALKNET — WebRTC Signaling Server
 *
 * ═══════════════════════════════════════════════════════
 * SYSTEM DESIGN: SIGNALING SERVER ROLE
 * ═══════════════════════════════════════════════════════
 *
 * WebRTC peers cannot find each other directly — they need
 * a rendezvous point to exchange connection metadata:
 *   1. SDP Offers/Answers (codec negotiation, IP hints)
 *   2. ICE Candidates (actual network paths to reach each other)
 *
 * Once ICE negotiation completes, audio flows DIRECTLY
 * peer-to-peer. This server is no longer in the audio path.
 *
 * TOPOLOGY: Mesh (full-mesh)
 *   Every peer connects to every other peer directly.
 *   N peers = N*(N-1)/2 total connections.
 *   Best for small teams (< 15 people). Beyond that, use SFU.
 *
 * SELECTIVE BROADCAST:
 *   This server tracks PTT state and notifies the UI about
 *   who is transmitting to whom. The actual audio gating
 *   happens on the CLIENT via sender.track.enabled = true/false
 *   on each specific RTCPeerConnection independently.
 *
 * ═══════════════════════════════════════════════════════
 * MESSAGE PROTOCOL
 * ═══════════════════════════════════════════════════════
 *
 * CLIENT → SERVER:
 *   join            { username, roomId }
 *   offer           { to, sdp }              → relayed
 *   answer          { to, sdp }              → relayed
 *   ice-candidate   { to, candidate }        → relayed
 *   status-change   { status }               → broadcast
 *   ptt-start       { targets: [peerId...] } → broadcast
 *   ptt-stop        {}                       → broadcast
 *
 * SERVER → CLIENT:
 *   room-joined     { peerId, peers: [...] }
 *   peer-joined     { peerId, username }
 *   peer-left       { peerId }
 *   peer-status     { peerId, status }
 *   offer           { from, sdp }
 *   answer          { from, sdp }
 *   ice-candidate   { from, candidate }
 *   transmission-start { from, username, targets }
 *   transmission-end   { from, username }
 */

const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

// ─── HTTP Server (serves static files) ───────────────────────────────────────

const server = http.createServer((req, res) => {
  const safePath = req.url === '/' ? 'index.html' : req.url.replace(/^\//, '');
  const filePath = path.join(__dirname, 'public', safePath);

  // Prevent path traversal
  if (!filePath.startsWith(path.join(__dirname, 'public'))) {
    res.writeHead(403); res.end('Forbidden'); return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not Found'); return; }
    const contentTypes = {
      '.html': 'text/html; charset=utf-8',
      '.js':   'text/javascript',
      '.css':  'text/css',
      '.ico':  'image/x-icon',
    };
    const ext = path.extname(filePath);
    res.writeHead(200, { 'Content-Type': contentTypes[ext] || 'text/plain' });
    res.end(data);
  });
});

// ─── State ───────────────────────────────────────────────────────────────────

const peers = {};
// peers[peerId] = { ws, username, roomId, status: 'available'|'busy'|'dnd' }

const rooms = {};
// rooms[roomId] = Set<peerId>

// ─── Helpers ─────────────────────────────────────────────────────────────────

function uid() {
  return Math.random().toString(36).slice(2, 9).toUpperCase();
}

function send(peerId, msg) {
  const peer = peers[peerId];
  if (peer && peer.ws.readyState === WebSocket.OPEN) {
    try { peer.ws.send(JSON.stringify(msg)); } catch {}
  }
}

function broadcast(roomId, msg, skipId = null) {
  rooms[roomId]?.forEach(id => {
    if (id !== skipId) send(id, msg);
  });
}

function getRoomSnapshot(roomId, excludeId) {
  return [...(rooms[roomId] || [])]
    .filter(id => id !== excludeId)
    .map(id => ({
      id,
      username: peers[id].username,
      status: peers[id].status,
    }));
}

// ─── WebSocket Signaling ──────────────────────────────────────────────────────

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  const myId = uid();
  // Reserve slot; populated on 'join'
  peers[myId] = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      // ── Join a room ──────────────────────────────────────────────────────
      case 'join': {
        const { username, roomId } = msg;
        if (!username || !roomId) return;

        if (!rooms[roomId]) rooms[roomId] = new Set();

        peers[myId] = { ws, username, roomId, status: 'available' };

        // Tell newcomer about existing peers BEFORE adding them to room
        // so they don't get their own entry in the list
        send(myId, {
          type: 'room-joined',
          peerId: myId,
          peers: getRoomSnapshot(roomId, myId),
        });

        // Add to room THEN notify others
        rooms[roomId].add(myId);
        broadcast(roomId, { type: 'peer-joined', peerId: myId, username }, myId);

        console.log(`[JOIN]  ${username} (${myId}) → room:${roomId}  [${rooms[roomId].size} online]`);
        break;
      }

      // ── WebRTC Signaling (relay only — server never inspects SDP) ────────
      case 'offer':
        send(msg.to, { type: 'offer', from: myId, sdp: msg.sdp });
        break;

      case 'answer':
        send(msg.to, { type: 'answer', from: myId, sdp: msg.sdp });
        break;

      case 'ice-candidate':
        send(msg.to, { type: 'ice-candidate', from: myId, candidate: msg.candidate });
        break;

      // ── Availability status ──────────────────────────────────────────────
      case 'status-change': {
        const peer = peers[myId];
        if (!peer) break;
        const status = msg.status === 'dnd' ? 'dnd' : 'available';
        peer.status = status;
        broadcast(peer.roomId, { type: 'peer-status', peerId: myId, status });
        console.log(`[STATUS] ${peer.username} → ${status}`);
        break;
      }

      // ── Push-to-talk start ───────────────────────────────────────────────
      case 'ptt-start': {
        const peer = peers[myId];
        if (!peer || peer.status === 'dnd') break;

        peer.status = 'busy';

        // If caller specified targets, use those; otherwise: everyone available
        const requestedTargets = Array.isArray(msg.targets) ? msg.targets : [];
        const targets = requestedTargets.length > 0
          ? requestedTargets.filter(id => peers[id])   // validate they exist
          : [...rooms[peer.roomId]].filter(id => id !== myId && peers[id]?.status !== 'dnd');

        broadcast(peer.roomId, {
          type: 'transmission-start',
          from: myId,
          username: peer.username,
          targets,
        });

        console.log(`[PTT]   ${peer.username} → ${targets.length === 0 ? 'ALL' : targets.join(', ')}`);
        break;
      }

      // ── Push-to-talk stop ────────────────────────────────────────────────
      case 'ptt-stop': {
        const peer = peers[myId];
        if (!peer) break;
        peer.status = 'available';
        broadcast(peer.roomId, {
          type: 'transmission-end',
          from: myId,
          username: peer.username,
        });
        break;
      }
    }
  });

  ws.on('close', () => {
    const peer = peers[myId];
    if (peer) {
      console.log(`[LEAVE] ${peer.username} (${myId})`);
      rooms[peer.roomId]?.delete(myId);
      broadcast(peer.roomId, { type: 'peer-left', peerId: myId });
      if (rooms[peer.roomId]?.size === 0) delete rooms[peer.roomId];
    }
    delete peers[myId];
  });

  ws.on('error', () => { try { ws.terminate(); } catch {} });
});

// ─── Boot ─────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🎙️  TALKNET Signaling Server`);
  console.log(`    http://localhost:${PORT}\n`);
});
