const http = require('http');
const express = require('express');
const cors = require('cors');
const { WebSocketServer, WebSocket } = require('ws');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;
const GOOGLE_MAPS_API_KEY = process.env.GOOGLE_MAPS_API_KEY || '';
const AUTO_MIGRATE = String(process.env.AUTO_MIGRATE || 'true') === 'true';

const pool = new Pool(
  DATABASE_URL
    ? {
        connectionString: DATABASE_URL,
      }
    : undefined,
);

const socketsByUser = new Map();
const socketsByRide = new Map();

function log(scope, message, meta = null) {
  const ts = new Date().toISOString();
  if (meta) {
    console.log(`[${ts}] [${scope}] ${message}`, meta);
    return;
  }
  console.log(`[${ts}] [${scope}] ${message}`);
}

const ensurePostgisSchema = `
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS rides (
  id UUID PRIMARY KEY,
  client_id TEXT NOT NULL,
  driver_id TEXT,
  status TEXT NOT NULL,
  vehicle_type TEXT NOT NULL,
  from_address TEXT NOT NULL,
  to_address TEXT NOT NULL,
  from_location GEOGRAPHY(POINT, 4326) NOT NULL,
  to_location GEOGRAPHY(POINT, 4326) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ride_location_updates (
  id BIGSERIAL PRIMARY KEY,
  ride_id UUID NOT NULL REFERENCES rides(id) ON DELETE CASCADE,
  driver_id TEXT NOT NULL,
  location GEOGRAPHY(POINT, 4326) NOT NULL,
  speed_mps DOUBLE PRECISION,
  heading DOUBLE PRECISION,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;

async function ensureSchema() {
  if (!AUTO_MIGRATE) return;
  const client = await pool.connect();
  try {
    await client.query(ensurePostgisSchema);
    log('db', 'PostGIS schema ready');
  } finally {
    client.release();
  }
}

function sendJson(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function broadcastRide(rideId, payload) {
  const subscribers = socketsByRide.get(rideId);
  if (!subscribers) return;
  for (const ws of subscribers) {
    sendJson(ws, payload);
  }
}

async function googlePlacesTextSearch(query) {
  const url = new URL('https://maps.googleapis.com/maps/api/place/textsearch/json');
  url.searchParams.set('query', query);
  url.searchParams.set('key', GOOGLE_MAPS_API_KEY);
  url.searchParams.set('region', 'tn');
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Google Places request failed with ${response.status}`);
  }
  const data = await response.json();
  return (data.results || []).slice(0, 7).map((item) => ({
    placeId: item.place_id,
    address: item.formatted_address,
    name: item.name,
    lat: item.geometry?.location?.lat,
    lng: item.geometry?.location?.lng,
  }));
}

async function googleReverseGeocode(lat, lng) {
  const url = new URL('https://maps.googleapis.com/maps/api/geocode/json');
  url.searchParams.set('latlng', `${lat},${lng}`);
  url.searchParams.set('key', GOOGLE_MAPS_API_KEY);
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Google Geocode request failed with ${response.status}`);
  }
  const data = await response.json();
  const top = (data.results || [])[0];
  return {
    address: top?.formatted_address || '',
  };
}

app.get('/health', (_, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

app.get('/api/maps/search', async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    log('maps', 'search request', { q });
    if (!q) {
      res.json({ results: [] });
      return;
    }
    if (!GOOGLE_MAPS_API_KEY) {
      res.status(500).json({
        error: 'GOOGLE_MAPS_API_KEY is missing on backend',
      });
      return;
    }
    const results = await googlePlacesTextSearch(q);
    log('maps', 'search results', { count: results.length });
    res.json({ results });
  } catch (error) {
    log('maps', 'search failed', { error: error.message });
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/maps/reverse', async (req, res) => {
  try {
    const lat = Number(req.query.lat);
    const lng = Number(req.query.lng);
    log('maps', 'reverse request', { lat, lng });
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      res.status(400).json({ error: 'lat/lng are required' });
      return;
    }
    if (!GOOGLE_MAPS_API_KEY) {
      res.status(500).json({
        error: 'GOOGLE_MAPS_API_KEY is missing on backend',
      });
      return;
    }
    const result = await googleReverseGeocode(lat, lng);
    log('maps', 'reverse resolved', { address: result.address });
    res.json(result);
  } catch (error) {
    log('maps', 'reverse failed', { error: error.message });
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/rides/request', async (req, res) => {
  try {
    const { id, clientId, from, to, vehicleType } = req.body || {};
    log('ride', 'request received', { id, clientId, vehicleType });
    if (!id || !clientId || !from || !to || !vehicleType) {
      res.status(400).json({ error: 'id, clientId, from, to, vehicleType are required' });
      return;
    }

    await pool.query(
      `
      INSERT INTO rides (
        id, client_id, status, vehicle_type, from_address, to_address, from_location, to_location
      )
      VALUES (
        $1, $2, 'requested', $3, $4, $5,
        ST_SetSRID(ST_MakePoint($6, $7), 4326)::geography,
        ST_SetSRID(ST_MakePoint($8, $9), 4326)::geography
      )
      `,
      [
        id,
        clientId,
        vehicleType,
        from.address,
        to.address,
        Number(from.lng),
        Number(from.lat),
        Number(to.lng),
        Number(to.lat),
      ],
    );

    const event = {
      type: 'ride_requested',
      ride: {
        id,
        clientId,
        vehicleType,
        from,
        to,
        status: 'requested',
      },
    };

    for (const [key, ws] of socketsByUser.entries()) {
      if (key.startsWith('driver:')) {
        sendJson(ws, event);
      }
    }
    const driversOnline = [...socketsByUser.keys()].filter((k) =>
      k.startsWith('driver:'),
    ).length;
    log('ride', 'request dispatched to drivers', { id, driversOnline });

    res.status(201).json({ rideId: id });
  } catch (error) {
    log('ride', 'request failed', { error: error.message });
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/rides/:rideId/accept', async (req, res) => {
  try {
    const { rideId } = req.params;
    const { driverId } = req.body || {};
    log('ride', 'accept request', { rideId, driverId });
    if (!driverId) {
      res.status(400).json({ error: 'driverId is required' });
      return;
    }

    const result = await pool.query(
      `
      UPDATE rides
      SET status = 'accepted', driver_id = $2, updated_at = NOW()
      WHERE id = $1
      RETURNING id, client_id, driver_id, status
      `,
      [rideId, driverId],
    );

    if (!result.rowCount) {
      res.status(404).json({ error: 'ride not found' });
      return;
    }

    const ride = result.rows[0];
    log('ride', 'accepted', { rideId, driverId, clientId: ride.client_id });
    broadcastRide(rideId, {
      type: 'ride_accepted',
      rideId,
      driverId,
      clientId: ride.client_id,
      status: ride.status,
    });

    const clientSocket = socketsByUser.get(`client:${ride.client_id}`);
    if (clientSocket) {
      sendJson(clientSocket, {
        type: 'ride_accepted',
        rideId,
        driverId,
        status: ride.status,
      });
    }

    res.json({ ok: true, rideId, driverId });
  } catch (error) {
    log('ride', 'accept failed', { rideId: req.params.rideId, error: error.message });
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/rides/:rideId/driver-location', async (req, res) => {
  try {
    const { rideId } = req.params;
    const { driverId, lat, lng, speedMps, heading } = req.body || {};
    if (!driverId || !Number.isFinite(lat) || !Number.isFinite(lng)) {
      res.status(400).json({ error: 'driverId, lat, lng are required' });
      return;
    }

    await pool.query(
      `
      INSERT INTO ride_location_updates (
        ride_id, driver_id, location, speed_mps, heading
      )
      VALUES (
        $1, $2, ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography, $5, $6
      )
      `,
      [rideId, driverId, Number(lng), Number(lat), speedMps ?? null, heading ?? null],
    );

    const payload = {
      type: 'driver_location_update',
      rideId,
      driverId,
      lat,
      lng,
      speedMps: speedMps ?? null,
      heading: heading ?? null,
      sentAt: new Date().toISOString(),
    };
    broadcastRide(rideId, payload);
    const rideOwner = await pool.query('SELECT client_id FROM rides WHERE id = $1 LIMIT 1', [rideId]);
    const clientId = rideOwner.rows[0]?.client_id;
    if (clientId) {
      const ws = socketsByUser.get(`client:${clientId}`);
      if (ws) sendJson(ws, payload);
    }

    log('ride', 'driver location update', {
      rideId,
      driverId,
      lat,
      lng,
      clientId: clientId || null,
    });

    res.json({ ok: true });
  } catch (error) {
    log('ride', 'driver location failed', { rideId: req.params.rideId, error: error.message });
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/rides/:rideId', async (req, res) => {
  try {
    const { rideId } = req.params;
    const rideResult = await pool.query(
      `
      SELECT
        id,
        client_id,
        driver_id,
        status,
        vehicle_type,
        from_address,
        to_address,
        ST_Y(from_location::geometry) AS from_lat,
        ST_X(from_location::geometry) AS from_lng,
        ST_Y(to_location::geometry) AS to_lat,
        ST_X(to_location::geometry) AS to_lng,
        created_at,
        updated_at
      FROM rides
      WHERE id = $1
      LIMIT 1
      `,
      [rideId],
    );
    if (!rideResult.rowCount) {
      res.status(404).json({ error: 'ride not found' });
      return;
    }
    res.json({ ride: rideResult.rows[0] });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const role = url.searchParams.get('role');
  const userId = url.searchParams.get('userId');
  const rideId = url.searchParams.get('rideId');

  if (role && userId) {
    socketsByUser.set(`${role}:${userId}`, ws);
  }
  if (rideId) {
    const subscribers = socketsByRide.get(rideId) || new Set();
    subscribers.add(ws);
    socketsByRide.set(rideId, subscribers);
  }

  sendJson(ws, { type: 'ws_connected', role, userId, rideId });
  log('ws', 'connected', {
    role,
    userId,
    rideId,
    users: socketsByUser.size,
  });

  ws.on('message', async (raw) => {
    try {
      const message = JSON.parse(raw.toString());
      log('ws', 'message', { role, userId, type: message.type, rideId: message.rideId || null });
      if (message.type === 'subscribe_ride' && message.rideId) {
        const subscribers = socketsByRide.get(message.rideId) || new Set();
        subscribers.add(ws);
        socketsByRide.set(message.rideId, subscribers);
        sendJson(ws, {
          type: 'subscribed',
          rideId: message.rideId,
        });
      }
      if (message.type === 'driver_location' && message.rideId) {
        const payload = {
          type: 'driver_location_update',
          rideId: message.rideId,
          driverId: message.driverId,
          lat: message.lat,
          lng: message.lng,
          heading: message.heading ?? null,
          speedMps: message.speedMps ?? null,
          sentAt: new Date().toISOString(),
        };
        broadcastRide(message.rideId, payload);
      }
    } catch (error) {
      log('ws', 'message parse failed', { error: error.message });
      sendJson(ws, { type: 'ws_error', error: error.message });
    }
  });

  ws.on('close', () => {
    if (role && userId) {
      socketsByUser.delete(`${role}:${userId}`);
    }
    for (const [id, subscribers] of socketsByRide.entries()) {
      subscribers.delete(ws);
      if (!subscribers.size) {
        socketsByRide.delete(id);
      }
    }
    log('ws', 'closed', {
      role,
      userId,
      users: socketsByUser.size,
    });
  });
});

ensureSchema()
  .then(() => {
    server.listen(PORT, () => {
      log('boot', 'backend started', {
        url: `http://localhost:${PORT}`,
        autoMigrate: AUTO_MIGRATE,
        hasDatabaseUrl: Boolean(DATABASE_URL),
        hasGoogleMapsKey: Boolean(GOOGLE_MAPS_API_KEY),
      });
    });
  })
  .catch((error) => {
    console.error('Failed to initialize backend:', error);
    process.exit(1);
  });
