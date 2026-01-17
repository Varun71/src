import express from "express";
import cors from "cors";
import http from "http";
import { Server } from "socket.io";

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

// In-memory sensor data storage
let sensorData = {};
const NUM_SENSORS = 10;
const HISTORY_LENGTH = 60; // Keep 60 seconds of data

// Initialize sensor data structure
function initializeSensorData() {
  for (let i = 1; i <= NUM_SENSORS; i++) {
    sensorData[i] = {
      history: [],
      latest: null
    };
  }
}

// Receive sensor data from simulator or Kafka consumer
app.post("/api/sensor-data", (req, res) => {
  try {
    const {
      sensor_id,
      sensor_name,
      temperature,
      pressure,
      current,
      voltage,
      humidity,
      time,
      data_source
    } = req.body;

    if (!sensor_id || !sensorData[sensor_id]) {
      return res.status(400).json({ error: "Invalid sensor_id" });
    }

    const reading = {
      time: time || new Date().toISOString(),
      sensor_id,
      sensor_name: sensor_name || `Sensor_${sensor_id}`,
      temperature,
      pressure,
      current,
      voltage,
      humidity,
      data_source: data_source || "api"
    };

    // Add to history
    sensorData[sensor_id].history.push(reading);

    // Keep only last HISTORY_LENGTH entries
    if (sensorData[sensor_id].history.length > HISTORY_LENGTH) {
      sensorData[sensor_id].history.shift();
    }

    // Update latest
    sensorData[sensor_id].latest = reading;

    res.json({ success: true, message: `Sensor ${sensor_id} data recorded` });
  } catch (err) {
    console.log("✗ Error receiving sensor data:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Get latest readings from all sensors
app.get("/latest", (req, res) => {
  try {
    const latestReadings = Object.keys(sensorData)
      .map(id => sensorData[id].latest)
      .filter(r => r !== null);

    res.json(latestReadings);
  } catch (err) {
    console.log("✗ Query error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Get history for a specific sensor
app.get("/history/:sensor_id/:minutes", (req, res) => {
  try {
    const sensorId = parseInt(req.params.sensor_id);
    const minutes = parseInt(req.params.minutes) || 1;

    if (!sensorData[sensorId]) {
      return res.status(404).json({ error: "Sensor not found" });
    }

    const cutoff = Date.now() - (minutes * 60 * 1000);
    const filtered = sensorData[sensorId].history.filter(d =>
      new Date(d.time).getTime() >= cutoff
    );

    res.json(filtered);
  } catch (err) {
    console.log("✗ Query error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Get analysis/aggregations for a sensor parameter
app.get("/analysis/:sensor_id/:param/:minutes", (req, res) => {
  try {
    const sensorId = parseInt(req.params.sensor_id);
    const param = req.params.param;
    const minutes = parseInt(req.params.minutes) || 1;

    const allowed = ["temperature", "pressure", "current", "voltage", "humidity"];
    if (!allowed.includes(param)) {
      return res.status(400).json({ error: "Invalid parameter" });
    }

    if (!sensorData[sensorId]) {
      return res.status(404).json({ error: "Sensor not found" });
    }

    const cutoff = Date.now() - (minutes * 60 * 1000);
    const filtered = sensorData[sensorId].history.filter(d =>
      new Date(d.time).getTime() >= cutoff
    );
    const values = filtered.map(d => d[param]);

    if (values.length === 0) {
      return res.json({
        mean: 0,
        max: 0,
        min: 0,
        stddev: 0,
        reading_count: 0
      });
    }

    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const max = Math.max(...values);
    const min = Math.min(...values);
    const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / values.length;
    const stddev = Math.sqrt(variance);

    res.json({ mean, max, min, stddev, reading_count: values.length });
  } catch (err) {
    console.log("✗ Query error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Get overview of all sensors
app.get("/overview/:minutes", (req, res) => {
  try {
    const minutes = parseInt(req.params.minutes) || 1;
    const cutoff = Date.now() - (minutes * 60 * 1000);

    const overview = Object.keys(sensorData).map(sensorId => {
      const filtered = sensorData[sensorId].history.filter(d =>
        new Date(d.time).getTime() >= cutoff
      );
      const temps = filtered.map(d => d.temperature);

      return {
        sensor_id: parseInt(sensorId),
        sensor_name: `Sensor_${sensorId}`,
        avg_temperature: temps.length > 0 ? temps.reduce((a, b) => a + b, 0) / temps.length : 0,
        max_temperature: temps.length > 0 ? Math.max(...temps) : 0,
        min_temperature: temps.length > 0 ? Math.min(...temps) : 0,
        avg_humidity: filtered.length > 0 ? filtered.reduce((a, d) => a + d.humidity, 0) / filtered.length : 0,
        reading_count: filtered.length,
        last_reading: filtered.length > 0 ? filtered[filtered.length - 1].time : null
      };
    });

    res.json(overview);
  } catch (err) {
    console.log("✗ Query error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Health check endpoint
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    sensors_online: NUM_SENSORS,
    storage: "in-memory",
    timestamp: new Date().toISOString()
  });
});

// Socket.io connection
io.on("connection", (socket) => {
  console.log("✓ Client connected:", socket.id);

  socket.on("disconnect", () => {
    console.log("✓ Client disconnected:", socket.id);
  });
});

// Broadcast latest sensor data every second
setInterval(() => {
  try {
    const latest = Object.keys(sensorData)
      .map(id => sensorData[id].latest)
      .filter(r => r !== null);

    if (latest.length > 0) {
      io.emit("sensor_update", latest);
    }
  } catch (err) {
    // Silently fail for broadcast
  }
}, 1000);

// Start server
async function start() {
  console.log("\n🚀 Starting Backend Server");
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log("Initializing in-memory storage...\n");

  initializeSensorData();

  server.listen(3001, () => {
    console.log("✓ Backend Server running on http://localhost:3001");
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━\n");
  });
}

// Graceful shutdown
process.on("SIGINT", () => {
  console.log("\n✓ Shutting down Backend Server...");
  process.exit(0);
});

start();

export default app;
