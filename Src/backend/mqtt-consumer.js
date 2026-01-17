import mqtt from "mqtt";
import pkg from "pg";

const { Pool } = pkg;
const MQTT_BROKER = "mqtt://localhost:1883";

let pool = null;
let mqttClient = null;
let isConnected = false;

// Initialize database connection
function initDB() {
  pool = new Pool({
    user: "postgres",
    host: "localhost",
    database: "sensors",
    password: "password",
    port: 5432
  });

  pool.on("connect", () => {
    console.log("✓ Connected to PostgreSQL/TimescaleDB");
    isConnected = true;
  });

  pool.on("error", (err) => {
    console.error("Database connection error:", err.message);
    isConnected = false;
  });
}

// Initialize MQTT connection
function initMQTT() {
  mqttClient = mqtt.connect(MQTT_BROKER, {
    reconnectPeriod: 1000,
    connectTimeout: 5000
  });

  mqttClient.on("connect", () => {
    console.log("✓ Connected to MQTT broker");
    // Subscribe to all sensor topics
    mqttClient.subscribe("sensors/+/data", (err) => {
      if (err) console.error("Subscribe error:", err);
      else console.log("✓ Subscribed to sensor topics");
    });
  });

  mqttClient.on("message", async (topic, message) => {
    try {
      const data = JSON.parse(message.toString());
      await insertSensorData(data);
    } catch (err) {
      console.error("Message processing error:", err.message);
    }
  });

  mqttClient.on("error", (err) => {
    console.error("MQTT error:", err.message);
  });

  mqttClient.on("disconnect", () => {
    console.log("Disconnected from MQTT");
  });
}

// Insert sensor data into TimescaleDB
async function insertSensorData(data) {
  if (!isConnected || !pool) return;

  try {
    const query = `
      INSERT INTO sensor_readings (
        time, sensor_id, sensor_name, temperature, pressure, 
        current, voltage, humidity, data_source
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9
      )
      ON CONFLICT (time, sensor_id) DO UPDATE SET
        temperature = $4,
        pressure = $5,
        current = $6,
        voltage = $7,
        humidity = $8;
    `;

    const time = data.time || new Date().toISOString();
    await pool.query(query, [
      time,
      data.sensor_id,
      data.sensor_name,
      data.temperature,
      data.pressure,
      data.current,
      data.voltage,
      data.humidity,
      "mqtt"
    ]);

    console.log(`[MQTT] Inserted sensor ${data.sensor_id} data`);
  } catch (err) {
    console.error("Insert error:", err.message);
  }
}

// Initialize connections
setTimeout(() => {
  initDB();
  setTimeout(() => {
    initMQTT();
  }, 2000);
}, 1000);

// Graceful shutdown
process.on("SIGINT", async () => {
  console.log("\nShutting down MQTT consumer...");
  if (mqttClient) {
    mqttClient.end();
  }
  if (pool) {
    await pool.end();
  }
  process.exit(0);
});

console.log("Starting MQTT Consumer Service...");
