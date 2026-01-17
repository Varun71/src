import mqtt from "mqtt";

const MQTT_BROKER = "mqtt://localhost:1883";
const BACKEND_API = "http://localhost:3001/api/sensor-data";
const NUM_SENSORS = 10;

let mqttClient = null;
let useMQTT = false;
let sensorData = [];

// Initialize sensor data
for (let i = 0; i < NUM_SENSORS; i++) {
  sensorData.push({
    sensor_id: i + 1,
    sensor_name: `Sensor_${i + 1}`,
    temperature: 20 + Math.random() * 15,
    pressure: 0.8 + Math.random() * 0.6,
    current: 1.5 + Math.random() * 2,
    voltage: 210 + Math.random() * 30,
    humidity: 30 + Math.random() * 40
  });
}

// Try to connect to MQTT broker
function connectMQTT() {
  try {
    mqttClient = mqtt.connect(MQTT_BROKER, {
      reconnectPeriod: 3000,
      connectTimeout: 10000
    });

    mqttClient.on("connect", () => {
      useMQTT = true;
      console.log("✓ Sensor Simulator: Connected to MQTT broker on", MQTT_BROKER);
    });

    mqttClient.on("error", (err) => {
      useMQTT = false;
      console.log("⚠ MQTT error:", err.message);
    });

    mqttClient.on("reconnect", () => {
      console.log("⚠ MQTT: Attempting to reconnect...");
    });
  } catch (err) {
    console.log("⚠ MQTT connection failed:", err.message);
  }
}

// Send data to backend API
async function sendToBackend(payload) {
  try {
    const response = await fetch(BACKEND_API, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    
    if (!response.ok) {
      console.log("⚠ Backend API error:", response.status);
    }
  } catch (err) {
    console.log("⚠ Backend API error:", err.message);
  }
}

// Generate and publish sensor data every 1 second
setInterval(() => {
  sensorData.forEach((sensor) => {
    // Add small random variation to simulate sensor changes
    sensor.temperature += (Math.random() - 0.5) * 0.5;
    sensor.pressure += (Math.random() - 0.5) * 0.02;
    sensor.current += (Math.random() - 0.5) * 0.1;
    sensor.voltage += (Math.random() - 0.5) * 2;
    sensor.humidity += (Math.random() - 0.5) * 1;

    // Keep values in reasonable ranges
    sensor.temperature = Math.max(10, Math.min(50, sensor.temperature));
    sensor.humidity = Math.max(0, Math.min(100, sensor.humidity));
    sensor.pressure = Math.max(0.8, Math.min(1.4, sensor.pressure));
    sensor.voltage = Math.max(200, Math.min(240, sensor.voltage));
    sensor.current = Math.max(0, Math.min(5, sensor.current));

    // Create data payload
    const payload = {
      sensor_id: sensor.sensor_id,
      sensor_name: sensor.sensor_name,
      temperature: parseFloat(sensor.temperature.toFixed(2)),
      pressure: parseFloat(sensor.pressure.toFixed(2)),
      current: parseFloat(sensor.current.toFixed(2)),
      voltage: parseFloat(sensor.voltage.toFixed(2)),
      humidity: parseFloat(sensor.humidity.toFixed(2)),
      time: new Date().toISOString(),
      data_source: useMQTT ? "mqtt" : "api"
    };

    // Publish to MQTT if connected
    if (useMQTT && mqttClient && mqttClient.connected) {
      const topic = `sensors/sensor${sensor.sensor_id}/data`;
      mqttClient.publish(topic, JSON.stringify(payload), { qos: 1 });
    }
    
    // Always send to backend API as well (for in-memory storage and real-time updates)
    sendToBackend(payload);

    console.log(`[Sensor ${sensor.sensor_id}] Temp: ${sensor.temperature.toFixed(2)}°C, Humidity: ${sensor.humidity.toFixed(2)}%`);
  });
}, 1000);

// Graceful shutdown
process.on("SIGINT", () => {
  console.log("\n✓ Shutting down Sensor Simulator...");
  if (mqttClient) {
    mqttClient.end();
  }
  process.exit(0);
});

console.log("\n🚀 Sensor Simulator Started");
console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━");
console.log("Generating data for 10 sensors");
console.log("Update rate: 1Hz (1 reading per second)");
console.log("Total: 10 readings per second");
console.log("Attempting MQTT connection...\n");

// Attempt MQTT connection
connectMQTT();
