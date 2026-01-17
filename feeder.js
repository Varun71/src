import mqtt from "mqtt";
import { v4 as uuidv4 } from "uuid";

const client = mqtt.connect("mqtt://localhost:1883");

const SENSOR_COUNT = 10;
const sensors = Array.from({ length: SENSOR_COUNT }, (_, i) => `sensor_${i + 1}`);

client.on("connect", () => {
  console.log("✅ Sensor simulator connected to MQTT");

  setInterval(() => {
    sensors.forEach(sensor_id => {
      const payload = {
        msg_id: uuidv4(),
        sensor_id,
        temperature: +(20 + Math.random() * 10).toFixed(2),
        pressure: +(900 + Math.random() * 100).toFixed(2),
        humidity: +(30 + Math.random() * 50).toFixed(2),
        voltage: +(210 + Math.random() * 20).toFixed(2),
        current: +(5 + Math.random() * 5).toFixed(2),
        timestamp: new Date().toISOString()
      };

      client.publish("sensors/data", JSON.stringify(payload));
    });
  }, 1000);
});
