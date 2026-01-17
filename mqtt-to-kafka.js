import mqtt from "mqtt";
import { Kafka } from "kafkajs";

const MQTT_BROKER = "mqtt://localhost:1883";
const KAFKA_BROKERS = ["localhost:9092"];
const KAFKA_TOPIC = "sensor-data";
const NUM_SENSORS = 10;

let mqttClient = null;
let kafkaProducer = null;

// Initialize Kafka
const kafka = new Kafka({
  clientId: "mqtt-to-kafka-bridge",
  brokers: KAFKA_BROKERS,
  connectionTimeout: 10000,
  requestTimeout: 30000,
  retry: {
    initialRetryTime: 100,
    retries: 8,
    multiplier: 2,
    maxRetryTime: 30000
  }
});

// Connect to Kafka
async function connectKafka() {
  try {
    kafkaProducer = kafka.producer({
      allowAutoTopicCreation: true,
      transactionTimeout: 30000
    });

    await kafkaProducer.connect();
    console.log("✓ MQTT→Kafka Bridge: Connected to Kafka on", KAFKA_BROKERS);
  } catch (err) {
    console.log("✗ Kafka connection error:", err.message);
    setTimeout(connectKafka, 5000);
  }
}

// Connect to MQTT and subscribe to sensor topics
function connectMQTT() {
  try {
    mqttClient = mqtt.connect(MQTT_BROKER, {
      reconnectPeriod: 3000,
      connectTimeout: 10000
    });

    mqttClient.on("connect", () => {
      console.log("✓ MQTT→Kafka Bridge: Connected to MQTT on", MQTT_BROKER);
      
      // Subscribe to all sensor topics
      for (let i = 1; i <= NUM_SENSORS; i++) {
        const topic = `sensors/sensor${i}/data`;
        mqttClient.subscribe(topic, { qos: 1 }, (err) => {
          if (err) {
            console.log(`✗ Error subscribing to ${topic}:`, err.message);
          } else {
            console.log(`✓ Subscribed to topic: ${topic}`);
          }
        });
      }
    });

    mqttClient.on("message", async (topic, message) => {
      try {
        const data = JSON.parse(message.toString());
        
        // Send to Kafka
        if (kafkaProducer) {
          await kafkaProducer.send({
            topic: KAFKA_TOPIC,
            messages: [
              {
                key: data.sensor_id.toString(),
                value: JSON.stringify(data),
                headers: {
                  "sensor_id": data.sensor_id.toString(),
                  "timestamp": new Date().toISOString()
                }
              }
            ]
          });
        }
      } catch (err) {
        console.log("✗ Error processing message:", err.message);
      }
    });

    mqttClient.on("error", (err) => {
      console.log("⚠ MQTT error:", err.message);
    });

    mqttClient.on("reconnect", () => {
      console.log("⚠ MQTT: Attempting to reconnect...");
    });
  } catch (err) {
    console.log("✗ MQTT connection error:", err.message);
    setTimeout(connectMQTT, 5000);
  }
}

// Start the bridge
async function start() {
  console.log("\n🚀 Starting MQTT→Kafka Bridge");
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log(`MQTT Broker: ${MQTT_BROKER}`);
  console.log(`Kafka Brokers: ${KAFKA_BROKERS.join(", ")}`);
  console.log(`Kafka Topic: ${KAFKA_TOPIC}`);
  console.log(`Listening to: sensors/sensor[1-${NUM_SENSORS}]/data`);
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

  await connectKafka();
  connectMQTT();
}

// Graceful shutdown
process.on("SIGINT", async () => {
  console.log("\n✓ Shutting down MQTT→Kafka Bridge...");
  if (mqttClient) {
    mqttClient.end();
  }
  if (kafkaProducer) {
    await kafkaProducer.disconnect();
  }
  process.exit(0);
});

start();
