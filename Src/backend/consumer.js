import pkg from "pg";
import { Kafka } from "kafkajs";

const { Pool } = pkg;

const pool = new Pool({
  user: "postgres",
  host: "localhost",
  database: "sensors",
  password: "postgres",
  port: 5432
});

const kafka = new Kafka({
  clientId: "sensor-consumer",
  brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({ groupId: "sensor-group" });

async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic: "sensor-readings", fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());

      await pool.query(
        `
        INSERT INTO sensors
        (time, sensor_id, temperature, pressure, humidity, voltage, current, msg_id)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT DO NOTHING
        `,
        [
          data.timestamp,
          data.sensor_id,
          data.temperature,
          data.pressure,
          data.humidity,
          data.voltage,
          data.current,
          data.msg_id
        ]
      );
    }
  });
}

start();
