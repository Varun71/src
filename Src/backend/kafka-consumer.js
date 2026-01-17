import pkg from "pg";
import { Kafka } from "kafkajs";

const { Pool } = pkg;

const KAFKA_BROKERS = ["localhost:9092"];
const KAFKA_TOPIC = "sensor-data";
const KAFKA_GROUP = "timescaledb-consumer-group";
const BATCH_SIZE = 10;
const BATCH_TIMEOUT = 5000;

let pool = null;
let kafkaConsumer = null;
let isDBConnected = false;
let dataBuffer = [];
let batchTimer = null;

// Initialize PostgreSQL/TimescaleDB connection
async function initDB() {
  try {
    pool = new Pool({
      user: "varunsankar",
      host: "localhost",
      database: "sensors",
      port: 5432,
      connectionTimeoutMillis: 5000,
      idleTimeoutMillis: 30000,
      max: 10
    });

    pool.on("error", (err) => {
      console.log("⚠ Database connection error:", err.message);
      isDBConnected = false;
    });

    // Test connection
    const client = await pool.connect();
    console.log("✓ Kafka Consumer: Connected to TimescaleDB");
    client.release();
    isDBConnected = true;

    await setupDatabase();
  } catch (err) {
    console.log("✗ Database connection failed:", err.message);
    setTimeout(initDB, 5000);
  }
}

// Setup database schema
async function setupDatabase() {
  if (!isDBConnected || !pool) return;

  try {
    // Try to create TimescaleDB extension (may fail if not installed)
    try {
      await pool.query("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;");
      console.log("✓ TimescaleDB extension loaded");
    } catch (err) {
      console.log("⚠ TimescaleDB not available, using PostgreSQL table");
    }
    
    // Create sensor_readings table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS sensor_readings (
        id SERIAL PRIMARY KEY,
        time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        sensor_id INTEGER NOT NULL,
        sensor_name VARCHAR(50),
        temperature FLOAT8,
        pressure FLOAT8,
        current FLOAT8,
        voltage FLOAT8,
        humidity FLOAT8,
        data_source VARCHAR(20)
      );
    `);

    // Try to convert to hypertable (will fail silently if TimescaleDB not available)
    try {
      await pool.query(
        "SELECT create_hypertable('sensor_readings', 'time', if_not_exists => TRUE);"
      );
      console.log("✓ TimescaleDB Hypertable created");
    } catch (err) {
      if (!err.message.includes("already exists") && !err.message.includes("is not a valid TimescaleDB")) {
        console.log("⚠ Could not create hypertable (TimescaleDB not available)");
      } else {
        console.log("✓ Hypertable already exists");
      }
    }

    // Create indexes for performance
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_sensor_id_time 
      ON sensor_readings (sensor_id DESC, time DESC);
    `);

    console.log("✓ Database schema verified and ready");
  } catch (err) {
    console.log("✗ Schema setup error:", err.message);
  }
}

// Insert sensor data into TimescaleDB
async function insertSensorData(dataPoints) {
  if (!isDBConnected || !pool) {
    console.log("⚠ Database not connected, buffering data...");
    dataBuffer.push(...dataPoints);
    return;
  }

  try {
    // Build bulk insert query for multiple rows
    let query = `
      INSERT INTO sensor_readings (
        time, sensor_id, sensor_name, temperature, pressure, 
        current, voltage, humidity, data_source
      ) VALUES 
    `;

    const values = [];
    let paramIndex = 1;

    dataPoints.forEach((data, idx) => {
      const time = data.time || new Date().toISOString();
      if (idx > 0) query += ", ";
      query += `($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3}, `;
      query += `$${paramIndex + 4}, $${paramIndex + 5}, $${paramIndex + 6}, $${paramIndex + 7}, $${paramIndex + 8})`;

      values.push(
        time,
        data.sensor_id,
        data.sensor_name,
        data.temperature || 0,
        data.pressure || 0,
        data.current || 0,
        data.voltage || 0,
        data.humidity || 0,
        "kafka"
      );

      paramIndex += 9;
    });

    // Execute batch insert
    await pool.query(query, values);
    console.log(`✓ Inserted ${dataPoints.length} sensor readings into TimescaleDB`);
  } catch (err) {
    console.log("✗ Insert error:", err.message);
  }
}

// Flush buffered data in batches
async function flushBatch() {
  if (dataBuffer.length === 0) return;

  const batchData = dataBuffer.splice(0, BATCH_SIZE);
  await insertSensorData(batchData);

  if (dataBuffer.length > 0) {
    batchTimer = setTimeout(flushBatch, BATCH_TIMEOUT);
  }
}

// Process data and add to buffer
function processKafkaMessage(data) {
  try {
    dataBuffer.push(data);
    console.log(`[Kafka] Received sensor ${data.sensor_id} data (buffer: ${dataBuffer.length})`);

    // Flush if batch is full
    if (dataBuffer.length >= BATCH_SIZE) {
      if (batchTimer) clearTimeout(batchTimer);
      flushBatch();
    } else {
      // Schedule batch flush with timeout
      if (!batchTimer) {
        batchTimer = setTimeout(flushBatch, BATCH_TIMEOUT);
      }
    }
  } catch (err) {
    console.log("✗ Error processing Kafka message:", err.message);
  }
}

// Initialize Kafka consumer
async function initKafka() {
  try {
    const kafka = new Kafka({
      clientId: "timescaledb-consumer",
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

    kafkaConsumer = kafka.consumer({
      groupId: KAFKA_GROUP,
      sessionTimeout: 30000,
      heartbeatInterval: 3000
    });

    await kafkaConsumer.connect();
    console.log("✓ Kafka Consumer: Connected to Kafka on", KAFKA_BROKERS);

    // Subscribe to topic
    await kafkaConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
    console.log(`✓ Subscribed to Kafka topic: ${KAFKA_TOPIC}`);

    // Run consumer
    await kafkaConsumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const data = JSON.parse(message.value.toString());
          processKafkaMessage(data);
        } catch (err) {
          console.log("✗ Error parsing Kafka message:", err.message);
        }
      }
    });
  } catch (err) {
    console.log("✗ Kafka connection error:", err.message);
    setTimeout(initKafka, 5000);
  }
}

// Start the consumer service
async function start() {
  console.log("\n🚀 Starting Kafka → TimescaleDB Consumer");
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log(`Kafka Brokers: ${KAFKA_BROKERS.join(", ")}`);
  console.log(`Kafka Topic: ${KAFKA_TOPIC}`);
  console.log(`Consumer Group: ${KAFKA_GROUP}`);
  console.log(`Batch Size: ${BATCH_SIZE} rows`);
  console.log(`Batch Timeout: ${BATCH_TIMEOUT}ms`);
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

  // Initialize database first
  await initDB();
  
  // Wait a moment then initialize Kafka
  setTimeout(() => {
    initKafka();
  }, 2000);
}

// Graceful shutdown
process.on("SIGINT", async () => {
  console.log("\n✓ Shutting down Kafka Consumer...");
  
  // Flush remaining data
  if (dataBuffer.length > 0) {
    console.log(`Flushing ${dataBuffer.length} remaining records...`);
    await flushBatch();
  }

  if (batchTimer) {
    clearTimeout(batchTimer);
  }

  if (kafkaConsumer) {
    await kafkaConsumer.disconnect();
  }

  if (pool) {
    await pool.end();
  }

  process.exit(0);
});

// Start the consumer
start();
