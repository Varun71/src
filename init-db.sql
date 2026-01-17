-- Create extension for TimescaleDB (if not already created)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create hypertable for sensor data
CREATE TABLE IF NOT EXISTS sensor_readings (
  time TIMESTAMPTZ NOT NULL,
  sensor_id INTEGER NOT NULL,
  sensor_name VARCHAR(50),
  temperature FLOAT8,
  pressure FLOAT8,
  current FLOAT8,
  voltage FLOAT8,
  humidity FLOAT8,
  data_source VARCHAR(20) -- 'kafka' or 'mqtt'
);

-- Convert to hypertable (if not already)
SELECT create_hypertable('sensor_readings', 'time', if_not_exists => TRUE);

-- Create continuous aggregate for 1-minute downsampling
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_1min AS
SELECT
  time_bucket('1 minute', time) AS minute,
  sensor_id,
  sensor_name,
  AVG(temperature) AS avg_temperature,
  MAX(temperature) AS max_temperature,
  MIN(temperature) AS min_temperature,
  AVG(pressure) AS avg_pressure,
  AVG(current) AS avg_current,
  AVG(voltage) AS avg_voltage,
  AVG(humidity) AS avg_humidity,
  COUNT(*) AS reading_count
FROM sensor_readings
GROUP BY minute, sensor_id, sensor_name
ORDER BY minute DESC;

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_sensor_readings_sensor_id ON sensor_readings (sensor_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_time ON sensor_readings (time DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_source ON sensor_readings (data_source, time DESC);

-- Create retention policy (keep data for 30 days)
SELECT add_retention_policy('sensor_readings', INTERVAL '30 days', if_not_exists => TRUE);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
