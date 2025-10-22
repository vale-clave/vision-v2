-- Extensión Timescale
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Tabla de eventos de zona
CREATE TABLE IF NOT EXISTS zone_events (
    id BIGSERIAL,
    tenant_id INT DEFAULT 1,
    camera_id INT,
    zone_id   INT,
    track_id  INT,
    event     TEXT,
    ts        TIMESTAMPTZ NOT NULL,
    dwell_seconds FLOAT,
    PRIMARY KEY (id, ts)
);

-- Convertir a hypertable particionada por hora
SELECT create_hypertable('zone_events', 'ts', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 hour');

-- Continuous aggregate para dwell_stats_minute
CREATE MATERIALIZED VIEW IF NOT EXISTS dwell_stats_minute
WITH (timescaledb.continuous) AS
SELECT
    tenant_id,
    camera_id,
    zone_id,
    time_bucket('1 minute', ts) AS bucket,
    avg(EXTRACT(EPOCH FROM (ts)) ) AS avg_ts
FROM zone_events
GROUP BY tenant_id, camera_id, zone_id, bucket;

-- Tabla de cámaras
CREATE TABLE IF NOT EXISTS cameras (
    id INT PRIMARY KEY,
    tenant_id INT DEFAULT 1,
    name TEXT,
    location TEXT,
    rtsp_url TEXT,
    fps INT DEFAULT 30,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Tabla de zonas (polígono en JSON)
CREATE TABLE IF NOT EXISTS zones (
    id INT PRIMARY KEY,
    tenant_id INT DEFAULT 1,
    camera_id INT REFERENCES cameras(id),
    name TEXT,
    metrics TEXT[], -- Cambiado de 'type' a 'metrics' para soportar múltiples
    polygon JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Tabla de umbrales/alertas por zona
CREATE TABLE IF NOT EXISTS zone_thresholds (
    zone_id INT REFERENCES zones(id) ON DELETE CASCADE,
    metric TEXT,                     -- occupancy|dwell
    threshold FLOAT,
    level TEXT DEFAULT 'warning',    -- warning|critical
    PRIMARY KEY (zone_id, metric)
);
