-- CLEANTRANSPARENCY v2 Schema
CREATE TABLE IF NOT EXISTS requests (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(100) UNIQUE NOT NULL,
    proveedor_rut VARCHAR(20) NOT NULL,
    proveedor_nombre VARCHAR(255),
    monto_contrato DECIMAL(15, 2),
    objeto_contrato TEXT,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workflow_executions (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(100) NOT NULL,
    workflow_type VARCHAR(50) NOT NULL,
    hash_ingest VARCHAR(64) NOT NULL,
    riesgo VARCHAR(20),
    hash_riesgo VARCHAR(64),
    cumplimiento BOOLEAN,
    hash_compliance VARCHAR(64),
    hash_final VARCHAR(64) NOT NULL,
    timestamp_final TIMESTAMP NOT NULL,
    metadata JSONB,
    FOREIGN KEY (request_id) REFERENCES requests(request_id)
);

CREATE TABLE IF NOT EXISTS certificates (
    id SERIAL PRIMARY KEY,
    certificado_id VARCHAR(50) UNIQUE NOT NULL,
    request_id VARCHAR(100) NOT NULL,
    hash_final VARCHAR(64) NOT NULL,
    firma_digital TEXT,
    issued_at TIMESTAMP NOT NULL,
    FOREIGN KEY (request_id) REFERENCES requests(request_id)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(100),
    event_type VARCHAR(100) NOT NULL,
    event_data JSONB NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
