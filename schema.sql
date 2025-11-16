-- Clean Transparency V2 Database Schema

-- Tabla de workflows Art17
CREATE TABLE IF NOT EXISTS art17_workflows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id VARCHAR(255) UNIQUE NOT NULL,
    proveedor_rut VARCHAR(20) NOT NULL,
    proveedor_nombre VARCHAR(255),
    monto_contrato DECIMAL(20, 2),
    objeto_contrato TEXT,
    status VARCHAR(50) DEFAULT 'pending',
    ingest_timestamp TIMESTAMPTZ NOT NULL,
    risk_check_timestamp TIMESTAMPTZ,
    compliance_check_timestamp TIMESTAMPTZ,
    final_timestamp TIMESTAMPTZ,
    riesgo VARCHAR(20),
    cumplimiento BOOLEAN,
    certificado_id VARCHAR(50) UNIQUE,
    hash_ingest VARCHAR(64),
    hash_riesgo VARCHAR(64),
    hash_compliance VARCHAR(64),
    hash_final VARCHAR(64),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hitl_decisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID REFERENCES art17_workflows(id) ON DELETE CASCADE,
    request_id VARCHAR(255) NOT NULL,
    decision VARCHAR(50) NOT NULL,
    reviewer VARCHAR(255) NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS digital_signatures (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID REFERENCES art17_workflows(id) ON DELETE CASCADE,
    certificado_id VARCHAR(50) NOT NULL,
    hash_original VARCHAR(64) NOT NULL,
    firma_base64 TEXT NOT NULL,
    certificate_subject VARCHAR(255),
    certificate_issuer VARCHAR(255),
    certificate_serial VARCHAR(100),
    signed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID REFERENCES art17_workflows(id) ON DELETE SET NULL,
    event_type VARCHAR(100) NOT NULL,
    event_data JSONB,
    actor VARCHAR(255),
    ip_address INET,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workflows_request_id ON art17_workflows(request_id);
CREATE INDEX IF NOT EXISTS idx_workflows_status ON art17_workflows(status);
CREATE INDEX IF NOT EXISTS idx_workflows_created_at ON art17_workflows(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_workflows_proveedor_rut ON art17_workflows(proveedor_rut);
CREATE INDEX IF NOT EXISTS idx_hitl_workflow_id ON hitl_decisions(workflow_id);
CREATE INDEX IF NOT EXISTS idx_hitl_created_at ON hitl_decisions(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_signatures_workflow_id ON digital_signatures(workflow_id);
CREATE INDEX IF NOT EXISTS idx_signatures_certificado ON digital_signatures(certificado_id);

CREATE INDEX IF NOT EXISTS idx_audit_workflow_id ON audit_log(workflow_id);
CREATE INDEX IF NOT EXISTS idx_signatures_certificado ON digital_signatures(certificado_id);

CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_event_type ON audit_log(event_type);
CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit_log(created_at DESC);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_art17_workflows_updated_at
    BEFORE UPDATE ON art17_workflows
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
-- Vista de resumen
CREATE OR REPLACE VIEW workflows_summary AS
SELECT 
    status,
    COUNT(*) as total,
    COUNT(CASE WHEN riesgo = 'BAJO' THEN 1 END) as riesgo_bajo,
    COUNT(CASE WHEN riesgo = 'MEDIO' THEN 1 END) as riesgo_medio,
    COUNT(CASE WHEN riesgo = 'ALTO' THEN 1 END) as riesgo_alto,
    AVG(EXTRACT(EPOCH FROM (final_timestamp - ingest_timestamp))) as avg_processing_time_seconds
FROM art17_workflows
WHERE final_timestamp IS NOT NULL
GROUP BY status;
