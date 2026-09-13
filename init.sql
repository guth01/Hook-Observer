CREATE TABLE IF NOT EXISTS endpoints (
    id VARCHAR(255) PRIMARY KEY,
    url VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY,
    endpoint_id VARCHAR(255) REFERENCES endpoints(id),
    headers JSONB,
    payload JSONB,
    status VARCHAR(50),
    received_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Insert a test endpoint so we can test right away
INSERT INTO endpoints (id, url) VALUES ('test-endpoint', 'https://httpbin.org/post') ON CONFLICT (id) DO NOTHING;
