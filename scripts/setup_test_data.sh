#!/bin/bash
# Run after docker-compose up -d
docker exec -i postgres psql -U admin -d orion <<EOF
CREATE TABLE IF NOT EXISTS features (
    id SERIAL PRIMARY KEY,
    temperature FLOAT,
    vibration FLOAT,
    rpm INT,
    is_anomaly INT
);
INSERT INTO features (temperature, vibration, rpm, is_anomaly) VALUES
(85.0, 1.2, 2500, 0),
(110.0, 4.5, 5800, 1),
(75.0, 0.8, 1800, 0);
EOF
echo "Test data inserted."
