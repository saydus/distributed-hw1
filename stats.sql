-- postgresql table creation script for dht_stats table
-- this is used for statistics accumulation for data analysis at the end


CREATE TABLE dht_stats (
  id SERIAL PRIMARY KEY,
  message_type VARCHAR(255),
  entity_name VARCHAR(255),
  lookup VARCHAR(255),
  publishers_count INTEGER,
  subscribers_count INTEGER,
  latency DOUBLE PRECISION,
);


