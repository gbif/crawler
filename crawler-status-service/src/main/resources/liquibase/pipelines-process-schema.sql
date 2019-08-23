CREATE EXTENSION IF NOT EXISTS hstore;

CREATE TABLE pipelines_process (
 key serial NOT NULL PRIMARY KEY,
 dataset_key uuid NOT NULL,
 attempt integer NOT NULL,
 dataset_title text,
 UNIQUE(dataset_key, attempt)
);

CREATE TYPE pipelines_step_status AS ENUM ('RUNNING', 'FAILED', 'COMPLETED');

CREATE TABLE pipelines_step(
 key serial NOT NULL PRIMARY KEY,
 name text,
 runner text,
 started timestamp with time zone,
 finished timestamp with time zone,
 state pipelines_step_status,
 message text,
 metrics hstore,
 pipelines_process_key integer REFERENCES pipelines_process (key) ON DELETE CASCADE
);
