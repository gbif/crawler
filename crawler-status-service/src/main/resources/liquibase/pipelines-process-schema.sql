CREATE EXTENSION IF NOT EXISTS hstore;

CREATE TABLE pipelines_process (
 id serial NOT NULL UNIQUE,
 dataset_key uuid NOT NULL,
 attempt integer NOT NULL,
 dataset_title text,
 PRIMARY KEY(dataset_key, attempt)
);

CREATE TYPE pipelines_step_status AS ENUM ('RUNNING', 'FAILED', 'COMPLETED');

CREATE TABLE pipelines_step(
 key serial NOT NULL PRIMARY KEY,
 name text,
 runner text,
 started_date timestamp with time zone DEFAULT now(),
 finished_date timestamp with time zone DEFAULT now(),
 state pipelines_step_status,
 message text,
 metrics hstore,
 pipelines_process_id integer REFERENCES pipelines_process (id) ON DELETE CASCADE
);
