CREATE EXTENSION IF NOT EXISTS hstore;

CREATE TABLE pipeline_process (
 key bigserial NOT NULL PRIMARY KEY,
 dataset_key uuid NOT NULL,
 attempt integer NOT NULL,
 dataset_title text,
 created timestamp with time zone NOT NULL DEFAULT now(),
 created_by text NOT NULL,
 UNIQUE(dataset_key, attempt)
);

CREATE TYPE pipeline_step_status AS ENUM ('SUBMITTED', 'RUNNING', 'FAILED', 'COMPLETED');

CREATE TYPE pipeline_step AS ENUM ('DWCA_TO_VERBATIM', 'XML_TO_VERBATIM', 'ABCD_TO_VERBATIM', 'VERBATIM_TO_INTERPRETED', 'INTERPRETED_TO_INDEX', 'HIVE_VIEW');

CREATE TABLE pipeline_step(
 key bigserial NOT NULL PRIMARY KEY,
 name pipeline_step NOT NULL,
 runner text,
 started timestamp with time zone,
 finished timestamp with time zone,
 state pipeline_step_status NOT NULL,
 message text,
 metrics hstore,
 rerun_reason text,
 created timestamp with time zone NOT NULL DEFAULT now(),
 created_by text NOT NULL,
 pipeline_process_key integer NOT NULL REFERENCES pipeline_process (key) ON DELETE CASCADE
);
