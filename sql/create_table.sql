BEGIN;
CREATE TABLE pgq_jobs (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  queue_name TEXT NOT NULL,
  data BYTEA NOT NULL,
  run_after TIMESTAMP WITH TIME ZONE NOT NULL,
  retry_waits TEXT[] NOT NULL,
  ran_at TIMESTAMP WITH TIME ZONE,
  error TEXT
);

-- Add an index for fast fetching of jobs by queue_name, sorted by run_after.  But only
-- index jobs that haven't been done yet, in case the user is keeping the job history around.
CREATE INDEX idx_pgq_jobs_fetch
	ON pgq_jobs (queue_name, run_after)
	WHERE ran_at IS NULL;
COMMIT;
