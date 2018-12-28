package pgq

// DO NOT EDIT.  Use "go generate" to regenerate this file from create_table.sql.
//go:generate go run sql/main.go

const createTableSQL = "BEGIN;\nCREATE TABLE pgq_jobs (\n  id SERIAL PRIMARY KEY,\n  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),\n  queue_name TEXT NOT NULL,\n  data BYTEA NOT NULL,\n  run_after TIMESTAMP WITH TIME ZONE NOT NULL,\n  retry_waits TEXT[] NOT NULL,\n  ran_at TIMESTAMP WITH TIME ZONE,\n  error TEXT\n);\n\n-- Add an index for fast fetching of jobs by queue_name, sorted by run_after.  But only\n-- index jobs that haven't been done yet, in case the user is keeping the job history around.\nCREATE INDEX idx_pgq_jobs_fetch\n\tON pgq_jobs (queue_name, run_after)\n\tWHERE ran_at IS NULL;\nCOMMIT;\n"
