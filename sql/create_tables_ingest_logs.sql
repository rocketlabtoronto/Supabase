-- Ingest logs: record success/failure for ingestion scripts with timing and details
create table if not exists ingest_logs (
  id           bigserial primary key,
  script       text        not null,           -- e.g., 'ingest_simfin_financials_csv_to_postgres_us.py'
  status       text        not null,           -- 'success' or 'failure'
  message      text,                            -- brief summary
  details      jsonb,                           -- optional structured details
  started_at   timestamptz default now(),       -- when run started
  ended_at     timestamptz,                     -- when run ended
  duration_ms  integer                          -- (ended - started) in ms
);

-- Helpful indexes for querying recent runs per script and by status
create index if not exists idx_ingest_logs_script_started on ingest_logs (script, started_at desc);
create index if not exists idx_ingest_logs_status_started on ingest_logs (status, started_at desc);
