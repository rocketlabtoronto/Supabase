create table if not exists financials (
  id serial primary key,
  ticker text,
  exchange text,
  fy_end_date date,
  stmt_type text,
  tag text,
  value numeric,
  unit text,
  source text,
  inserted_at timestamp default now()
);

create index if not exists idx_financials_core on financials (ticker, fy_end_date, stmt_type);
