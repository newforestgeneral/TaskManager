-- ============================================================
--  Newforest Task Tracker — Supabase Schema
--  Paste this into: SQL Editor → New query → Run
-- ============================================================


-- ── TASKS ────────────────────────────────────────────────────
create table tasks (
  id                 uuid primary key default gen_random_uuid(),
  name               text not null,
  creator_name       text,
  description        text,
  project            text,
  lineage            text,
  stage              text not null default 'assigned',
  priority           text not null default 'medium',
  assignees          text[]  default '{}',
  start_date         date,
  due_date           date,
  follow_up_date     date,
  estimated_hours    numeric,
  percent_complete   integer not null default 0,
  human_resources    integer,
  location           text,
  suggested_schedule text,
  weather_dependent  text not null default 'no',
  weather_note       text,
  steps              jsonb   not null default '[]',
  materials          jsonb   not null default '[]',
  tools              text[]  default '{}',
  task_notes         text,
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now()
);

-- ── TASK UPDATES ─────────────────────────────────────────────
create table task_updates (
  id         uuid primary key default gen_random_uuid(),
  task_id    uuid not null references tasks(id) on delete cascade,
  author     text not null,
  text       text not null,
  date       date not null default current_date,
  via        text not null default 'Web app',
  created_at timestamptz not null default now()
);

-- ── PROFILES (used in Phase 2 auth) ──────────────────────────
create table profiles (
  id         uuid primary key references auth.users(id) on delete cascade,
  name       text,
  role       text not null default 'worker',  -- 'manager' or 'worker'
  lineage    text,
  created_at timestamptz not null default now()
);

-- ── AUTO-UPDATE updated_at on tasks ──────────────────────────
create or replace function set_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create trigger tasks_updated_at
  before update on tasks
  for each row execute function set_updated_at();

-- ── INDEXES ──────────────────────────────────────────────────
create index idx_tasks_stage    on tasks(stage);
create index idx_tasks_lineage  on tasks(lineage);
create index idx_tasks_priority on tasks(priority);
create index idx_task_updates_task_id on task_updates(task_id);

-- ── ROW LEVEL SECURITY (disabled until auth is added) ────────
alter table tasks        enable row level security;
alter table task_updates enable row level security;
alter table profiles     enable row level security;

-- Temporary open policies — lets the app read/write without login.
-- These get replaced when we add auth in Step 4.
create policy "allow all tasks"        on tasks        for all using (true) with check (true);
create policy "allow all task_updates" on task_updates for all using (true) with check (true);
create policy "allow all profiles"     on profiles     for all using (true) with check (true);

-- ── DONE ─────────────────────────────────────────────────────
-- After running this, go to:
--   Project Settings → API
-- Copy the "Project URL" and "anon public" key — you'll need both for Step 2.
