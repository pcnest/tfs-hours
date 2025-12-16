-- Sync runs (one row per agent execution)
CREATE TABLE IF NOT EXISTS public.tfs_hours_runs (
  run_id     bigserial PRIMARY KEY,
  run_at     timestamptz NOT NULL,
  source     text NOT NULL,
  item_count int NOT NULL
);

-- Latest state per Task (fast grid / lookup)
CREATE TABLE IF NOT EXISTS public.tfs_task_hours_latest (
  task_id            int PRIMARY KEY,
  task_title         text,
  task_changed_date  timestamptz,
  task_activity      text,
  task_assigned_to   text,
  task_assigned_upn  text,
  task_actual_hours  double precision,

  parent_id          int,
  parent_type        text,
  parent_title       text,
  account_code       int,

  synced_at          timestamptz NOT NULL
);

-- Snapshots per run (for deltas / rollups)
CREATE TABLE IF NOT EXISTS public.tfs_task_hours_snapshots (
  run_id            bigint NOT NULL REFERENCES public.tfs_hours_runs(run_id),
  snapshot_at       timestamptz NOT NULL,

  task_id           int NOT NULL,
  task_assigned_upn text,
  task_assigned_to  text,
  task_changed_date timestamptz,
  task_activity     text,
  task_actual_hours double precision,

  parent_id         int,
  account_code      int,

  PRIMARY KEY (run_id, task_id)
);

CREATE INDEX IF NOT EXISTS ix_hours_snap_task_time
  ON public.tfs_task_hours_snapshots(task_id, snapshot_at);

CREATE INDEX IF NOT EXISTS ix_hours_snap_time
  ON public.tfs_task_hours_snapshots(snapshot_at);

CREATE INDEX IF NOT EXISTS ix_hours_latest_assigned
  ON public.tfs_task_hours_latest(task_assigned_upn);
