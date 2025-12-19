const express = require('express');
const path = require('path');
const { Pool } = require('pg');

const app = express();
app.use(express.json({ limit: '10mb' }));

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;
const SYNC_API_KEY = process.env.SYNC_API_KEY || '';
const TFS_WORKITEM_URL_TEMPLATE = process.env.TFS_WORKITEM_URL_TEMPLATE || '';
const REPORT_TZ_OFFSET_MINUTES = Number(
  process.env.REPORT_TZ_OFFSET_MINUTES || '0'
); // PST = -480
const REPORT_TZ_LABEL = process.env.REPORT_TZ_LABEL || 'UTC';

if (!DATABASE_URL) {
  console.error('ERROR: DATABASE_URL env var not set.');
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl:
    process.env.PGSSLMODE === 'disable' ? false : { rejectUnauthorized: false },
});

// ---------- Health ----------
app.get('/health', async (req, res) => {
  try {
    const r = await pool.query('select 1 as ok');
    res.json({ ok: true, db: r.rows?.[0]?.ok === 1 });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.get('/api/config', (req, res) => {
  res.json({
    ok: true,
    tfsWorkItemUrlTemplate: TFS_WORKITEM_URL_TEMPLATE,
    reportTzOffsetMinutes: Number.isFinite(REPORT_TZ_OFFSET_MINUTES)
      ? REPORT_TZ_OFFSET_MINUTES
      : 0,
    reportTzLabel: REPORT_TZ_LABEL,
  });
});

// ---------- Helpers ----------
function requireApiKey(req, res) {
  if (!SYNC_API_KEY) return true; // leaving empty disables auth (not recommended)
  const key = req.header('x-api-key');
  if (!key || key !== SYNC_API_KEY) {
    res.status(401).json({ error: 'unauthorized' });
    return false;
  }
  return true;
}

function toDateOrNull(v) {
  if (!v) return null;
  const d = new Date(v);
  return isNaN(d.getTime()) ? null : d;
}

function normInt(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function normNum(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function getReportOffsetMinutes() {
  return Number.isFinite(REPORT_TZ_OFFSET_MINUTES)
    ? REPORT_TZ_OFFSET_MINUTES
    : 0;
}

// Parses "YYYY-MM-DD" safely
function parseYmd(s) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(s || '').trim());
  if (!m) return null;
  const y = Number(m[1]);
  const mo = Number(m[2]);
  const d = Number(m[3]);
  if (!Number.isFinite(y) || !Number.isFinite(mo) || !Number.isFinite(d))
    return null;
  return { y, mo, d };
}

// Converts local-midnight (in report timezone) to UTC Date.
// Formula: utc = UTCmidnight(date) - offsetMinutes
function localMidnightToUtcDate(dateStr, offsetMinutes) {
  const p = parseYmd(dateStr);
  if (!p) return null;
  const utcMidnightMs = Date.UTC(p.y, p.mo - 1, p.d, 0, 0, 0, 0);
  return new Date(utcMidnightMs - offsetMinutes * 60 * 1000);
}

// From/To are *calendar days* in report timezone (PST).
// Returns { fromUtc, toExclusiveUtc } where toExclusive is next-day midnight (PST) in UTC.
function rangeFromToUtc(fromStr, toStr, offsetMinutes) {
  const fromUtc = localMidnightToUtcDate(fromStr, offsetMinutes);
  const toStartUtc = localMidnightToUtcDate(toStr, offsetMinutes);
  if (
    !fromUtc ||
    !toStartUtc ||
    isNaN(fromUtc.getTime()) ||
    isNaN(toStartUtc.getTime())
  )
    return null;
  const toExclusiveUtc = new Date(toStartUtc.getTime() + 86400 * 1000);
  return { fromUtc, toExclusiveUtc };
}

// ---------- Ingest ----------
function buildUpsertLatest(rows) {
  // Reduce to latest per task_id within this batch to avoid ON CONFLICT
  // collisions in a single INSERT statement.
  const latestByTask = new Map();
  for (const r of rows) {
    const tid = normInt(r.taskId);
    if (tid === null || tid === undefined) continue;
    const tcd = toDateOrNull(r.taskChangedDate);
    const prev = latestByTask.get(tid);
    if (!prev) {
      latestByTask.set(tid, { ...r, taskId: tid, taskChangedDate: tcd });
      continue;
    }
    const prevDate = prev.taskChangedDate;
    if (!prevDate || (tcd && tcd > prevDate)) {
      latestByTask.set(tid, { ...r, taskId: tid, taskChangedDate: tcd });
    }
  }
  const uniq = Array.from(latestByTask.values());

  const cols = [
    'task_id',
    'task_title',
    'task_changed_date',
    'task_activity',
    'task_assigned_to',
    'task_assigned_upn',
    'task_actual_hours',
    'parent_id',
    'parent_type',
    'parent_title',
    'account_code',
    'synced_at',
  ];

  const values = [];
  const valuesSql = uniq
    .map((r, idx) => {
      const base = idx * cols.length;
      const p = (i) => `$${base + i + 1}`;

      values.push(
        r.taskId,
        r.taskTitle ?? null,
        toDateOrNull(r.taskChangedDate),
        r.activity ?? null,
        r.taskAssignedTo ?? null,
        r.taskAssignedToUPN ?? null,
        normNum(r.actualHours),
        normInt(r.parentId),
        r.parentType ?? null,
        r.parentTitle ?? null,
        normInt(r.accountCode),
        toDateOrNull(r.syncedAtUtc) ?? new Date()
      );

      return `(${cols.map((_, j) => p(j)).join(',')})`;
    })
    .join(',');

  const text = `
    INSERT INTO public.tfs_task_hours_latest (${cols.join(',')})
    VALUES ${valuesSql}
    ON CONFLICT (task_id) DO UPDATE SET
      task_title        = EXCLUDED.task_title,
      task_changed_date = EXCLUDED.task_changed_date,
      task_activity     = EXCLUDED.task_activity,
      task_assigned_to  = EXCLUDED.task_assigned_to,
      task_assigned_upn = EXCLUDED.task_assigned_upn,
      task_actual_hours = EXCLUDED.task_actual_hours,
      parent_id         = EXCLUDED.parent_id,
      parent_type       = EXCLUDED.parent_type,
      parent_title      = EXCLUDED.parent_title,
      account_code      = EXCLUDED.account_code,
      synced_at         = EXCLUDED.synced_at
    WHERE public.tfs_task_hours_latest.task_changed_date <= EXCLUDED.task_changed_date
  `;
  return { text, values };
}

function buildSnapshotInsert(runId, snapshotAt, rows) {
  // Dedupe within the batch by (task_id, task_changed_date) so a single insert
  // doesn't generate multiple conflicts on the same row for this run.
  const seen = new Set();
  const uniq = [];
  for (const r of rows) {
    const tid = normInt(r.taskId);
    const tcd = toDateOrNull(r.taskChangedDate);
    const key = `${tid ?? 'null'}|${tcd ? tcd.toISOString() : 'null'}`;
    if (seen.has(key)) continue;
    seen.add(key);
    uniq.push({ ...r, taskId: tid, taskChangedDate: tcd });
  }

  const cols = [
    'run_id',
    'snapshot_at',
    'task_id',
    'task_assigned_upn',
    'task_assigned_to',
    'task_changed_date',
    'task_activity',
    'task_actual_hours',
    'parent_id',
    'account_code',
  ];

  const values = [];
  const valuesSql = uniq
    .map((r, idx) => {
      const base = idx * cols.length;
      const p = (i) => `$${base + i + 1}`;

      values.push(
        runId,
        snapshotAt,
        r.taskId ?? null,
        r.taskAssignedToUPN ?? null,
        r.taskAssignedTo ?? null,
        r.taskChangedDate,
        r.activity ?? null,
        normNum(r.actualHours),
        normInt(r.parentId),
        normInt(r.accountCode)
      );

      return `(${cols.map((_, j) => p(j)).join(',')})`;
    })
    .join(',');

  const text = `
    INSERT INTO public.tfs_task_hours_snapshots (${cols.join(',')})
    VALUES ${valuesSql}
    ON CONFLICT (run_id, task_id, task_changed_date) DO UPDATE SET
      snapshot_at       = EXCLUDED.snapshot_at,
      task_assigned_upn = EXCLUDED.task_assigned_upn,
      task_assigned_to  = EXCLUDED.task_assigned_to,
      task_activity     = EXCLUDED.task_activity,
      task_actual_hours = EXCLUDED.task_actual_hours,
      parent_id         = EXCLUDED.parent_id,
      account_code      = EXCLUDED.account_code
  `;
  return { text, values };
}

app.post('/api/tfs-hours-sync', async (req, res) => {
  if (!requireApiKey(req, res)) return;

  const { source, syncedAtUtc, rows } = req.body || {};
  if (!rows || !Array.isArray(rows) || rows.length === 0) {
    return res.status(400).json({ ok: false, error: 'rows array required' });
  }

  const syncTs = syncedAtUtc ? new Date(syncedAtUtc) : new Date();
  const src = source ?? 'tfs-hours-sync';

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const runR = await client.query(
      `INSERT INTO public.tfs_hours_runs(run_at, source, item_count)
       VALUES ($1, $2, $3)
       RETURNING run_id, run_at`,
      [syncTs, src, rows.length]
    );
    const runId = runR.rows[0].run_id;
    const runAt = runR.rows[0].run_at;

    const chunks = chunkArray(rows, 200);
    for (const ch of chunks) {
      const enriched = ch.map((r) => ({
        ...r,
        syncedAtUtc: runAt.toISOString(),
      }));

      const u = buildUpsertLatest(enriched);
      await client.query(u.text, u.values);

      const s = buildSnapshotInsert(runId, runAt, enriched);
      await client.query(s.text, s.values);
    }

    await client.query('COMMIT');
    res.json({ ok: true, count: rows.length, runId, runAt });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('INGEST ERROR:', e);
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  } finally {
    client.release();
  }
});

app.get('/api/hours/latest', async (req, res) => {
  const fromStr = (req.query.from || '').toString().trim(); // YYYY-MM-DD
  const toStr = (req.query.to || '').toString().trim(); // YYYY-MM-DD
  const assignedToUPN = (req.query.assignedToUPN || '').toString().trim();
  const accountCodeRaw = (req.query.accountCode || '').toString().trim();
  const accountCode = accountCodeRaw ? Number(accountCodeRaw) : null;

  const limit = Math.min(2000, Math.max(1, Number(req.query.limit || 200)));
  const offset = Math.max(0, Number(req.query.offset || 0));

  let from = null,
    toExclusive = null;
  if (fromStr && toStr) {
    const offsetMin = getReportOffsetMinutes(); // PST = -480
    const rng = rangeFromToUtc(fromStr, toStr, offsetMin);
    if (!rng)
      return res.status(400).json({ ok: false, error: 'invalid from/to date' });
    from = rng.fromUtc;
    toExclusive = rng.toExclusiveUtc;
  }

  const params = [];
  const where = [];

  if (from && toExclusive) {
    params.push(from.toISOString(), toExclusive.toISOString());
    where.push(
      `COALESCE(task_changed_date, synced_at) >= $${
        params.length - 1
      } AND COALESCE(task_changed_date, synced_at) < $${params.length}`
    );
  }

  // make AssignedToUPN filter forgiving (works even with older snapshots that stored "Name <UPN>")
  if (assignedToUPN) {
    params.push(`%${assignedToUPN}%`);
    where.push(`COALESCE(task_assigned_upn,'') ILIKE $${params.length}`);
  }

  if (Number.isFinite(accountCode)) {
    params.push(accountCode);
    where.push(`account_code = $${params.length}`);
  }

  params.push(limit, offset);

  const sql = `
    SELECT
      task_id,
      task_title,
      task_changed_date,
      task_activity,
      task_assigned_to,
      task_assigned_upn,
      task_actual_hours,
      parent_id,
      parent_type,
      parent_title,
      account_code,
      COUNT(*) OVER() AS total_count
    FROM public.tfs_task_hours_latest
    ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
    ORDER BY COALESCE(task_changed_date, synced_at) DESC NULLS LAST

    LIMIT $${params.length - 1} OFFSET $${params.length}
  `;

  try {
    const r = await pool.query(sql, params);
    const total = r.rows.length ? Number(r.rows[0].total_count) : 0;
    const rows = r.rows.map(({ total_count, ...rest }) => rest);
    res.json({ ok: true, total, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- Hours summary (delta-based; supports negative corrections) ----------
app.get('/api/hours/summary', async (req, res) => {
  const bucketRaw = (req.query.bucket || 'day').toString().trim().toLowerCase();
  const bucketAllowed = new Set(['day', 'week', 'month']);
  const bucket = bucketAllowed.has(bucketRaw) ? bucketRaw : 'day';

  const fromStr = (req.query.from || '').toString().trim(); // YYYY-MM-DD
  const toStr = (req.query.to || '').toString().trim(); // YYYY-MM-DD (inclusive in UI)

  if (!fromStr || !toStr) {
    return res
      .status(400)
      .json({ ok: false, error: 'from and to required (YYYY-MM-DD)' });
  }

  const offsetMin = getReportOffsetMinutes(); // PST = -480
  const rng = rangeFromToUtc(fromStr, toStr, offsetMin);
  if (!rng) {
    return res.status(400).json({ ok: false, error: 'invalid from/to date' });
  }
  const from = rng.fromUtc;
  const toExclusive = rng.toExclusiveUtc;

  const assignedToUPN = (req.query.assignedToUPN || '').toString().trim();
  const accountCodeRaw = (req.query.accountCode || '').toString().trim();
  const accountCode = accountCodeRaw ? Number(accountCodeRaw) : null;

  const params = [
    from.toISOString(),
    toExclusive.toISOString(),
    bucket,
    offsetMin,
  ];
  let idx = params.length;

  // optional filters
  const filters = [];
  if (assignedToUPN) {
    idx += 1;
    params.push(`%${assignedToUPN}%`);
    filters.push(`AND COALESCE(d.task_assigned_upn,'') ILIKE $${idx}`);
  }
  if (Number.isFinite(accountCode)) {
    idx += 1;
    params.push(accountCode);
    filters.push(`AND d.account_code = $${idx}`);
  }

  const sql = `
  WITH snaps AS (
    -- Deduplicate by (task_id, effective change time) taking the latest snapshot per change
    SELECT DISTINCT ON (task_id, COALESCE(task_changed_date, snapshot_at))
      task_id,
      snapshot_at,
      COALESCE(task_changed_date, snapshot_at) AS t,
      task_assigned_upn,
      task_assigned_to,
      account_code,
      COALESCE(task_actual_hours, 0) AS h
    FROM public.tfs_task_hours_snapshots
    ORDER BY task_id, COALESCE(task_changed_date, snapshot_at), snapshot_at DESC, run_id DESC
  ),
  prior AS (
    SELECT DISTINCT ON (task_id)
      task_id, snapshot_at, t, task_assigned_upn, task_assigned_to, account_code, h
    FROM snaps
    WHERE t < $1::timestamptz
    ORDER BY task_id, t DESC, snapshot_at DESC
  ),
  inrange AS (
    SELECT
      task_id, snapshot_at, t, task_assigned_upn, task_assigned_to, account_code, h
    FROM snaps
    WHERE t >= $1::timestamptz AND t < $2::timestamptz
  ),
  s AS (
    SELECT * FROM prior
    UNION ALL
    SELECT * FROM inrange
  ),
  w AS (
    SELECT
      task_id, snapshot_at, t, task_assigned_upn, task_assigned_to, account_code, h,
      LAG(h) OVER (PARTITION BY task_id ORDER BY t, snapshot_at) AS prev_h
    FROM s
  ),
  d AS (
    SELECT
      (date_trunc($3, t + ($4 || ' minutes')::interval) - ($4 || ' minutes')::interval) AS bucket,
      task_assigned_upn,
      task_assigned_to,
      account_code,
      (h - COALESCE(prev_h, 0)) AS delta_h
    FROM w
    WHERE t >= $1::timestamptz AND t < $2::timestamptz
  )
  SELECT
    bucket,
    task_assigned_upn AS "assignedToUPN",
    task_assigned_to  AS "assignedTo",
    account_code      AS "accountCode",
    SUM(delta_h)      AS "hours"
  FROM d
  WHERE 1=1
    ${filters.join('\n ')}
  GROUP BY 1,2,3,4
  ORDER BY 1 ASC, 3 ASC;
`;

  try {
    const r = await pool.query(sql, params);
    res.json({
      ok: true,
      bucket,
      from: fromStr,
      to: toStr,
      rows: r.rows,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- Entries (by Date Changed) ----------
app.get('/api/hours/entries', async (req, res) => {
  const fromStr = (req.query.from || '').toString().trim(); // YYYY-MM-DD
  const toStr = (req.query.to || '').toString().trim(); // YYYY-MM-DD inclusive
  if (!fromStr || !toStr) {
    return res
      .status(400)
      .json({ ok: false, error: 'from and to required (YYYY-MM-DD)' });
  }

  const offsetMin = getReportOffsetMinutes(); // PST = -480
  const rng = rangeFromToUtc(fromStr, toStr, offsetMin);
  if (!rng) {
    return res.status(400).json({ ok: false, error: 'invalid from/to date' });
  }
  const from = rng.fromUtc;
  const toExclusive = rng.toExclusiveUtc;

  const assignedToUPN = (req.query.assignedToUPN || '').toString().trim();
  const accountCodeRaw = (req.query.accountCode || '').toString().trim();
  const accountCode = accountCodeRaw ? Number(accountCodeRaw) : null;

  const limit = Math.min(5000, Math.max(1, Number(req.query.limit || 500)));
  const offset = Math.max(0, Number(req.query.offset || 0));

  const params = [from.toISOString(), toExclusive.toISOString()];
  let idx = params.length;

  const filters = [];
  if (assignedToUPN) {
    idx += 1;
    params.push(`%${assignedToUPN}%`);
    filters.push(`AND COALESCE(d.task_assigned_upn,'') ILIKE $${idx}`);
  }
  if (Number.isFinite(accountCode)) {
    idx += 1;
    params.push(accountCode);
    filters.push(`AND d.account_code = $${idx}`);
  }

  idx += 1;
  params.push(limit);
  idx += 1;
  params.push(offset);

  const sql = `
    WITH snaps AS (
      -- Deduplicate by (task_id, effective change time); keep the latest snapshot per change
      SELECT DISTINCT ON (s.task_id, COALESCE(s.task_changed_date, s.snapshot_at))
        s.run_id,
        s.snapshot_at,
        COALESCE(s.task_changed_date, s.snapshot_at) AS t,
        s.task_id,
        s.task_assigned_upn,
        s.task_assigned_to,
        s.task_activity,
        COALESCE(s.task_actual_hours, 0) AS h,
        s.parent_id,
        s.account_code
      FROM public.tfs_task_hours_snapshots s
      ORDER BY s.task_id, COALESCE(s.task_changed_date, s.snapshot_at), s.snapshot_at DESC, s.run_id DESC
    ),
    prior AS (
      SELECT DISTINCT ON (task_id)
        task_id, snapshot_at, t, h
      FROM snaps
      WHERE t < $1::timestamptz
      ORDER BY task_id, t DESC, snapshot_at DESC
    ),
    inrange AS (
      SELECT *
      FROM snaps
      WHERE t >= $1::timestamptz AND t < $2::timestamptz
    ),
    s AS (
      SELECT
        NULL::bigint AS run_id,
        p.snapshot_at,
        p.t,
        p.task_id,
        NULL::text AS task_assigned_upn,
        NULL::text AS task_assigned_to,
        NULL::text AS task_activity,
        p.h,
        NULL::int  AS parent_id,
        NULL::int  AS account_code,
        TRUE AS is_prior
      FROM prior p
      UNION ALL
      SELECT
        i.run_id,
        i.snapshot_at,
        i.t,
        i.task_id,
        i.task_assigned_upn,
        i.task_assigned_to,
        i.task_activity,
        i.h,
        i.parent_id,
        i.account_code,
        FALSE AS is_prior
      FROM inrange i
    ),
    w AS (
      SELECT
        *,
        LAG(h) OVER (PARTITION BY task_id ORDER BY t, snapshot_at) AS prev_h
      FROM s
    ),
    d AS (
      SELECT
        run_id,
        snapshot_at,
        t AS changed_at,
        task_id,
        task_assigned_upn,
        task_assigned_to,
        task_activity,
        COALESCE(prev_h, 0) AS prev_hours,
        h AS actual_hours,
        (h - COALESCE(prev_h, 0)) AS delta_hours,
        parent_id,
        account_code
      FROM w
      WHERE is_prior = FALSE
    )
    SELECT
      d.changed_at,
      d.snapshot_at,
      d.task_id,
      l.task_title,
      d.task_activity,
      d.task_assigned_to,
      d.task_assigned_upn,
      d.prev_hours,
      d.actual_hours,
      d.delta_hours,
      d.parent_id,
      l.parent_type,
      l.parent_title,
      d.account_code,
      COUNT(*) OVER() AS total_count
    FROM d
    LEFT JOIN public.tfs_task_hours_latest l ON l.task_id = d.task_id
    WHERE 1=1
      ${filters.join('\n ')}
    ORDER BY d.changed_at ASC, d.task_id ASC
    LIMIT $${idx - 1} OFFSET $${idx};
  `;

  try {
    const r = await pool.query(sql, params);
    const total = r.rows.length ? Number(r.rows[0].total_count) : 0;
    const rows = r.rows.map(({ total_count, ...rest }) => rest);
    res.json({ ok: true, total, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- CSV export ----------
function csvEscape(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  if (/[,"\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

app.get('/api/hours/export.csv', async (req, res) => {
  // just call JSON endpoint internally by reusing the same query logic:
  // easiest way: do a direct fetch in JS; but server-side is cleaner.
  // We'll replicate the query call by hitting /api/hours/summary logic quickly:
  const bucket = (req.query.bucket || 'day').toString().trim();
  const from = (req.query.from || '').toString().trim();
  const to = (req.query.to || '').toString().trim();
  const assignedToUPN = (req.query.assignedToUPN || '').toString().trim();
  const accountCode = (req.query.accountCode || '').toString().trim();

  // run the same SQL by invoking pool query via a small local function:
  // simplest: duplicate minimal code:
  const bucketRaw = bucket.toLowerCase();
  const bucketAllowed = new Set(['day', 'week', 'month']);
  const unit = bucketAllowed.has(bucketRaw) ? bucketRaw : 'day';

  if (!from || !to) return res.status(400).send('from/to required');

  const offsetMin = getReportOffsetMinutes(); // PST = -480
  const rng = rangeFromToUtc(from, to, offsetMin);
  if (!rng) return res.status(400).send('invalid from/to');

  const fromD = rng.fromUtc;
  const toExclusive = rng.toExclusiveUtc;

  const params = [
    fromD.toISOString(),
    toExclusive.toISOString(),
    unit,
    offsetMin,
  ];
  let idx = params.length;

  const filters = [];
  if (assignedToUPN) {
    idx += 1;
    params.push(`%${assignedToUPN}%`);
    filters.push(`AND COALESCE(d.task_assigned_upn,'') ILIKE $${idx}`);
  }

  const ac = accountCode ? Number(accountCode) : null;
  if (Number.isFinite(ac)) {
    idx += 1;
    params.push(ac);
    filters.push(`AND d.account_code = $${idx}`);
  }

  const sql = `
  WITH snaps AS (
    -- Deduplicate by (task_id, effective change time) taking the latest snapshot per change
    SELECT DISTINCT ON (task_id, COALESCE(task_changed_date, snapshot_at))
      task_id,
      snapshot_at,
      COALESCE(task_changed_date, snapshot_at) AS t,
      task_assigned_upn,
      task_assigned_to,
      account_code,
      COALESCE(task_actual_hours, 0) AS h
    FROM public.tfs_task_hours_snapshots
    ORDER BY task_id, COALESCE(task_changed_date, snapshot_at), snapshot_at DESC, run_id DESC
  ),
  prior AS (
    SELECT DISTINCT ON (task_id)
      task_id, snapshot_at, t, task_assigned_upn, task_assigned_to, account_code, h
    FROM snaps
    WHERE t < $1::timestamptz
    ORDER BY task_id, t DESC, snapshot_at DESC
  ),
  inrange AS (
    SELECT
      task_id, snapshot_at, t, task_assigned_upn, task_assigned_to, account_code, h
    FROM snaps
    WHERE t >= $1::timestamptz AND t < $2::timestamptz
  ),
  s AS (
    SELECT * FROM prior
    UNION ALL
    SELECT * FROM inrange
  ),
  w AS (
    SELECT
      task_id, snapshot_at, t, task_assigned_upn, task_assigned_to, account_code, h,
      LAG(h) OVER (PARTITION BY task_id ORDER BY t, snapshot_at) AS prev_h
    FROM s
  ),
  d AS (
    SELECT
      (date_trunc($3, t + ($4 || ' minutes')::interval) - ($4 || ' minutes')::interval) AS bucket,

      task_assigned_upn,
      task_assigned_to,
      account_code,
      (h - COALESCE(prev_h, 0)) AS delta_h
    FROM w
    WHERE t >= $1::timestamptz AND t < $2::timestamptz
  )
  SELECT
    bucket,
    task_assigned_upn AS "assignedToUPN",
    task_assigned_to  AS "assignedTo",
    account_code      AS "accountCode",
    SUM(delta_h)      AS "hours"
  FROM d
  WHERE 1=1
    ${filters.join('\n ')}
  GROUP BY 1,2,3,4
  ORDER BY 1 ASC, 3 ASC;
`;

  try {
    const r = await pool.query(sql, params);

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader(
      'Content-Disposition',
      'attachment; filename=tfs_hours_summary.csv'
    );

    const headers = [
      'bucket',
      'assignedTo',
      'assignedToUPN',
      'accountCode',
      'hours',
    ];

    res.write(headers.join(',') + '\n');

    for (const row of r.rows) {
      const line = [
        row.bucket?.toISOString?.()
          ? row.bucket.toISOString().slice(0, 10)
          : row.bucket,
        row.assignedTo,
        row.assignedToUPN,
        row.accountCode,
        row.hours,
      ]

        .map(csvEscape)
        .join(',');
      res.write(line + '\n');
    }

    res.end();
  } catch (e) {
    res.status(500).send(String(e?.message || e));
  }
});

// ---------- Static UI ----------
app.use('/', express.static(path.join(__dirname, 'public')));

app.listen(PORT, () => {
  console.log(`tfs-hours-dashboard listening on :${PORT}`);
});
