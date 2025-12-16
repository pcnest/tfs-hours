const express = require('express');
const path = require('path');
const { Pool } = require('pg');

const app = express();
app.use(express.json({ limit: '10mb' }));

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;
const SYNC_API_KEY = process.env.SYNC_API_KEY || '';
const TFS_WORKITEM_URL_TEMPLATE = process.env.TFS_WORKITEM_URL_TEMPLATE || '';

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
    tfsWorkItemUrlTemplate: TFS_WORKITEM_URL_TEMPLATE, // ".../_workitems/edit/{id}"
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

function addDaysIso(dateObj, days) {
  const d = new Date(dateObj.getTime() + days * 86400 * 1000);
  return d.toISOString();
}

// ---------- Ingest ----------
function buildUpsertLatest(rows) {
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
  const valuesSql = rows
    .map((r, idx) => {
      const base = idx * cols.length;
      const p = (i) => `$${base + i + 1}`;

      values.push(
        normInt(r.taskId),
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
  `;
  return { text, values };
}

function buildSnapshotInsert(runId, snapshotAt, rows) {
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
  const valuesSql = rows
    .map((r, idx) => {
      const base = idx * cols.length;
      const p = (i) => `$${base + i + 1}`;

      values.push(
        runId,
        snapshotAt,
        normInt(r.taskId),
        r.taskAssignedToUPN ?? null,
        r.taskAssignedTo ?? null,
        toDateOrNull(r.taskChangedDate),
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
    from = new Date(`${fromStr}T00:00:00.000Z`);
    const toInc = new Date(`${toStr}T00:00:00.000Z`);
    if (isNaN(from.getTime()) || isNaN(toInc.getTime())) {
      return res.status(400).json({ ok: false, error: 'invalid from/to date' });
    }
    toExclusive = new Date(toInc.getTime() + 86400 * 1000);
  }

  const params = [];
  const where = [];

  if (from && toExclusive) {
    params.push(from.toISOString(), toExclusive.toISOString());
    where.push(
      `task_changed_date >= $${params.length - 1} AND task_changed_date < $${
        params.length
      }`
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
    ORDER BY task_changed_date DESC NULLS LAST
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

  const from = new Date(`${fromStr}T00:00:00.000Z`);
  const toInclusive = new Date(`${toStr}T00:00:00.000Z`);
  if (isNaN(from.getTime()) || isNaN(toInclusive.getTime())) {
    return res.status(400).json({ ok: false, error: 'invalid from/to date' });
  }

  const toExclusive = new Date(toInclusive.getTime() + 86400 * 1000);

  const assignedToUPN = (req.query.assignedToUPN || '').toString().trim();
  const accountCodeRaw = (req.query.accountCode || '').toString().trim();
  const accountCode = accountCodeRaw ? Number(accountCodeRaw) : null;

  const params = [from.toISOString(), toExclusive.toISOString(), bucket];
  let idx = params.length;

  // optional filters
  const filters = [];
  if (assignedToUPN) {
    idx += 1;
    params.push(assignedToUPN);
    filters.push(`AND d.task_assigned_upn = $${idx}`);
  }
  if (Number.isFinite(accountCode)) {
    idx += 1;
    params.push(accountCode);
    filters.push(`AND d.account_code = $${idx}`);
  }

  const sql = `
    WITH prior AS (
      SELECT DISTINCT ON (task_id)
        task_id,
        snapshot_at,
        task_assigned_upn,
        task_assigned_to,
        account_code,
        COALESCE(task_actual_hours, 0) AS h
      FROM public.tfs_task_hours_snapshots
      WHERE snapshot_at < $1::timestamptz
      ORDER BY task_id, snapshot_at DESC
    ),
    inrange AS (
      SELECT
        task_id,
        snapshot_at,
        task_assigned_upn,
        task_assigned_to,
        account_code,
        COALESCE(task_actual_hours, 0) AS h
      FROM public.tfs_task_hours_snapshots
      WHERE snapshot_at >= $1::timestamptz
        AND snapshot_at <  $2::timestamptz
    ),
    s AS (
      SELECT * FROM prior
      UNION ALL
      SELECT * FROM inrange
    ),
    w AS (
      SELECT
        task_id,
        snapshot_at,
        task_assigned_upn,
        task_assigned_to,
        account_code,
        h,
        LAG(h) OVER (PARTITION BY task_id ORDER BY snapshot_at) AS prev_h
      FROM s
    ),
    d AS (
      SELECT
        date_trunc($3, snapshot_at) AS bucket,
        task_assigned_upn,
        task_assigned_to,
        account_code,
        CASE
          WHEN prev_h IS NULL THEN 0
          ELSE h - prev_h
        END AS delta_h
      FROM w
      WHERE snapshot_at >= $1::timestamptz
        AND snapshot_at <  $2::timestamptz
    )
    SELECT
      bucket,
      task_assigned_upn AS "assignedToUPN",
      task_assigned_to  AS "assignedTo",
      account_code      AS "accountCode",
      SUM(delta_h)      AS "hours"
    FROM d
    WHERE 1=1
      ${filters.join('\n      ')}
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

  const url = new URL('http://localhost/api/hours/summary');
  url.searchParams.set('bucket', bucket);
  url.searchParams.set('from', from);
  url.searchParams.set('to', to);
  if (assignedToUPN) url.searchParams.set('assignedToUPN', assignedToUPN);
  if (accountCode) url.searchParams.set('accountCode', accountCode);

  // call the handler by doing the query again (no HTTP)
  req.query.bucket = bucket;
  req.query.from = from;
  req.query.to = to;
  req.query.assignedToUPN = assignedToUPN;
  req.query.accountCode = accountCode;

  // run the same SQL by invoking pool query via a small local function:
  // simplest: duplicate minimal code:
  const bucketRaw = bucket.toLowerCase();
  const bucketAllowed = new Set(['day', 'week', 'month']);
  const unit = bucketAllowed.has(bucketRaw) ? bucketRaw : 'day';

  if (!from || !to) return res.status(400).send('from/to required');

  const fromD = new Date(`${from}T00:00:00.000Z`);
  const toD = new Date(`${to}T00:00:00.000Z`);
  if (isNaN(fromD.getTime()) || isNaN(toD.getTime()))
    return res.status(400).send('invalid from/to');

  const toExclusive = new Date(toD.getTime() + 86400 * 1000);

  const params = [fromD.toISOString(), toExclusive.toISOString(), unit];
  let idx = params.length;
  const filters = [];
  if (assignedToUPN) {
    idx += 1;
    params.push(assignedToUPN);
    filters.push(`AND d.task_assigned_upn = $${idx}`);
  }
  const ac = accountCode ? Number(accountCode) : null;
  if (Number.isFinite(ac)) {
    idx += 1;
    params.push(ac);
    filters.push(`AND d.account_code = $${idx}`);
  }

  const sql = `
    WITH prior AS (
      SELECT DISTINCT ON (task_id)
        task_id, snapshot_at, task_assigned_upn, task_assigned_to, account_code,
        COALESCE(task_actual_hours, 0) AS h
      FROM public.tfs_task_hours_snapshots
      WHERE snapshot_at < $1::timestamptz
      ORDER BY task_id, snapshot_at DESC
    ),
    inrange AS (
      SELECT
        task_id, snapshot_at, task_assigned_upn, task_assigned_to, account_code,
        COALESCE(task_actual_hours, 0) AS h
      FROM public.tfs_task_hours_snapshots
      WHERE snapshot_at >= $1::timestamptz
        AND snapshot_at <  $2::timestamptz
    ),
    s AS ( SELECT * FROM prior UNION ALL SELECT * FROM inrange ),
    w AS (
      SELECT
        task_id, snapshot_at, task_assigned_upn, task_assigned_to, account_code, h,
        LAG(h) OVER (PARTITION BY task_id ORDER BY snapshot_at) AS prev_h
      FROM s
    ),
    d AS (
      SELECT
        date_trunc($3, snapshot_at) AS bucket,
        task_assigned_upn,
        task_assigned_to,
        account_code,
        CASE WHEN prev_h IS NULL THEN h ELSE h - prev_h END AS delta_h
      FROM w
      WHERE snapshot_at >= $1::timestamptz
        AND snapshot_at <  $2::timestamptz
    )
    SELECT
      bucket,
      task_assigned_upn AS assigned_to_upn,
      task_assigned_to  AS assigned_to,
      account_code,
      SUM(delta_h)      AS hours
    FROM d
    WHERE 1=1
      ${filters.join('\n      ')}
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
      'assigned_to',
      'assigned_to_upn',
      'account_code',
      'hours',
    ];
    res.write(headers.join(',') + '\n');

    for (const row of r.rows) {
      const line = [
        row.bucket?.toISOString?.()
          ? row.bucket.toISOString().slice(0, 10)
          : row.bucket,
        row.assigned_to,
        row.assigned_to_upn,
        row.account_code,
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
