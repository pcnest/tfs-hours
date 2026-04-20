const express = require('express');
const path = require('path');
const { Pool } = require('pg');
const nodemailer = require('nodemailer');

const app = express();
app.use(express.json({ limit: '10mb' }));

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;
const SYNC_API_KEY = process.env.SYNC_API_KEY || '';
const TFS_WORKITEM_URL_TEMPLATE = process.env.TFS_WORKITEM_URL_TEMPLATE || '';
const REPORT_TZ_OFFSET_MINUTES = Number(
  process.env.REPORT_TZ_OFFSET_MINUTES || '0',
); // PST = -480
const REPORT_TZ_LABEL = process.env.REPORT_TZ_LABEL || 'UTC';
const REPORT_TZ_IANA = (process.env.REPORT_TZ_IANA || '').trim();

const BREVO_SMTP_USER = process.env.BREVO_SMTP_USER || '';
const BREVO_SMTP_KEY = process.env.BREVO_SMTP_KEY || '';
const NOTIFY_FROM_EMAIL = process.env.NOTIFY_FROM_EMAIL || '';
const NOTIFY_FROM_NAME = process.env.NOTIFY_FROM_NAME || 'TFS Hours Report';
const NOTIFY_MANAGER_EMAIL = process.env.NOTIFY_MANAGER_EMAIL || '';

function createMailTransporter() {
  return nodemailer.createTransport({
    host: 'smtp-relay.brevo.com',
    port: 587,
    secure: false,
    auth: { user: BREVO_SMTP_USER, pass: BREVO_SMTP_KEY },
  });
}

function escapeEmailHtml(v) {
  return String(v ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function fmtH(n) {
  const v = Number(n);
  return Number.isFinite(v) ? v.toFixed(1) : '0.0';
}

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
    reportTzIana: REPORT_TZ_IANA || null,
    notifyManagerEmail: NOTIFY_MANAGER_EMAIL || null,
    smtpConfigured: !!(BREVO_SMTP_USER && BREVO_SMTP_KEY && NOTIFY_FROM_EMAIL),
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

function normText(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s ? s : null;
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

function getReportTimeZone() {
  return REPORT_TZ_IANA || null;
}

function getTimeZoneParts(date, timeZone) {
  const dtf = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
  const parts = dtf.formatToParts(date);
  const map = {};
  for (const p of parts) {
    if (p.type !== 'literal') map[p.type] = p.value;
  }
  return {
    year: Number(map.year),
    month: Number(map.month),
    day: Number(map.day),
    hour: Number(map.hour),
    minute: Number(map.minute),
    second: Number(map.second),
  };
}

function getTimeZoneOffsetMinutes(date, timeZone) {
  const parts = getTimeZoneParts(date, timeZone);
  const asUtc = Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    parts.hour,
    parts.minute,
    parts.second,
  );
  return (asUtc - date.getTime()) / 60000;
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

function addDaysToYmd(ymd, days) {
  const p = parseYmd(ymd);
  if (!p) return null;
  const base = Date.UTC(p.y, p.mo - 1, p.d, 0, 0, 0, 0);
  return new Date(base + days * 86400 * 1000).toISOString().slice(0, 10);
}

// Converts local-midnight (in report timezone) to UTC Date.
// Formula: utc = UTCmidnight(date) - offsetMinutes
function localMidnightToUtcDate(dateStr, offsetMinutes) {
  const p = parseYmd(dateStr);
  if (!p) return null;
  const utcMidnightMs = Date.UTC(p.y, p.mo - 1, p.d, 0, 0, 0, 0);
  return new Date(utcMidnightMs - offsetMinutes * 60 * 1000);
}

function localMidnightToUtcDateInZone(dateStr, timeZone) {
  const p = parseYmd(dateStr);
  if (!p) return null;
  const utcMidnight = new Date(Date.UTC(p.y, p.mo - 1, p.d, 0, 0, 0, 0));
  let offset = getTimeZoneOffsetMinutes(utcMidnight, timeZone);
  let utc = new Date(utcMidnight.getTime() - offset * 60 * 1000);
  const offset2 = getTimeZoneOffsetMinutes(utc, timeZone);
  if (offset2 !== offset) {
    utc = new Date(utcMidnight.getTime() - offset2 * 60 * 1000);
  }
  return utc;
}

// From/To are *calendar days* in report timezone.
// Returns { fromUtc, toExclusiveUtc } where toExclusive is next-day midnight in that timezone (UTC).
function rangeFromToUtc(fromStr, toStr, offsetMinutes, timeZone) {
  const toNext = addDaysToYmd(toStr, 1);
  const fromUtc = timeZone
    ? localMidnightToUtcDateInZone(fromStr, timeZone)
    : localMidnightToUtcDate(fromStr, offsetMinutes);
  const toExclusiveUtc = timeZone
    ? localMidnightToUtcDateInZone(toNext, timeZone)
    : localMidnightToUtcDate(toNext, offsetMinutes);
  if (
    !fromUtc ||
    !toExclusiveUtc ||
    isNaN(fromUtc.getTime()) ||
    isNaN(toExclusiveUtc.getTime())
  )
    return null;
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
    'task_changed_by',
    'task_changed_by_upn',
    'task_actual_hours',
    'ticket_id',
    'ticket_type',
    'ticket_title',
    'feature_id',
    'feature_title',
    'cost_type',
    'synced_at',
  ];

  const values = [];
  const valuesSql = uniq
    .map((r, idx) => {
      const base = idx * cols.length;
      const p = (i) => `$${base + i + 1}`;

      const ticketId = normInt(r.ticketId ?? r.parentId);
      const ticketType = normText(r.ticketType ?? r.parentType);
      const ticketTitle = normText(r.ticketTitle ?? r.parentTitle);
      const featureId = normInt(r.featureId);
      const featureTitle = normText(r.featureTitle);
      const costType = normText(r.costType ?? r.accountCode ?? r.account_code);

      values.push(
        r.taskId,
        r.taskTitle ?? null,
        toDateOrNull(r.taskChangedDate),
        r.activity ?? null,
        r.taskAssignedTo ?? null,
        r.taskAssignedToUPN ?? null,
        r.taskChangedBy ?? null,
        r.taskChangedByUPN ?? null,
        normNum(r.actualHours),
        ticketId,
        ticketType,
        ticketTitle,
        featureId,
        featureTitle,
        costType,
        toDateOrNull(r.syncedAtUtc) ?? new Date(),
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
      task_changed_by   = EXCLUDED.task_changed_by,
      task_changed_by_upn = EXCLUDED.task_changed_by_upn,
      task_actual_hours = EXCLUDED.task_actual_hours,
      ticket_id         = EXCLUDED.ticket_id,
      ticket_type       = EXCLUDED.ticket_type,
      ticket_title      = EXCLUDED.ticket_title,
      feature_id        = EXCLUDED.feature_id,
      feature_title     = EXCLUDED.feature_title,
      cost_type         = EXCLUDED.cost_type,
      synced_at         = EXCLUDED.synced_at
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
    'task_changed_by',
    'task_changed_by_upn',
    'task_changed_date',
    'task_activity',
    'task_actual_hours',
    'ticket_id',
    'feature_id',
    'cost_type',
  ];

  const values = [];
  const valuesSql = uniq
    .map((r, idx) => {
      const base = idx * cols.length;
      const p = (i) => `$${base + i + 1}`;

      const ticketId = normInt(r.ticketId ?? r.parentId);
      const featureId = normInt(r.featureId);
      const costType = normText(r.costType ?? r.accountCode ?? r.account_code);

      values.push(
        runId,
        snapshotAt,
        r.taskId ?? null,
        r.taskAssignedToUPN ?? null,
        r.taskAssignedTo ?? null,
        r.taskChangedBy ?? null,
        r.taskChangedByUPN ?? null,
        r.taskChangedDate,
        r.activity ?? null,
        normNum(r.actualHours),
        ticketId,
        featureId,
        costType,
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
      task_changed_by   = EXCLUDED.task_changed_by,
      task_changed_by_upn = EXCLUDED.task_changed_by_upn,
      task_activity     = EXCLUDED.task_activity,
      task_actual_hours = EXCLUDED.task_actual_hours,
      ticket_id         = EXCLUDED.ticket_id,
      feature_id        = EXCLUDED.feature_id,
      cost_type         = EXCLUDED.cost_type
  `;
  return { text, values };
}

// ---------- Shared summary SQL ----------
// Params layout: [$1=fromUtc, $2=toExclusiveUtc, $3=bucket, $4=offsetMinutes, $5+=filter values]
// filters: array of SQL fragments like "AND COALESCE(d.task_assigned_to,'') ILIKE $5"
function buildSummarySql(filters) {
  return `
  WITH snaps AS (
    SELECT DISTINCT ON (task_id, COALESCE(task_changed_date, snapshot_at))
      task_id,
      snapshot_at,
      COALESCE(task_changed_date, snapshot_at) AS t,
      task_assigned_upn,
      task_assigned_to,
      cost_type,
      COALESCE(task_actual_hours, 0) AS h
    FROM public.tfs_task_hours_snapshots
    ORDER BY task_id, COALESCE(task_changed_date, snapshot_at), snapshot_at DESC, run_id DESC
  ),
  prior AS (
    SELECT DISTINCT ON (task_id)
      task_id, snapshot_at, t, task_assigned_upn, task_assigned_to, cost_type, h
    FROM snaps
    WHERE t < $1::timestamptz
    ORDER BY task_id, t DESC, snapshot_at DESC
  ),
  inrange AS (
    SELECT
      task_id, snapshot_at, t, task_assigned_upn, task_assigned_to, cost_type, h
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
      task_id, snapshot_at, t, task_assigned_upn, task_assigned_to, cost_type, h,
      LAG(h) OVER (PARTITION BY task_id ORDER BY t, snapshot_at) AS prev_h
    FROM s
  ),
  d AS (
    SELECT
      (date_trunc($3, t + ($4 || ' minutes')::interval) - ($4 || ' minutes')::interval) AS bucket,
      task_assigned_upn,
      task_assigned_to,
      cost_type,
      (h - COALESCE(prev_h, 0)) AS delta_h
    FROM w
    WHERE t >= $1::timestamptz AND t < $2::timestamptz
  )
  SELECT
    bucket,
    task_assigned_upn AS "assignedToUPN",
    task_assigned_to  AS "assignedTo",
    cost_type         AS "accountCode",
    cost_type         AS "costType",
    SUM(delta_h)      AS "hours"
  FROM d
  WHERE 1=1
    ${filters.join('\n    ')}
  GROUP BY 1,2,3,4
  ORDER BY 1 ASC, 3 ASC;`;
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
      [syncTs, src, rows.length],
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
  const changedBy = (req.query.changedBy || '').toString().trim();
  const changedByUPN = (req.query.changedByUPN || '').toString().trim();
  const assignedTo = (req.query.assignedTo || '').toString().trim();
  const assignedToUPN = (req.query.assignedToUPN || '').toString().trim();
  const costTypeRaw = (req.query.costType || req.query.accountCode || '')
    .toString()
    .trim();
  const costType = costTypeRaw || null;

  const limit = Math.min(2000, Math.max(1, Number(req.query.limit || 200)));
  const offset = Math.max(0, Number(req.query.offset || 0));

  let from = null,
    toExclusive = null;
  if (fromStr && toStr) {
    const offsetMin = getReportOffsetMinutes(); // PST = -480
    const tz = getReportTimeZone();
    const rng = rangeFromToUtc(fromStr, toStr, offsetMin, tz);
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
      } AND COALESCE(task_changed_date, synced_at) < $${params.length}`,
    );
  }

  // Changed By: allow name or UPN.
  const changedFilter = changedBy || changedByUPN;
  if (changedFilter) {
    params.push(`%${changedFilter}%`);
    where.push(
      `(COALESCE(task_changed_by,'') ILIKE $${params.length} OR COALESCE(task_changed_by_upn,'') ILIKE $${params.length})`,
    );
  }

  // Assigned To: allow name or UPN.
  const assignedFilter = assignedTo || assignedToUPN;
  if (assignedFilter) {
    params.push(`%${assignedFilter}%`);
    where.push(
      `(COALESCE(task_assigned_to,'') ILIKE $${params.length} OR COALESCE(task_assigned_upn,'') ILIKE $${params.length})`,
    );
  }

  if (costType) {
    params.push(costType);
    where.push(`LOWER(cost_type) = LOWER($${params.length})`);
  }

  // Default: match desktop behavior by excluding zero/negative hours.
  where.push('COALESCE(task_actual_hours, 0) > 0');

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
      ticket_id,
      ticket_type,
      ticket_title,
      feature_id,
      feature_title,
      cost_type,
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
  const changedBy = (req.query.changedBy || '').toString().trim();
  const changedByUPN = (req.query.changedByUPN || '').toString().trim();

  if (!fromStr || !toStr) {
    return res
      .status(400)
      .json({ ok: false, error: 'from and to required (YYYY-MM-DD)' });
  }

  const offsetMin = getReportOffsetMinutes(); // PST = -480
  const tz = getReportTimeZone();
  const rng = rangeFromToUtc(fromStr, toStr, offsetMin, tz);
  if (!rng) {
    return res.status(400).json({ ok: false, error: 'invalid from/to date' });
  }
  const from = rng.fromUtc;
  const toExclusive = rng.toExclusiveUtc;

  const assignedTo = (req.query.assignedTo || '').toString().trim();
  const assignedToUPN = (req.query.assignedToUPN || '').toString().trim();
  const costTypeRaw = (req.query.costType || req.query.accountCode || '')
    .toString()
    .trim();
  const costType = costTypeRaw || null;

  const params = [
    from.toISOString(),
    toExclusive.toISOString(),
    bucket,
    offsetMin,
  ];
  let idx = params.length;

  // optional filters
  const filters = [];
  const changedFilter = changedBy || changedByUPN;
  if (changedFilter) {
    idx += 1;
    params.push(`%${changedFilter}%`);
    filters.push(
      `AND (COALESCE(d.task_changed_by,'') ILIKE $${idx} OR COALESCE(d.task_changed_by_upn,'') ILIKE $${idx})`,
    );
  }
  const assignedFilter = assignedTo || assignedToUPN;
  if (assignedFilter) {
    idx += 1;
    params.push(`%${assignedFilter}%`);
    filters.push(
      `AND (COALESCE(d.task_assigned_to,'') ILIKE $${idx} OR COALESCE(d.task_assigned_upn,'') ILIKE $${idx})`,
    );
  }
  if (costType) {
    idx += 1;
    params.push(costType);
    filters.push(`AND LOWER(d.cost_type) = LOWER($${idx})`);
  }

  const sql = buildSummarySql(filters);

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
  const tz = getReportTimeZone();
  const rng = rangeFromToUtc(fromStr, toStr, offsetMin, tz);
  if (!rng) {
    return res.status(400).json({ ok: false, error: 'invalid from/to date' });
  }
  const from = rng.fromUtc;
  const toExclusive = rng.toExclusiveUtc;

  const assignedTo = (req.query.assignedTo || '').toString().trim();
  const assignedToUPN = (req.query.assignedToUPN || '').toString().trim();
  const costTypeRaw = (req.query.costType || req.query.accountCode || '')
    .toString()
    .trim();
  const costType = costTypeRaw || null;

  const limit = Math.min(5000, Math.max(1, Number(req.query.limit || 500)));
  const offset = Math.max(0, Number(req.query.offset || 0));

  const params = [from.toISOString(), toExclusive.toISOString()];
  let idx = params.length;

  const filters = [];
  const assignedFilter = assignedTo || assignedToUPN;
  if (assignedFilter) {
    idx += 1;
    params.push(`%${assignedFilter}%`);
    filters.push(
      `AND (COALESCE(d.task_assigned_to,'') ILIKE $${idx} OR COALESCE(d.task_assigned_upn,'') ILIKE $${idx})`,
    );
  }
  if (costType) {
    idx += 1;
    params.push(costType);
    filters.push(`AND LOWER(d.cost_type) = LOWER($${idx})`);
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
        s.task_changed_by,
        s.task_changed_by_upn,
        COALESCE(s.task_actual_hours, 0) AS h,
        s.ticket_id,
        s.feature_id,
        s.cost_type
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
        NULL::text AS task_changed_by,
        NULL::text AS task_changed_by_upn,
        p.h,
        NULL::int  AS ticket_id,
        NULL::int  AS feature_id,
        NULL::text AS cost_type,
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
        i.task_changed_by,
        i.task_changed_by_upn,
        i.h,
        i.ticket_id,
        i.feature_id,
        i.cost_type,
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
        task_changed_by,
        task_changed_by_upn,
        COALESCE(prev_h, 0) AS prev_hours,
        h AS actual_hours,
        (h - COALESCE(prev_h, 0)) AS delta_hours,
        ticket_id,
        feature_id,
        cost_type
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
      d.task_changed_by,
      d.task_changed_by_upn,
      d.prev_hours,
      d.actual_hours,
      d.delta_hours,
      d.ticket_id,
      l.ticket_type,
      l.ticket_title,
      l.feature_id,
      l.feature_title,
      d.cost_type AS cost_type,
      COUNT(*) OVER() AS total_count
    FROM d
    LEFT JOIN public.tfs_task_hours_latest l ON l.task_id = d.task_id
    WHERE d.actual_hours > 0
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
  const bucket = (req.query.bucket || 'day').toString().trim();
  const from = (req.query.from || '').toString().trim();
  const to = (req.query.to || '').toString().trim();
  const assignedToUPN = (req.query.assignedToUPN || '').toString().trim();
  const costType = (req.query.costType || req.query.accountCode || '')
    .toString()
    .trim();

  const bucketAllowed = new Set(['day', 'week', 'month']);
  const unit = bucketAllowed.has(bucket.toLowerCase())
    ? bucket.toLowerCase()
    : 'day';

  if (!from || !to) return res.status(400).send('from/to required');

  const offsetMin = getReportOffsetMinutes(); // PST = -480
  const tz = getReportTimeZone();
  const rng = rangeFromToUtc(from, to, offsetMin, tz);
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

  if (costType) {
    idx += 1;
    params.push(costType);
    filters.push(`AND LOWER(d.cost_type) = LOWER($${idx})`);
  }

  const sql = buildSummarySql(filters);

  try {
    const r = await pool.query(sql, params);

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader(
      'Content-Disposition',
      'attachment; filename=tfs_hours_summary.csv',
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

// ---------- Users ----------
app.get('/api/users', async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT DISTINCT
        task_assigned_to  AS name,
        task_assigned_upn AS upn
      FROM public.tfs_task_hours_latest
      WHERE task_assigned_to IS NOT NULL
        AND task_assigned_to <> ''
      ORDER BY task_assigned_to ASC
    `);
    res.json({ ok: true, users: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- Cost types ----------
app.get('/api/cost-types', async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT DISTINCT cost_type
      FROM public.tfs_task_hours_latest
      WHERE cost_type IS NOT NULL
        AND cost_type <> ''
      ORDER BY cost_type ASC
    `);
    res.json({ ok: true, costTypes: r.rows.map((x) => x.cost_type) });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- PTO / Team-Off / Holidays helpers ----------
function validateDateStr(v) {
  const p = parseYmd(String(v || '').trim());
  if (!p) return null;
  return `${String(p.y).padStart(4, '0')}-${String(p.mo).padStart(2, '0')}-${String(p.d).padStart(2, '0')}`;
}

function validateHours(v) {
  const n = normNum(v);
  return n !== null && n > 0 && n <= 24 ? n : null;
}

// ---------- Public Holidays ----------
app.get('/api/holidays', async (req, res) => {
  const fromStr = validateDateStr(req.query.from);
  const toStr = validateDateStr(req.query.to);
  const params = [];
  const where = [];
  if (fromStr) {
    params.push(fromStr);
    where.push(`holiday_date >= $${params.length}::date`);
  }
  if (toStr) {
    params.push(toStr);
    where.push(`holiday_date <= $${params.length}::date`);
  }
  try {
    const r = await pool.query(
      `SELECT id, holiday_date::text, name, hours, created_at
       FROM public.public_holidays
       ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
       ORDER BY holiday_date ASC`,
      params,
    );
    res.json({ ok: true, rows: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.post('/api/holidays', async (req, res) => {
  const {
    holiday_date: hdRaw,
    name: nameRaw,
    hours: hoursRaw,
  } = req.body || {};
  const holiday_date = validateDateStr(hdRaw);
  const name = normText(nameRaw);
  const hours = validateHours(hoursRaw ?? 8);
  if (!holiday_date)
    return res
      .status(400)
      .json({ ok: false, error: 'holiday_date is required (YYYY-MM-DD)' });
  if (!name)
    return res.status(400).json({ ok: false, error: 'name is required' });
  if (hours === null)
    return res
      .status(400)
      .json({ ok: false, error: 'hours must be between 0.5 and 24' });
  try {
    const r = await pool.query(
      `INSERT INTO public.public_holidays (holiday_date, name, hours)
       VALUES ($1::date, $2, $3)
       ON CONFLICT (holiday_date) DO UPDATE SET name = EXCLUDED.name, hours = EXCLUDED.hours
       RETURNING id, holiday_date::text, name, hours`,
      [holiday_date, name, hours],
    );
    res.status(201).json({ ok: true, row: r.rows[0] });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.delete('/api/holidays/:id', async (req, res) => {
  const id = normInt(req.params.id);
  if (!id || id < 1)
    return res.status(400).json({ ok: false, error: 'invalid id' });
  try {
    const r = await pool.query(
      'DELETE FROM public.public_holidays WHERE id = $1 RETURNING id',
      [id],
    );
    if (!r.rows.length)
      return res.status(404).json({ ok: false, error: 'not found' });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- Team Off ----------
app.get('/api/team-off', async (req, res) => {
  const fromStr = validateDateStr(req.query.from);
  const toStr = validateDateStr(req.query.to);
  const params = [];
  const where = [];
  if (fromStr) {
    params.push(fromStr);
    where.push(`entry_date >= $${params.length}::date`);
  }
  if (toStr) {
    params.push(toStr);
    where.push(`entry_date <= $${params.length}::date`);
  }
  try {
    const r = await pool.query(
      `SELECT id, entry_date::text, hours, notes, created_at
       FROM public.team_off_entries
       ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
       ORDER BY entry_date ASC`,
      params,
    );
    res.json({ ok: true, rows: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.post('/api/team-off', async (req, res) => {
  const {
    entry_date: edRaw,
    hours: hoursRaw,
    notes: notesRaw,
  } = req.body || {};
  const entry_date = validateDateStr(edRaw);
  const hours = validateHours(hoursRaw ?? 8);
  const notes = normText(notesRaw);
  if (!entry_date)
    return res
      .status(400)
      .json({ ok: false, error: 'entry_date is required (YYYY-MM-DD)' });
  if (hours === null)
    return res
      .status(400)
      .json({ ok: false, error: 'hours must be between 0.5 and 24' });
  try {
    const r = await pool.query(
      `INSERT INTO public.team_off_entries (entry_date, hours, notes)
       VALUES ($1::date, $2, $3)
       ON CONFLICT (entry_date) DO UPDATE SET hours = EXCLUDED.hours, notes = EXCLUDED.notes
       RETURNING id, entry_date::text, hours, notes`,
      [entry_date, hours, notes],
    );
    res.status(201).json({ ok: true, row: r.rows[0] });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.delete('/api/team-off/:id', async (req, res) => {
  const id = normInt(req.params.id);
  if (!id || id < 1)
    return res.status(400).json({ ok: false, error: 'invalid id' });
  try {
    const r = await pool.query(
      'DELETE FROM public.team_off_entries WHERE id = $1 RETURNING id',
      [id],
    );
    if (!r.rows.length)
      return res.status(404).json({ ok: false, error: 'not found' });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- Individual PTO ----------
app.get('/api/pto', async (req, res) => {
  const fromStr = validateDateStr(req.query.from);
  const toStr = validateDateStr(req.query.to);
  const userFilter = normText(req.query.userUpn || req.query.assignedTo);
  const params = [];
  const where = [];
  if (fromStr) {
    params.push(fromStr);
    where.push(`entry_date >= $${params.length}::date`);
  }
  if (toStr) {
    params.push(toStr);
    where.push(`entry_date <= $${params.length}::date`);
  }
  if (userFilter) {
    params.push(`%${userFilter}%`);
    where.push(
      `(COALESCE(user_upn,'') ILIKE $${params.length} OR COALESCE(user_name,'') ILIKE $${params.length})`,
    );
  }
  try {
    const r = await pool.query(
      `SELECT id, user_upn, user_name, entry_date::text, hours, notes, created_at
       FROM public.pto_entries
       ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
       ORDER BY entry_date ASC, user_name ASC`,
      params,
    );
    res.json({ ok: true, rows: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.post('/api/pto', async (req, res) => {
  const {
    user_name: unRaw,
    user_upn: uupnRaw,
    entry_date: edRaw,
    hours: hoursRaw,
    notes: notesRaw,
  } = req.body || {};
  const user_name = normText(unRaw);
  const user_upn = normText(uupnRaw) || user_name;
  const entry_date = validateDateStr(edRaw);
  const hours = validateHours(hoursRaw ?? 8);
  const notes = normText(notesRaw);
  if (!user_name && !user_upn)
    return res
      .status(400)
      .json({ ok: false, error: 'user_name or user_upn is required' });
  if (!entry_date)
    return res
      .status(400)
      .json({ ok: false, error: 'entry_date is required (YYYY-MM-DD)' });
  if (hours === null)
    return res
      .status(400)
      .json({ ok: false, error: 'hours must be between 0.5 and 24' });
  try {
    const r = await pool.query(
      `INSERT INTO public.pto_entries (user_upn, user_name, entry_date, hours, notes)
       VALUES ($1, $2, $3::date, $4, $5)
       RETURNING id, user_upn, user_name, entry_date::text, hours, notes`,
      [user_upn, user_name, entry_date, hours, notes],
    );
    res.status(201).json({ ok: true, row: r.rows[0] });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.delete('/api/pto/:id', async (req, res) => {
  const id = normInt(req.params.id);
  if (!id || id < 1)
    return res.status(400).json({ ok: false, error: 'invalid id' });
  try {
    const r = await pool.query(
      'DELETE FROM public.pto_entries WHERE id = $1 RETURNING id',
      [id],
    );
    if (!r.rows.length)
      return res.status(404).json({ ok: false, error: 'not found' });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- Hours metrics ----------
app.get('/api/hours/metrics', async (req, res) => {
  const fromStr = validateDateStr(req.query.from);
  const toStr = validateDateStr(req.query.to);
  if (!fromStr || !toStr)
    return res
      .status(400)
      .json({ ok: false, error: 'from and to required (YYYY-MM-DD)' });
  const assignedTo = normText(req.query.assignedTo);
  try {
    // Mon–Fri weekday count × 8
    const wdR = await pool.query(
      `SELECT (COUNT(*) * 8)::float AS weekday_hours
       FROM generate_series($1::date, $2::date, '1 day'::interval) AS d
       WHERE EXTRACT(DOW FROM d) BETWEEN 1 AND 5`,
      [fromStr, toStr],
    );
    const weekdayHours = Number(wdR.rows[0]?.weekday_hours ?? 0);

    // Team off + public holiday deduction
    const offR = await pool.query(
      `SELECT
         COALESCE((SELECT SUM(hours) FROM public.team_off_entries WHERE entry_date BETWEEN $1::date AND $2::date), 0) +
         COALESCE((SELECT SUM(hours) FROM public.public_holidays  WHERE holiday_date BETWEEN $1::date AND $2::date), 0)
         AS team_off_hours`,
      [fromStr, toStr],
    );
    const teamOffHours = Number(offR.rows[0]?.team_off_hours ?? 0);
    const requiredHours = Math.max(0, weekdayHours - teamOffHours);

    // Individual PTO (optionally filtered to one user by name/UPN)
    const ptoParams = [fromStr, toStr];
    let ptoWhere = 'entry_date BETWEEN $1::date AND $2::date';
    if (assignedTo) {
      ptoParams.push(`%${assignedTo}%`);
      ptoWhere += ` AND (COALESCE(user_name,'') ILIKE $3 OR COALESCE(user_upn,'') ILIKE $3)`;
    }
    const ptoR = await pool.query(
      `SELECT COALESCE(SUM(hours), 0) AS pto_hours FROM public.pto_entries WHERE ${ptoWhere}`,
      ptoParams,
    );
    const individualPtoHours = Number(ptoR.rows[0]?.pto_hours ?? 0);

    res.json({
      ok: true,
      weekdayHours,
      teamOffHours,
      requiredHours,
      individualPtoHours,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- Shared helper: compute per-user hours for a period ----------
async function computeUserHours(fromStr, toStr, fromUtc, toExclusiveUtc) {
  const wdR = await pool.query(
    `SELECT (COUNT(*) * 8)::float AS weekday_hours
     FROM generate_series($1::date, $2::date, '1 day'::interval) AS d
     WHERE EXTRACT(DOW FROM d) BETWEEN 1 AND 5`,
    [fromStr, toStr],
  );
  const weekdayHours = Number(wdR.rows[0]?.weekday_hours ?? 0);

  const offR = await pool.query(
    `SELECT
       COALESCE((SELECT SUM(hours) FROM public.team_off_entries  WHERE entry_date   BETWEEN $1::date AND $2::date), 0) +
       COALESCE((SELECT SUM(hours) FROM public.public_holidays   WHERE holiday_date BETWEEN $1::date AND $2::date), 0)
       AS shared_off_hours`,
    [fromStr, toStr],
  );
  const sharedOffHours = Number(offR.rows[0]?.shared_off_hours ?? 0);
  const requiredHours = Math.max(0, weekdayHours - sharedOffHours);

  const ptoR = await pool.query(
    `SELECT LOWER(TRIM(COALESCE(user_name, user_upn))) AS name_key,
            COALESCE(SUM(hours), 0) AS pto_hours
     FROM public.pto_entries
     WHERE entry_date BETWEEN $1::date AND $2::date
     GROUP BY 1`,
    [fromStr, toStr],
  );
  const ptoByName = new Map(
    ptoR.rows.map((r) => [r.name_key, Number(r.pto_hours)]),
  );

  // Sum actual_hours for each distinct (task, changed_date) snapshot within the
  // range — identical to what /api/hours/entries returns and what the stat cards
  // sum via loadMetrics(), so the preview table values match the UI cards exactly.
  const loggedR = await pool.query(
    `SELECT
       LOWER(TRIM(task_assigned_to)) AS name_key,
       SUM(h)                        AS logged_hours
     FROM (
       SELECT DISTINCT ON (s.task_id, COALESCE(s.task_changed_date, s.snapshot_at))
         s.task_assigned_to,
         COALESCE(s.task_actual_hours, 0) AS h
       FROM public.tfs_task_hours_snapshots s
       WHERE COALESCE(s.task_changed_date, s.snapshot_at) >= $1::timestamptz
         AND COALESCE(s.task_changed_date, s.snapshot_at) < $2::timestamptz
         AND s.task_assigned_to IS NOT NULL
       ORDER BY s.task_id,
                COALESCE(s.task_changed_date, s.snapshot_at),
                s.snapshot_at DESC,
                s.run_id DESC
     ) sub
     WHERE h > 0
     GROUP BY 1`,
    [fromUtc.toISOString(), toExclusiveUtc.toISOString()],
  );
  const loggedByName = new Map(
    loggedR.rows.map((r) => [r.name_key, Number(r.logged_hours)]),
  );

  const usersR = await pool.query(
    `SELECT email, name FROM public.users WHERE email LIKE '%@%' ORDER BY name ASC`,
  );

  return {
    weekdayHours,
    sharedOffHours,
    requiredHours,
    ptoByName,
    loggedByName,
    users: usersR.rows,
  };
}

// ---------- Hours preview (no emails sent) ----------
app.get('/api/notifications/hours-preview', async (req, res) => {
  const fromStr = validateDateStr(req.query.from);
  const toStr = validateDateStr(req.query.to);
  const threshold = normNum(req.query.threshold) ?? 16;

  if (!fromStr || !toStr)
    return res
      .status(400)
      .json({ ok: false, error: 'from and to required (YYYY-MM-DD)' });
  if (!Number.isFinite(threshold) || threshold < 0)
    return res
      .status(400)
      .json({ ok: false, error: 'threshold must be a positive number' });

  const offsetMin = getReportOffsetMinutes();
  const tz = getReportTimeZone();
  const rng = rangeFromToUtc(fromStr, toStr, offsetMin, tz);
  if (!rng)
    return res.status(400).json({ ok: false, error: 'invalid from/to date' });

  try {
    const {
      weekdayHours,
      sharedOffHours,
      requiredHours,
      ptoByName,
      loggedByName,
      users,
    } = await computeUserHours(fromStr, toStr, rng.fromUtc, rng.toExclusiveUtc);

    const rows = users.map((user) => {
      const nameKey = user.name ? user.name.trim().toLowerCase() : '';
      const ptoHours = ptoByName.get(nameKey) ?? 0;
      const loggedHours = loggedByName.get(nameKey) ?? 0;
      const missing = requiredHours - ptoHours - loggedHours;
      return {
        name: user.name || user.email,
        email: user.email,
        weekdayHours,
        sharedOffHours,
        requiredHours,
        ptoHours,
        loggedHours,
        missing,
        overThreshold: missing > threshold,
      };
    });

    rows.sort((a, b) => b.missing - a.missing);
    res.json({ ok: true, weekdayHours, sharedOffHours, requiredHours, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- Missing hours notifications ----------
app.post('/api/notifications/missing-hours', async (req, res) => {
  if (!BREVO_SMTP_USER || !BREVO_SMTP_KEY) {
    return res.status(503).json({
      ok: false,
      error:
        'SMTP not configured on server (BREVO_SMTP_USER / BREVO_SMTP_KEY).',
    });
  }
  if (!NOTIFY_FROM_EMAIL) {
    return res.status(503).json({
      ok: false,
      error: 'NOTIFY_FROM_EMAIL not configured on server.',
    });
  }

  const {
    from: fromRaw,
    to: toRaw,
    threshold: thresholdRaw,
    managerEmail: managerEmailRaw,
  } = req.body || {};

  const fromStr = validateDateStr(fromRaw);
  const toStr = validateDateStr(toRaw);
  const threshold = normNum(thresholdRaw) ?? 16;
  const managerEmail = normText(managerEmailRaw) || NOTIFY_MANAGER_EMAIL;

  if (!fromStr || !toStr)
    return res
      .status(400)
      .json({ ok: false, error: 'from and to are required (YYYY-MM-DD)' });
  if (!Number.isFinite(threshold) || threshold < 0)
    return res
      .status(400)
      .json({ ok: false, error: 'threshold must be a positive number' });

  const offsetMin = getReportOffsetMinutes();
  const tz = getReportTimeZone();
  const rng = rangeFromToUtc(fromStr, toStr, offsetMin, tz);
  if (!rng)
    return res.status(400).json({ ok: false, error: 'invalid from/to date' });

  const { fromUtc, toExclusiveUtc } = rng;

  try {
    const {
      weekdayHours,
      sharedOffHours,
      requiredHours,
      ptoByName,
      loggedByName,
      users,
    } = await computeUserHours(fromStr, toStr, rng.fromUtc, rng.toExclusiveUtc);

    // Identify offenders
    const offenders = [];
    for (const user of users) {
      const nameKey = user.name ? user.name.trim().toLowerCase() : '';
      const ptoHours = ptoByName.get(nameKey) ?? 0;
      const loggedHours = loggedByName.get(nameKey) ?? 0;
      const missing = requiredHours - ptoHours - loggedHours;
      if (missing > threshold) {
        offenders.push({
          email: user.email,
          name: user.name || user.email,
          weekdayHours,
          sharedOffHours,
          requiredHours,
          ptoHours,
          loggedHours,
          missing,
        });
      }
    }

    if (offenders.length === 0) {
      return res.json({
        ok: true,
        sent: 0,
        offenders: 0,
        message: `No users have missing hours above ${threshold}h for this period.`,
      });
    }

    // 7 — Send individual emails
    const transporter = createMailTransporter();
    const period = `${fromStr} to ${toStr}`;
    let sent = 0;
    const errors = [];

    for (const u of offenders) {
      const html = `
        <div style="font-family:sans-serif;font-size:14px;color:#1f2b2c;max-width:520px;">
          <p>Hi <strong>${escapeEmailHtml(u.name)}</strong>,</p>
          <p>This is a reminder that you have
             <strong style="color:#c8742b;">${fmtH(u.missing)} missing hours</strong>
             for the period <strong>${escapeEmailHtml(period)}</strong>.</p>
          <table border="1" cellpadding="8" cellspacing="0"
                 style="border-collapse:collapse;font-size:13px;width:100%;margin:12px 0;">
            <thead>
              <tr style="background:#f0ebe0;">
                <th>Workday Hrs</th><th>Team Off / Holiday</th>
                <th>Your PTO</th><th>Logged</th><th>Missing</th>
              </tr>
            </thead>
            <tbody>
              <tr style="text-align:center;">
                <td>${fmtH(u.weekdayHours)}</td>
                <td>${fmtH(u.sharedOffHours)}</td>
                <td>${fmtH(u.ptoHours)}</td>
                <td>${fmtH(u.loggedHours)}</td>
                <td style="color:#c8742b;font-weight:bold;">${fmtH(u.missing)}</td>
              </tr>
            </tbody>
          </table>
          <p>Please log your hours in TFS at your earliest convenience.</p>
          <p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report</p>
        </div>`;
      try {
        await transporter.sendMail({
          from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
          to: u.email,
          ...(managerEmail ? { cc: managerEmail } : {}),
          subject: `Missing Hours Alert \u2013 ${period}`,
          html,
        });
        sent++;
      } catch (e) {
        errors.push({ email: u.email, error: String(e?.message || e) });
      }
    }

    // 8 — Manager digest (separate summary email)
    if (managerEmail) {
      const tableRows = offenders
        .map(
          (u) => `
        <tr>
          <td>${escapeEmailHtml(u.name)}</td>
          <td>${escapeEmailHtml(u.email)}</td>
          <td style="text-align:center;">${fmtH(u.requiredHours)}</td>
          <td style="text-align:center;">${fmtH(u.ptoHours)}</td>
          <td style="text-align:center;">${fmtH(u.loggedHours)}</td>
          <td style="text-align:center;color:#c8742b;font-weight:bold;">${fmtH(u.missing)}</td>
        </tr>`,
        )
        .join('');
      const digestHtml = `
        <div style="font-family:sans-serif;font-size:14px;color:#1f2b2c;max-width:700px;">
          <p><strong>${offenders.length} user(s)</strong> have missing hours
             &gt; ${fmtH(threshold)}h for <strong>${escapeEmailHtml(period)}</strong>:</p>
          <table border="1" cellpadding="8" cellspacing="0"
                 style="border-collapse:collapse;font-size:13px;width:100%;margin:12px 0;">
            <thead>
              <tr style="background:#f0ebe0;">
                <th>Name</th><th>Email</th><th>Required</th>
                <th>PTO</th><th>Logged</th><th>Missing</th>
              </tr>
            </thead>
            <tbody>${tableRows}</tbody>
          </table>
          <p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report</p>
        </div>`;
      try {
        await transporter.sendMail({
          from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
          to: managerEmail,
          subject: `Missing Hours Digest \u2013 ${period} \u2013 ${offenders.length} user(s)`,
          html: digestHtml,
        });
      } catch (_) {
        /* digest failure is non-critical */
      }
    }

    res.json({
      ok: true,
      sent,
      offenders: offenders.length,
      ...(errors.length ? { errors } : {}),
    });
  } catch (e) {
    console.error('NOTIFY ERROR:', e);
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- Static UI ----------
app.use('/', express.static(path.join(__dirname, 'public')));

app.listen(PORT, () => {
  console.log(`tfs-hours-dashboard listening on :${PORT}`);
});
