const express = require('express');
const path = require('path');
const fs = require('fs');
const { Pool } = require('pg');
const nodemailer = require('nodemailer');
const crypto = require('crypto');
const PDFDocument = require('pdfkit');

const app = express();
app.use(express.json({ limit: '10mb' }));

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;
const SYNC_API_KEY = process.env.SYNC_API_KEY || '';
const PTO_REMINDER_API_KEY = process.env.PTO_REMINDER_API_KEY || '';
const MISSING_HOURS_NOTIFY_API_KEY =
  process.env.MISSING_HOURS_NOTIFY_API_KEY || '';
const MISSING_HOURS_THRESHOLD = Math.max(
  0,
  Number(process.env.MISSING_HOURS_THRESHOLD || '16') || 16,
);
const TFS_WORKITEM_URL_TEMPLATE = process.env.TFS_WORKITEM_URL_TEMPLATE || '';
const REPORT_TZ_OFFSET_MINUTES = Number(
  process.env.REPORT_TZ_OFFSET_MINUTES || '0',
); // PST = -480
const REPORT_TZ_LABEL = process.env.REPORT_TZ_LABEL || 'UTC';
const REPORT_TZ_IANA = (process.env.REPORT_TZ_IANA || '').trim();

const BREVO_API_KEY = process.env.BREVO_API_KEY || '';
const NOTIFY_FROM_EMAIL = process.env.NOTIFY_FROM_EMAIL || '';
const NOTIFY_FROM_NAME = process.env.NOTIFY_FROM_NAME || 'TFS Hours Report';
const NOTIFY_MANAGER_EMAIL = process.env.NOTIFY_MANAGER_EMAIL || '';
// Optional fixed CC list for PM final approval emails (HR, managers, etc.)
// Comma-separated list of email addresses, e.g. "hr@company.com,ceo@company.com"
const EXTRA_CC_EMAILS = (process.env.EXTRA_CC_EMAILS || '')
  .split(',')
  .map((e) => e.trim())
  .filter((e) => e.includes('@'));
const SPECIAL_PTO_WORKFLOW_TEAM = (
  process.env.SPECIAL_PTO_WORKFLOW_TEAM || ''
).trim();
const SPECIAL_PTO_WORKFLOW_TEAM_KEY = SPECIAL_PTO_WORKFLOW_TEAM.toLowerCase();
const SPECIAL_PTO_EXTERNAL_APPROVER_EMAILS = (
  process.env.SPECIAL_PTO_EXTERNAL_APPROVER_EMAILS || ''
)
  .split(',')
  .map((e) => e.trim())
  .filter((e) => e.includes('@'));
const EXTERNAL_PTO_TOKEN_TTL_DAYS = Math.max(
  1,
  Number(process.env.EXTERNAL_PTO_TOKEN_TTL_DAYS || '14') || 14,
);
const PUBLIC_BASE_URL = (
  process.env.PUBLIC_BASE_URL ||
  process.env.APP_BASE_URL ||
  process.env.RENDER_EXTERNAL_URL ||
  ''
).trim();
// Set PTO_APPROVAL_ENABLED=false in .env to bypass the approval workflow (all PTOs auto-approved)
const PTO_APPROVAL_ENABLED =
  String(process.env.PTO_APPROVAL_ENABLED).trim().toLowerCase() !== 'false';

function parseEmailList(str) {
  if (!str) return [];
  return String(str)
    .split(',')
    .map((s) => {
      const m = s.trim().match(/^"?([^"<]*)"?\s*<([^>]+)>$/);
      if (m) {
        const name = m[1].trim();
        return name ? { name, email: m[2].trim() } : { email: m[2].trim() };
      }
      const e = s.trim();
      return e ? { email: e } : null;
    })
    .filter(Boolean);
}

function createMailTransporter() {
  return {
    sendMail: async (opts) => {
      const { from, to, cc, subject, html, text, headers, attachments } = opts;
      const senderList = parseEmailList(from);
      const sender = senderList[0] || {
        email: NOTIFY_FROM_EMAIL,
        name: NOTIFY_FROM_NAME,
      };
      const body = {
        sender,
        to: parseEmailList(to),
        subject,
        ...(html ? { htmlContent: html } : {}),
        ...(text ? { textContent: text } : {}),
        ...(cc ? { cc: parseEmailList(cc) } : {}),
        ...(headers && Object.keys(headers).length ? { headers } : {}),
        ...(attachments?.length
          ? {
              attachment: attachments.map((a) => ({
                name: a.filename,
                content: Buffer.isBuffer(a.content)
                  ? a.content.toString('base64')
                  : a.content,
              })),
            }
          : {}),
      };
      const resp = await fetch('https://api.brevo.com/v3/smtp/email', {
        method: 'POST',
        headers: {
          'api-key': BREVO_API_KEY,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      });
      if (!resp.ok) {
        const errText = await resp.text().catch(() => resp.statusText);
        throw new Error(`Brevo API error ${resp.status}: ${errText}`);
      }
      return resp.json();
    },
  };
}

function requireConfiguredApiKey(req, res, configuredKey, configName) {
  if (!configuredKey) {
    res.status(503).json({
      ok: false,
      error: `${configName} not configured on server.`,
    });
    return false;
  }
  const key = req.header('x-api-key');
  if (!key || key !== configuredKey) {
    res.status(401).json({ ok: false, error: 'unauthorized' });
    return false;
  }
  return true;
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

function fmtWorkingDaysFromHours(hours) {
  const days = Number(hours) / 8;
  return Number.isFinite(days) ? days.toFixed(1) : '0.0';
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
    smtpConfigured: !!(BREVO_API_KEY && NOTIFY_FROM_EMAIL),
    specialPtoWorkflowTeam: SPECIAL_PTO_WORKFLOW_TEAM || null,
  });
});

// ---------- Helpers ----------
function requireApiKey(req, res) {
  if (!SYNC_API_KEY) return true; // leaving empty disables auth (not recommended)
  return requireConfiguredApiKey(req, res, SYNC_API_KEY, 'SYNC_API_KEY');
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

// ---------- Password helpers ----------
function verifyLegacyPassword(pw, encoded) {
  const parts = String(encoded || '').split(':');
  if (parts.length !== 2) return false;
  try {
    const saltBase64 = parts[0];
    const salt = Buffer.from(saltBase64, 'base64');
    const expected = Buffer.from(parts[1], 'base64');
    const pwBuf = Buffer.from(pw, 'utf8');

    // Try all common SHA256+salt orderings used by legacy Node auth implementations
    const candidates = [
      // variant 1: SHA256(salt_bytes + pw_bytes)
      crypto.createHash('sha256').update(salt).update(pwBuf).digest(),
      // variant 2: SHA256(pw_bytes + salt_bytes)
      crypto.createHash('sha256').update(pwBuf).update(salt).digest(),
      // variant 3: SHA256(saltBase64String + pw)
      crypto
        .createHash('sha256')
        .update(saltBase64 + pw)
        .digest(),
      // variant 4: SHA256(pw + saltBase64String)
      crypto
        .createHash('sha256')
        .update(pw + saltBase64)
        .digest(),
      // variant 5: HMAC-SHA256(pw, salt_bytes)
      crypto.createHmac('sha256', salt).update(pwBuf).digest(),
      // variant 6: HMAC-SHA256(pw, saltBase64String)
      crypto.createHmac('sha256', saltBase64).update(pwBuf).digest(),
    ];

    return candidates.some(
      (h) =>
        h.length === expected.length && crypto.timingSafeEqual(h, expected),
    );
  } catch {
    return false;
  }
}

async function hashPassword(pw) {
  const salt = crypto.randomBytes(16);
  const hash = await new Promise((resolve, reject) => {
    crypto.scrypt(Buffer.from(pw, 'utf8'), salt, 32, (err, key) =>
      err ? reject(err) : resolve(key),
    );
  });
  // Shared format with tfs-daily-updates: saltB64:hashB64 (no prefix)
  return `${salt.toString('base64')}:${hash.toString('base64')}`;
}

// Returns the matched format string, or null on failure:
//   'scrypt-prefixed' — old tfs-hours native format (scrypt:saltB64:hashB64)
//   'scrypt-shared'   — shared format with tfs-daily-updates (saltB64:hashB64, scrypt)
//   'legacy'          — old SHA256-based format
//   null              — no match / wrong password
async function verifyPassword(pw, encoded) {
  const s = String(encoded || '');
  if (s.startsWith('scrypt:')) {
    // Old tfs-hours native format: scrypt:saltB64:hashB64
    const parts = s.split(':');
    if (parts.length !== 3) return null;
    try {
      const salt = Buffer.from(parts[1], 'base64');
      const expected = Buffer.from(parts[2], 'base64');
      const derived = await new Promise((resolve, reject) => {
        crypto.scrypt(Buffer.from(pw, 'utf8'), salt, 32, (err, key) =>
          err ? reject(err) : resolve(key),
        );
      });
      return crypto.timingSafeEqual(expected, derived)
        ? 'scrypt-prefixed'
        : null;
    } catch {
      return null;
    }
  }
  // Shared format with tfs-daily-updates: saltB64:hashB64 (scrypt, no prefix)
  const parts = s.split(':');
  if (parts.length === 2) {
    try {
      const salt = Buffer.from(parts[0], 'base64');
      const expected = Buffer.from(parts[1], 'base64');
      const calc = crypto.scryptSync(pw, salt, 32);
      if (crypto.timingSafeEqual(calc, expected)) return 'scrypt-shared';
    } catch {
      // fall through to legacy
    }
  }
  return verifyLegacyPassword(pw, s) ? 'legacy' : null;
}

// ---------- Auth middleware ----------
async function requireAuth(req, res, next) {
  const authHeader = req.header('Authorization') || '';
  const tok = authHeader.startsWith('Bearer ')
    ? authHeader.slice(7).trim()
    : null;
  if (!tok) return res.status(401).json({ ok: false, error: 'unauthorized' });
  try {
    const r = await pool.query(
      `SELECT s.email, s.user_id, u.role, u.name, u.team
       FROM sessions s
       JOIN public.users u ON u.email = s.email
       WHERE s.token = $1`,
      [tok],
    );
    if (!r.rows.length)
      return res.status(401).json({ ok: false, error: 'unauthorized' });
    req.userEmail = r.rows[0].email;
    req.userId = r.rows[0].user_id;
    req.userRole = r.rows[0].role || 'dev';
    req.userName = r.rows[0].name;
    req.userTeam = r.rows[0].team || null;
    next();
  } catch (e) {
    console.error('requireAuth error:', e);
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
}

function requireManagerOrAbove(req, res, next) {
  if (req.userRole !== 'admin' && req.userRole !== 'pm')
    return res.status(403).json({ ok: false, error: 'forbidden' });
  next();
}

function requireLeadOrPm(req, res, next) {
  if (req.userRole !== 'lead' && req.userRole !== 'pm')
    return res.status(403).json({ ok: false, error: 'forbidden' });
  next();
}

const REPORT_BROAD_ROLES = new Set(['admin', 'pm']);
const REPORT_OWN_ONLY_ROLES = new Set(['lead', 'dev', 'qa', 'ts']);

function normIdentity(v) {
  return String(v ?? '')
    .trim()
    .toLowerCase();
}

function isSpecialPtoTeamName(team) {
  return (
    !!SPECIAL_PTO_WORKFLOW_TEAM_KEY &&
    normIdentity(team) === SPECIAL_PTO_WORKFLOW_TEAM_KEY
  );
}

function getReportingSelf(req) {
  const email = normText(req.userEmail);
  const name = normText(req.userName) || email;
  return { email, name };
}

function resolveReportingScope(req, assignedToRaw, assignedToUPNRaw) {
  const assignedTo = normText(assignedToRaw);
  const assignedToUPN = normText(assignedToUPNRaw);
  const self = getReportingSelf(req);

  if (!REPORT_OWN_ONLY_ROLES.has(req.userRole)) {
    return { ok: true, broad: REPORT_BROAD_ROLES.has(req.userRole), self };
  }

  const emailKey = normIdentity(self.email);
  const nameKey = normIdentity(self.name);
  const assignedToKey = normIdentity(assignedTo);
  const assignedToUPNKey = normIdentity(assignedToUPN);

  const assignedToMatches =
    !assignedToKey ||
    assignedToKey === emailKey ||
    (nameKey && assignedToKey === nameKey);
  const assignedToUPNMatches =
    !assignedToUPNKey || assignedToUPNKey === emailKey;

  if (!assignedToMatches || !assignedToUPNMatches) {
    return { ok: false, error: 'forbidden' };
  }

  return { ok: true, broad: false, self };
}

function buildOwnReportingAssigneeClause(params, upnExpr, nameExpr, self) {
  const emailIdx = params.push(self.email);
  if (self.name) {
    const nameIdx = params.push(self.name);
    return `(LOWER(COALESCE(${upnExpr}, '')) = LOWER($${emailIdx}) OR LOWER(COALESCE(${nameExpr}, '')) = LOWER($${nameIdx}))`;
  }
  return `LOWER(COALESCE(${upnExpr}, '')) = LOWER($${emailIdx})`;
}

/**
 * Returns list of approver emails based on filer's role and team.
 * Gracefully falls back to ALL leads/PMs when team is null.
 */
async function getApproverEmails(filerRole, filerTeam, filerEmail) {
  let q, params;
  const specialTeam = isSpecialPtoTeamName(filerTeam);
  if (filerRole === 'dev' || filerRole === 'qa') {
    if (filerTeam) {
      q = specialTeam
        ? `SELECT email FROM public.users WHERE role = 'lead' AND LOWER(COALESCE(team, '')) = LOWER($1)`
        : `SELECT email FROM public.users WHERE role = 'lead' AND team = $1`;
      params = [filerTeam];
    } else {
      q = `SELECT email FROM public.users WHERE role = 'lead'`;
      params = [];
    }
  } else if (filerRole === 'lead') {
    if (specialTeam) {
      q = `SELECT email FROM public.users WHERE role = 'pm' AND LOWER(COALESCE(team, '')) = LOWER($1)`;
      params = [filerTeam];
    } else {
      q = `SELECT email FROM public.users WHERE role = 'pm'`;
      params = [];
    }
  } else if (filerRole === 'pm') {
    if (specialTeam) {
      if (filerEmail) {
        q = `SELECT email FROM public.users
             WHERE role = 'pm'
               AND LOWER(COALESCE(team, '')) = LOWER($1)
               AND LOWER(email) != LOWER($2)`;
        params = [filerTeam, filerEmail];
      } else {
        q = `SELECT email FROM public.users
             WHERE role = 'pm'
               AND LOWER(COALESCE(team, '')) = LOWER($1)`;
        params = [filerTeam];
      }
    } else {
      if (filerEmail) {
        q = `SELECT email FROM public.users WHERE role = 'pm' AND LOWER(email) != LOWER($1)`;
        params = [filerEmail];
      } else {
        q = `SELECT email FROM public.users WHERE role = 'pm'`;
        params = [];
      }
    }
  } else if (filerRole === 'ts') {
    q = `SELECT email FROM public.users WHERE role = 'pm'`;
    params = [];
  } else {
    return [];
  }
  try {
    const r = await pool.query(q, params);
    return r.rows.map((row) => row.email);
  } catch {
    return [];
  }
}

async function getRoleEmails(roles, excludeEmail) {
  const roleList = Array.isArray(roles)
    ? roles.filter((role) => role && typeof role === 'string')
    : [];
  if (!roleList.length) return [];
  try {
    const params = [roleList];
    let where = `role = ANY($1::text[])`;
    if (excludeEmail) {
      params.push(excludeEmail);
      where += ` AND LOWER(email) != LOWER($2)`;
    }
    const r = await pool.query(
      `SELECT email FROM public.users WHERE ${where} ORDER BY email ASC`,
      params,
    );
    return r.rows.map((row) => row.email);
  } catch {
    return [];
  }
}

// ---------- Auth routes ----------
app.post('/api/auth/login', async (req, res) => {
  const email = normText(req.body?.email);
  const pw = String(req.body?.password || '');
  if (!email || !pw)
    return res
      .status(400)
      .json({ ok: false, error: 'email and password required' });
  try {
    const r = await pool.query(
      'SELECT email, pw, role, name, id FROM public.users WHERE LOWER(email) = LOWER($1)',
      [email],
    );
    if (!r.rows.length) {
      await new Promise((resolve) => setTimeout(resolve, 200));
      return res.status(401).json({ ok: false, error: 'invalid credentials' });
    }
    const user = r.rows[0];
    const format = await verifyPassword(pw, user.pw);
    if (!format)
      return res.status(401).json({ ok: false, error: 'invalid credentials' });
    // Migrate 'scrypt-prefixed' (old tfs-hours format) and 'legacy' (SHA256) to shared format.
    // Never re-hash 'scrypt-shared' — it's already the correct shared format.
    if (format === 'scrypt-prefixed' || format === 'legacy') {
      try {
        const newHash = await hashPassword(pw);
        await pool.query('UPDATE public.users SET pw = $1 WHERE email = $2', [
          newHash,
          user.email,
        ]);
      } catch {
        /* non-critical */
      }
    }
    const tok = crypto.randomBytes(32).toString('hex');
    await pool.query(
      'INSERT INTO sessions (token, email, user_id) VALUES ($1, $2, $3)',
      [tok, user.email, user.id],
    );
    res.json({
      ok: true,
      token: tok,
      email: user.email,
      name: user.name,
      role: user.role || 'dev',
    });
  } catch (e) {
    console.error('LOGIN ERROR:', e);
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.post('/api/auth/logout', async (req, res) => {
  const authHeader = req.header('Authorization') || '';
  const tok = authHeader.startsWith('Bearer ')
    ? authHeader.slice(7).trim()
    : null;
  if (tok) {
    try {
      await pool.query('DELETE FROM sessions WHERE token = $1', [tok]);
    } catch {
      /* ignore */
    }
  }
  res.json({ ok: true });
});

app.get('/api/auth/me', requireAuth, (req, res) => {
  res.json({
    ok: true,
    email: req.userEmail,
    name: req.userName,
    role: req.userRole,
    team: req.userTeam,
  });
});

// ---------- User team assignment (admin only) ----------
app.patch('/api/users/:email/team', requireAuth, async (req, res) => {
  if (req.userRole !== 'admin')
    return res.status(403).json({ ok: false, error: 'forbidden' });
  const targetEmail = normText(req.params.email);
  const team = normText(req.body?.team) || null;
  if (!targetEmail)
    return res.status(400).json({ ok: false, error: 'email required' });
  try {
    const r = await pool.query(
      `UPDATE public.users SET team = $1 WHERE LOWER(email) = LOWER($2) RETURNING email, team`,
      [team, targetEmail],
    );
    if (!r.rows.length)
      return res.status(404).json({ ok: false, error: 'user not found' });
    res.json({ ok: true, email: r.rows[0].email, team: r.rows[0].team });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

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
    ON CONFLICT (task_id, task_changed_date) DO UPDATE SET
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

app.get('/api/hours/latest', requireAuth, async (req, res) => {
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
  const scope = resolveReportingScope(req, assignedTo, assignedToUPN);
  if (!scope.ok) return res.status(403).json({ ok: false, error: scope.error });

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
  if (!scope.broad) {
    where.push(
      buildOwnReportingAssigneeClause(
        params,
        'task_assigned_upn',
        'task_assigned_to',
        scope.self,
      ),
    );
  } else if (assignedFilter) {
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

app.get('/api/hours/meta', requireAuth, async (req, res) => {
  try {
    res.json({ ok: true, lastSyncAt: await fetchLastSyncAt() });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- Hours summary (delta-based; supports negative corrections) ----------
app.get('/api/hours/summary', requireAuth, async (req, res) => {
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
  const scope = resolveReportingScope(req, assignedTo, assignedToUPN);
  if (!scope.ok) return res.status(403).json({ ok: false, error: scope.error });

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
  if (!scope.broad) {
    filters.push(
      `AND ${buildOwnReportingAssigneeClause(
        params,
        'd.task_assigned_upn',
        'd.task_assigned_to',
        scope.self,
      )}`,
    );
    idx = params.length;
  } else if (assignedFilter) {
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
app.get('/api/hours/entries', requireAuth, async (req, res) => {
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
  const scope = resolveReportingScope(req, assignedTo, assignedToUPN);
  if (!scope.ok) return res.status(403).json({ ok: false, error: scope.error });

  const limit = Math.min(5000, Math.max(1, Number(req.query.limit || 500)));
  const offset = Math.max(0, Number(req.query.offset || 0));

  const params = [from.toISOString(), toExclusive.toISOString()];
  let idx = params.length;

  const filters = [];
  const assignedFilter = assignedTo || assignedToUPN;
  if (!scope.broad) {
    filters.push(
      `AND ${buildOwnReportingAssigneeClause(
        params,
        'd.task_assigned_upn',
        'd.task_assigned_to',
        scope.self,
      )}`,
    );
    idx = params.length;
  } else if (assignedFilter) {
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
      d.actual_hours AS logged_hours,
      d.ticket_id,
      l.ticket_type,
      l.ticket_title,
      l.feature_id,
      l.feature_title,
      d.cost_type AS cost_type,
      COUNT(*) OVER() AS total_count
    FROM d
    LEFT JOIN public.tfs_task_hours_latest l ON l.task_id = d.task_id
    WHERE d.actual_hours <> 0
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

app.get('/api/hours/export.csv', requireAuth, async (req, res) => {
  const bucket = (req.query.bucket || 'day').toString().trim();
  const from = (req.query.from || '').toString().trim();
  const to = (req.query.to || '').toString().trim();
  const assignedTo = (req.query.assignedTo || '').toString().trim();
  const assignedToUPN = (req.query.assignedToUPN || '').toString().trim();
  const costType = (req.query.costType || req.query.accountCode || '')
    .toString()
    .trim();
  const scope = resolveReportingScope(req, assignedTo, assignedToUPN);
  if (!scope.ok) return res.status(403).send(scope.error);

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
  const assignedFilter = assignedTo || assignedToUPN;
  if (!scope.broad) {
    filters.push(
      `AND ${buildOwnReportingAssigneeClause(
        params,
        'd.task_assigned_upn',
        'd.task_assigned_to',
        scope.self,
      )}`,
    );
    idx = params.length;
  } else if (assignedFilter) {
    idx += 1;
    params.push(`%${assignedFilter}%`);
    filters.push(
      `AND (COALESCE(d.task_assigned_upn,'') ILIKE $${idx} OR COALESCE(d.task_assigned_to,'') ILIKE $${idx})`,
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
app.get('/api/users', requireAuth, async (req, res) => {
  if (!REPORT_BROAD_ROLES.has(req.userRole)) {
    const self = getReportingSelf(req);
    return res.json({
      ok: true,
      users: [{ name: self.name || self.email, upn: self.email }],
    });
  }
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

app.get('/api/pto-users', requireAuth, async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT
        COALESCE(NULLIF(name, ''), email) AS name,
        email                             AS upn
      FROM public.users
      WHERE email IS NOT NULL
        AND email <> ''
        AND email LIKE '%@%'
      ORDER BY COALESCE(NULLIF(name, ''), email) ASC, email ASC
    `);
    res.json({ ok: true, users: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.get('/api/report-registered-users', requireAuth, async (req, res) => {
  if (!REPORT_BROAD_ROLES.has(req.userRole)) {
    const self = getReportingSelf(req);
    return res.json({
      ok: true,
      users: [{ name: self.name || self.email, upn: self.email }],
    });
  }
  try {
    const r = await pool.query(`
      SELECT
        COALESCE(NULLIF(name, ''), email) AS name,
        email                             AS upn
      FROM public.users
      WHERE email IS NOT NULL
        AND email <> ''
        AND email LIKE '%@%'
      ORDER BY COALESCE(NULLIF(name, ''), email) ASC, email ASC
    `);
    res.json({ ok: true, users: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- Cost types ----------
app.get('/api/cost-types', requireAuth, async (req, res) => {
  try {
    if (!REPORT_BROAD_ROLES.has(req.userRole)) {
      const params = [];
      const ownClause = buildOwnReportingAssigneeClause(
        params,
        'task_assigned_upn',
        'task_assigned_to',
        getReportingSelf(req),
      );
      const r = await pool.query(
        `
          SELECT DISTINCT cost_type
          FROM public.tfs_task_hours_latest
          WHERE cost_type IS NOT NULL
            AND cost_type <> ''
            AND ${ownClause}
          ORDER BY cost_type ASC
        `,
        params,
      );
      return res.json({ ok: true, costTypes: r.rows.map((x) => x.cost_type) });
    }

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

function validatePtoHours(v) {
  const n = normNum(v);
  return n === 4 || n === 8 ? n : null;
}

function validatePtoDayPart(v) {
  const s = normText(v);
  return s && VALID_PTO_DAY_PARTS.has(s) ? s : null;
}

function requiresPtoDayPart(hours) {
  return Number(hours) === 4;
}

function formatPtoDayPart(dayPart) {
  if (dayPart === 'first_half') return 'First half';
  if (dayPart === 'second_half') return 'Second half';
  return '';
}

function buildPtoDayPartHtml(dayPart, withBreak = true) {
  const label = formatPtoDayPart(dayPart);
  if (!label) return '';
  return `<strong>Day Part:</strong> ${escapeEmailHtml(label)}${withBreak ? '<br>' : ''}`;
}

function buildPtoDayPartText(dayPart) {
  const label = formatPtoDayPart(dayPart);
  return label ? `\nDay Part: ${label}` : '';
}

/** Format a YYYY-MM-DD string as "Month D, YYYY" for use in email subjects. */
function fmtSubjectDate(ymd) {
  const d = new Date((ymd || '') + 'T00:00:00');
  if (isNaN(d)) return ymd || '';
  return d.toLocaleDateString('en-US', {
    month: 'long',
    day: 'numeric',
    year: 'numeric',
  });
}

/** Format a date range for email subjects. Same date → "Month D, YYYY"; range → "Month D, YYYY – Month D, YYYY". */
function fmtSubjectDateRange(from, to) {
  if (!to || to === from) return fmtSubjectDate(from);
  return `${fmtSubjectDate(from)} \u2013 ${fmtSubjectDate(to)}`;
}

function fmtReportCalendarDateFromTimestamp(value) {
  const d = value instanceof Date ? value : new Date(value || '');
  if (isNaN(d)) return value ? String(value) : '';
  const timeZone = getReportTimeZone();
  if (timeZone) {
    return new Intl.DateTimeFormat('en-US', {
      timeZone,
      month: 'long',
      day: 'numeric',
      year: 'numeric',
    }).format(d);
  }
  const shifted = new Date(d.getTime() + getReportOffsetMinutes() * 60000);
  return new Intl.DateTimeFormat('en-US', {
    timeZone: 'UTC',
    month: 'long',
    day: 'numeric',
    year: 'numeric',
  }).format(shifted);
}

function fmtReportYmdFromTimestamp(value) {
  const d = value instanceof Date ? value : new Date(value || '');
  if (isNaN(d)) return '';
  const timeZone = getReportTimeZone();
  if (timeZone) {
    const parts = new Intl.DateTimeFormat('en-US', {
      timeZone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).formatToParts(d);
    const map = {};
    for (const p of parts) {
      if (p.type !== 'literal') map[p.type] = p.value;
    }
    return `${map.year}-${map.month}-${map.day}`;
  }
  const shifted = new Date(d.getTime() + getReportOffsetMinutes() * 60000);
  return shifted.toISOString().slice(0, 10);
}

function fmtReportDateTimeFromTimestamp(value) {
  const d = value instanceof Date ? value : new Date(value || '');
  if (isNaN(d)) return '';
  const timeZone = getReportTimeZone();
  let formatted;
  if (timeZone) {
    const parts = new Intl.DateTimeFormat('en-US', {
      timeZone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).formatToParts(d);
    const map = {};
    for (const p of parts) {
      if (p.type !== 'literal') map[p.type] = p.value;
    }
    formatted = `${map.year}-${map.month}-${map.day} ${map.hour}:${map.minute}`;
  } else {
    const shifted = new Date(d.getTime() + getReportOffsetMinutes() * 60000);
    formatted = shifted.toISOString().replace('T', ' ').slice(0, 16);
  }
  return `${formatted} ${REPORT_TZ_LABEL || 'UTC'}`;
}

async function fetchLastSyncAt() {
  const r = await pool.query(
    `SELECT run_at
     FROM public.tfs_hours_runs
     ORDER BY run_at DESC, run_id DESC
     LIMIT 1`,
  );
  return r.rows[0]?.run_at ?? null;
}

function buildLastSyncEmailSuffix(lastSyncAt) {
  const formatted = fmtReportDateTimeFromTimestamp(lastSyncAt);
  return formatted
    ? `, based on <strong>Last sync: ${escapeEmailHtml(formatted)}</strong>`
    : '';
}

function sanitizeFilenamePart(value, fallback) {
  const cleaned = String(value || '')
    .replace(/[\\/:*?"<>|\x00-\x1F]+/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/[. ]+$/g, '');
  return cleaned || fallback;
}

function buildPtoPdfFilename(userName, submittedAt) {
  const safeName = sanitizeFilenamePart(userName, 'Employee');
  const requestedDate = sanitizeFilenamePart(
    fmtReportYmdFromTimestamp(submittedAt),
    'Unknown Date',
  );
  return `Leave Form - ${safeName} - ${requestedDate}.pdf`;
}

// ---------- Public Holidays ----------
app.get('/api/holidays', requireAuth, async (req, res) => {
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

app.post(
  '/api/holidays',
  requireAuth,
  requireManagerOrAbove,
  async (req, res) => {
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
  },
);

app.delete(
  '/api/holidays/:id',
  requireAuth,
  requireManagerOrAbove,
  async (req, res) => {
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
  },
);

// ---------- Team Off ----------
app.get('/api/team-off', requireAuth, async (req, res) => {
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

app.post(
  '/api/team-off',
  requireAuth,
  requireManagerOrAbove,
  async (req, res) => {
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
  },
);

app.delete(
  '/api/team-off/:id',
  requireAuth,
  requireManagerOrAbove,
  async (req, res) => {
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
  },
);

// ---------- Outlook Thread-Index helpers ----------
// Outlook/Exchange uses Thread-Topic + Thread-Index (MS-proprietary headers) for conversation
// grouping — independent of Message-Id, which Brevo rewrites on delivery.
// Base block: 22 bytes = 0x01 (version) + 4-byte timestamp (zeroed) + 16-byte GUID + 1-byte reserved.
// Reply block: parent base + 5 random bytes per reply level.

function _threadIndexBase(uuid) {
  const uuidBytes = Buffer.from(uuid.replace(/-/g, ''), 'hex'); // 16 bytes
  return Buffer.concat([
    Buffer.from([0x01]),
    Buffer.alloc(4),
    uuidBytes,
    Buffer.alloc(1),
  ]);
}

/** Build Thread-Index for the root email given the UUID portion of our generated Message-Id. */
function buildThreadIndex(uuid) {
  return _threadIndexBase(uuid).toString('base64');
}

/**
 * Build Thread-Index for a reply email.
 * parentMsgId must be in the form <UUID@tfs-hours>; returns undefined for old-format IDs.
 */
function buildReplyThreadIndex(parentMsgId) {
  const m = (parentMsgId || '').match(/^<([0-9a-f-]{36})@tfs-hours>$/i);
  if (!m) return undefined;
  return Buffer.concat([
    _threadIndexBase(m[1]),
    crypto.randomBytes(5),
  ]).toString('base64');
}

// ---------- Individual PTO ----------
const VALID_LEAVE_TYPES = new Set([
  'Personal',
  'Emergency',
  'Sick',
  'Maternity',
  'Bereavement',
]);
const VALID_PTO_DAY_PARTS = new Set(['first_half', 'second_half']);

/** Roles that must enforce own-data filtering (cannot view/file for others) */
const OWN_DATA_ROLES = new Set(['dev', 'qa', 'ts']);

app.get('/api/pto', requireAuth, async (req, res) => {
  const fromStr = validateDateStr(req.query.from);
  const toStr = validateDateStr(req.query.to);
  const userFilter = normText(req.query.userUpn || req.query.assignedTo);
  const actionRequired = req.query.actionRequired === 'true';

  const SELECT = `
    SELECT p.id, p.user_upn, p.user_name, p.entry_date::text, p.hours, p.day_part, p.leave_type, p.notes, p.created_at,
           p.status, p.filer_role, COALESCE(p.filer_team, u.team) AS filer_team,
           p.approved_by_lead, p.lead_actioned_at,
           p.approved_by_pm, p.pm_actioned_at, p.denied_by, p.denied_at, p.denial_note, p.batch_id,
           p.cancelled_by, p.cancelled_at, p.cancel_note,
           p.external_requested_at, p.external_requested_by, p.external_request_recipients,
           p.external_received_at, p.external_received_by, p.external_received_note
    FROM public.pto_entries p
    LEFT JOIN public.users u ON LOWER(u.email) = LOWER(p.user_upn)`;

  try {
    // Own-data roles: dev, qa — see only their own entries
    if (OWN_DATA_ROLES.has(req.userRole)) {
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
      params.push(`%${req.userEmail}%`);
      where.push(
        `(COALESCE(user_upn,'') ILIKE $${params.length} OR COALESCE(user_name,'') ILIKE $${params.length})`,
      );
      const r = await pool.query(
        `${SELECT} ${where.length ? 'WHERE ' + where.join(' AND ') : ''} ORDER BY entry_date ASC, user_name ASC`,
        params,
      );
      return res.json({ ok: true, rows: r.rows });
    }

    // Lead: own entries UNION team's visible dev/qa entries
    if (req.userRole === 'lead') {
      const params = [];
      const dateWhere = [];
      if (fromStr) {
        params.push(fromStr);
        dateWhere.push(`entry_date >= $${params.length}::date`);
      }
      if (toStr) {
        params.push(toStr);
        dateWhere.push(`entry_date <= $${params.length}::date`);
      }
      const dateClause = dateWhere.length
        ? 'AND ' + dateWhere.join(' AND ')
        : '';

      let actionableTeamClause;
      let visibleTeamClause;
      if (req.userTeam) {
        params.push(req.userTeam);
        const teamIdx = params.length;
        const pendingTeamClause = `filer_role IN ('dev','qa') AND status = 'pending' AND COALESCE(p.filer_team, u.team) = $${teamIdx}`;
        const approvedTeamClause = `filer_role IN ('dev','qa') AND status = 'approved' AND COALESCE(p.filer_team, u.team) = $${teamIdx}`;
        if (isSpecialPtoTeamName(req.userTeam)) {
          actionableTeamClause = `((${pendingTeamClause}) OR (filer_role = 'qa' AND status IN ('pending','external_pending') AND LOWER(COALESCE(p.filer_team, u.team, '')) = LOWER($${teamIdx})))`;
        } else {
          actionableTeamClause = pendingTeamClause;
        }
        visibleTeamClause = `((${actionableTeamClause}) OR (${approvedTeamClause}))`;
      } else {
        if (SPECIAL_PTO_WORKFLOW_TEAM_KEY) {
          params.push(SPECIAL_PTO_WORKFLOW_TEAM);
          actionableTeamClause = `filer_role IN ('dev','qa') AND status = 'pending' AND NOT (filer_role = 'qa' AND LOWER(COALESCE(p.filer_team, u.team, '')) = LOWER($${params.length}))`;
        } else {
          actionableTeamClause = `filer_role IN ('dev','qa') AND status = 'pending'`;
        }
        visibleTeamClause = actionableTeamClause;
      }

      if (actionRequired) {
        // Only show actionable team entries; approved rows are visible-only.
        const r = await pool.query(
          `${SELECT} WHERE ${actionableTeamClause} ${dateClause} ORDER BY entry_date ASC, user_name ASC`,
          params,
        );
        return res.json({ ok: true, rows: r.rows });
      }

      params.push(req.userEmail);
      const ownIdx = params.length;

      const r = await pool.query(
        `${SELECT} WHERE (LOWER(COALESCE(user_upn,'')) = LOWER($${ownIdx}) ${dateClause})
           OR (${visibleTeamClause} ${dateClause})
         ORDER BY entry_date ASC, user_name ASC`,
        params,
      );
      return res.json({ ok: true, rows: r.rows });
    }

    // PM and admin: full access with optional filters
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
    const r = await pool.query(
      `${SELECT} ${where.length ? 'WHERE ' + where.join(' AND ') : ''} ORDER BY entry_date ASC, user_name ASC`,
      params,
    );
    let rows = r.rows;
    if (actionRequired && req.userRole === 'pm') {
      const actor = {
        role: req.userRole,
        email: req.userEmail,
        team: req.userTeam,
      };
      rows = rows.filter((row) => {
        const internalError = checkApprovalAccess(
          row,
          req.userRole,
          req.userEmail,
          req.userTeam,
        );
        const externalTransition = getExternalReceivedTransition(row, actor);
        return !internalError || !externalTransition.error;
      });
    }
    res.json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// Max working days allowed per date-range PTO submission
const PTO_MAX_WORKING_DAYS = 15;

/** Return an array of YYYY-MM-DD strings for each weekday (Mon–Fri) between fromStr and toStr (inclusive). */
function weekdaysInRange(fromStr, toStr) {
  const dates = [];
  const cur = new Date(fromStr + 'T00:00:00');
  const end = new Date(toStr + 'T00:00:00');
  while (cur <= end) {
    const dow = cur.getDay(); // 0=Sun, 6=Sat
    if (dow !== 0 && dow !== 6) {
      const y = cur.getFullYear();
      const m = String(cur.getMonth() + 1).padStart(2, '0');
      const d = String(cur.getDate()).padStart(2, '0');
      dates.push(`${y}-${m}-${d}`);
    }
    cur.setDate(cur.getDate() + 1);
  }
  return dates;
}

function formatYmd(year, month, day) {
  return `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}

function currentReportYmd() {
  const now = new Date();
  const tz = getReportTimeZone();
  if (tz) {
    const parts = getTimeZoneParts(now, tz);
    return formatYmd(parts.year, parts.month, parts.day);
  }
  return new Date(now.getTime() + getReportOffsetMinutes() * 60 * 1000)
    .toISOString()
    .slice(0, 10);
}

function isWeekdayYmd(ymd) {
  const p = parseYmd(ymd);
  if (!p) return false;
  const dow = new Date(Date.UTC(p.y, p.mo - 1, p.d)).getUTCDay();
  return dow >= 1 && dow <= 5;
}

function countOverdueBusinessDays(startYmd, todayYmd, sharedOffDays) {
  if (!startYmd || !todayYmd || startYmd >= todayYmd) return 0;
  let count = 0;
  let cur = addDaysToYmd(startYmd, 1);
  while (cur && cur <= todayYmd) {
    if (isWeekdayYmd(cur) && !sharedOffDays.has(cur)) count++;
    cur = addDaysToYmd(cur, 1);
  }
  return count;
}

function dedupeEmails(emails) {
  const out = [];
  const seen = new Set();
  for (const raw of emails || []) {
    const email = String(raw || '').trim();
    if (!email) continue;
    const key = email.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(email);
  }
  return out;
}

function makeExternalPtoToken() {
  return crypto.randomBytes(32).toString('base64url');
}

function hashExternalPtoToken(token) {
  return crypto
    .createHash('sha256')
    .update(String(token || ''))
    .digest('hex');
}

function getRequestBaseUrl(req) {
  if (PUBLIC_BASE_URL) return PUBLIC_BASE_URL.replace(/\/+$/, '');
  const proto = req.get('x-forwarded-proto') || req.protocol || 'http';
  return `${proto}://${req.get('host')}`;
}

function externalPtoRequestKey(entry) {
  if (entry?.batch_id) return `batch:${entry.batch_id}`;
  return `single:${entry?.id}`;
}

function externalPtoActionLinks(baseUrl, token) {
  const page = `${baseUrl}/external/pto/${encodeURIComponent(token)}`;
  return {
    page,
    approve: `${page}?action=approve`,
    deny: `${page}?action=deny`,
  };
}

async function resolveRegisteredPtoUser(userName, userUpn) {
  const upn = normText(userUpn);
  if (upn) {
    const byEmail = await pool.query(
      `SELECT email, COALESCE(NULLIF(name, ''), email) AS name, team
       FROM public.users
       WHERE email IS NOT NULL
         AND email <> ''
         AND LOWER(email) = LOWER($1)`,
      [upn],
    );
    if (byEmail.rows[0]) return { user: byEmail.rows[0] };
  }

  const name = normText(userName);
  if (name) {
    const byName = await pool.query(
      `SELECT email, COALESCE(NULLIF(name, ''), email) AS name, team
       FROM public.users
       WHERE email IS NOT NULL
         AND email <> ''
         AND name IS NOT NULL
         AND name <> ''
         AND LOWER(name) = LOWER($1)
       ORDER BY email ASC`,
      [name],
    );
    if (byName.rows.length === 1) return { user: byName.rows[0] };
    if (byName.rows.length > 1) {
      return {
        error:
          'Multiple registered users share this name. Please use email/UPN.',
      };
    }
  }

  return { error: 'Selected user is not a registered app user' };
}

async function resolvePtoFilerTeam(userUpn, fallbackTeam = null) {
  const upn = normText(userUpn);
  if (!upn) return normText(fallbackTeam);
  try {
    const r = await pool.query(
      `SELECT team
       FROM public.users
       WHERE email IS NOT NULL
         AND email <> ''
         AND LOWER(email) = LOWER($1)`,
      [upn],
    );
    return normText(r.rows[0]?.team) || normText(fallbackTeam);
  } catch {
    return normText(fallbackTeam);
  }
}

async function resolveUserDisplayNameByEmail(email) {
  const upn = normText(email);
  if (!upn) return '';
  const r = await pool.query(
    `SELECT COALESCE(NULLIF(name, ''), email) AS name
     FROM public.users
     WHERE email IS NOT NULL
       AND email <> ''
       AND LOWER(email) = LOWER($1)`,
    [upn],
  );
  return r.rows[0]?.name || upn;
}

async function buildLeadApprovalEmailDetails(approvedByLead, leadActionedAt) {
  const leadEmail = normText(approvedByLead);
  if (!leadEmail) return { html: '', text: '' };
  const leadName = await resolveUserDisplayNameByEmail(leadEmail);
  const leadDate = normText(leadActionedAt)
    ? fmtReportCalendarDateFromTimestamp(leadActionedAt)
    : '';
  const summaryHtml = leadDate
    ? `Lead approval: <strong>${escapeEmailHtml(leadName)}</strong> on <strong>${escapeEmailHtml(leadDate)}</strong>.`
    : `Lead approval: <strong>${escapeEmailHtml(leadName)}</strong>.`;
  const summaryText = leadDate
    ? `Lead approval: ${leadName} on ${leadDate}.`
    : `Lead approval: ${leadName}.`;
  return {
    html: `<p>${summaryHtml}</p>`,
    text: `\n${summaryText}`,
  };
}

function ptoRowsDateRange(rows) {
  const list = Array.isArray(rows) && rows.length ? rows : [];
  const first = list[0];
  const last = list[list.length - 1] || first;
  return fmtSubjectDateRange(first?.entry_date, last?.entry_date);
}

function ptoRowsTotalHours(rows) {
  return (rows || []).reduce((sum, row) => sum + Number(row.hours || 0), 0);
}

function ptoThreadTopic(entry, dateLabel) {
  return `LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${dateLabel}`;
}

function ptoReplyHeaders(entry, dateLabel) {
  return buildPtoThreadHeaders(
    entry.email_message_id,
    ptoThreadTopic(entry, dateLabel),
  );
}

async function createExternalApprovalTokens(entry, baseUrl) {
  const toList = dedupeEmails(SPECIAL_PTO_EXTERNAL_APPROVER_EMAILS);
  if (!toList.length) return [];

  const requestKey = externalPtoRequestKey(entry);
  const expiresAt = new Date(
    Date.now() + EXTERNAL_PTO_TOKEN_TTL_DAYS * 24 * 60 * 60 * 1000,
  );

  await pool.query(
    `UPDATE public.pto_external_approval_tokens
     SET revoked_at = NOW()
     WHERE request_key = $1
       AND used_at IS NULL
       AND revoked_at IS NULL`,
    [requestKey],
  );

  const created = [];
  for (const email of toList) {
    const token = makeExternalPtoToken();
    const tokenHash = hashExternalPtoToken(token);
    await pool.query(
      `INSERT INTO public.pto_external_approval_tokens
         (token_hash, request_key, pto_entry_id, batch_id, recipient_email, expires_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        tokenHash,
        requestKey,
        entry.id || null,
        entry.batch_id || null,
        email,
        expiresAt.toISOString(),
      ],
    );
    created.push({
      email,
      token,
      links: externalPtoActionLinks(baseUrl, token),
    });
  }
  return created;
}

async function sendSpecialExternalApprovalRequestEmail(
  entry,
  rows,
  actorLabel,
  baseUrl,
) {
  const tokenRecords = await createExternalApprovalTokens(entry, baseUrl);
  if (!tokenRecords.length) return;

  const transporter = createMailTransporter();
  const dateLabel = ptoRowsDateRange(rows);
  const displayName = entry.user_name || entry.user_upn;
  const totalHours = ptoRowsTotalHours(rows);
  const headers = ptoReplyHeaders(entry, dateLabel);

  for (const tokenRecord of tokenRecords) {
    await transporter.sendMail({
      from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
      to: tokenRecord.email,
      subject: `External PTO Approval Request \u2013 ${displayName} \u2013 ${entry.leave_type} Leave on ${dateLabel}`,
      html: `<p>Hi @Team,</p>
<p>The PTO request for <strong>${escapeEmailHtml(displayName)}</strong> has completed internal review and now needs external approval.</p>
<p style="font-family:sans-serif;font-size:13px;line-height:1.8">
  <strong>Leave Date:</strong> ${escapeEmailHtml(dateLabel)}<br>
  <strong>Leave Duration:</strong> ${fmtH(totalHours)} hrs<br>
  ${buildPtoDayPartHtml(entry.day_part)}
  <strong>Leave Type:</strong> ${escapeEmailHtml(entry.leave_type)}<br>
  <strong>Internal Reviewer:</strong> ${escapeEmailHtml(actorLabel || '')}<br>
  <strong>Reason for Leave:</strong> ${escapeEmailHtml(entry.notes || '\u2014')}
</p>
<p>
  <a href="${escapeEmailHtml(tokenRecord.links.approve)}" style="display:inline-block;padding:9px 14px;background:#1f7a6b;color:#ffffff;text-decoration:none;border-radius:4px;margin-right:8px;">Approve</a>
  <a href="${escapeEmailHtml(tokenRecord.links.deny)}" style="display:inline-block;padding:9px 14px;background:#c8742b;color:#ffffff;text-decoration:none;border-radius:4px;">Deny</a>
</p>
<p>If the buttons do not open, use this link: <a href="${escapeEmailHtml(tokenRecord.links.page)}">${escapeEmailHtml(tokenRecord.links.page)}</a></p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report. Please do not reply to this email.</p>`,
      text: `Hi @Team,\n\nThe PTO request for ${displayName} has completed internal review and now needs external approval.\n\nLeave Date: ${dateLabel}\nLeave Duration: ${fmtH(totalHours)} hrs${buildPtoDayPartText(entry.day_part)}\nLeave Type: ${entry.leave_type}\nInternal Reviewer: ${actorLabel || ''}\nReason for Leave: ${entry.notes || '-'}\n\nApprove: ${tokenRecord.links.approve}\nDeny: ${tokenRecord.links.deny}\nReview: ${tokenRecord.links.page}\n\n---\nAutomated message — TFS Hours Report. Please do not reply to this email.`,
      headers,
    });
  }
}

async function sendSpecialExternalReceivedLeadApprovedEmail(
  entry,
  rows,
  actorLabel,
) {
  const pmEmails = await getApproverEmails('lead', entry.filer_team, null);
  const toList = dedupeEmails(pmEmails);
  if (!toList.length) return;

  const transporter = createMailTransporter();
  const dateLabel = ptoRowsDateRange(rows);
  const displayName = entry.user_name || entry.user_upn;
  const totalHours = ptoRowsTotalHours(rows);
  const headers = ptoReplyHeaders(entry, dateLabel);

  await transporter.sendMail({
    from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
    to: toList.join(', '),
    subject: `Re: ${ptoThreadTopic(entry, dateLabel)}`,
    html: `<p>Hi @Team,</p>
<p>External approval for <strong>${escapeEmailHtml(displayName)}</strong>'s PTO request on <strong>${escapeEmailHtml(dateLabel)}</strong> has been marked received by <strong>${escapeEmailHtml(actorLabel || '')}</strong>.</p>
<p><strong>Leave Duration:</strong> ${fmtH(totalHours)} hrs<br>${buildPtoDayPartHtml(entry.day_part)}<strong>Leave Type:</strong> ${escapeEmailHtml(entry.leave_type)}</p>
<p>This request is ready for same-team PM final approval.</p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report. Please do not reply to this email.</p>`,
    text: `External approval for ${displayName}'s PTO request on ${dateLabel} has been marked received by ${actorLabel || ''}.\nLeave Duration: ${fmtH(totalHours)} hrs${buildPtoDayPartText(entry.day_part)}\nLeave Type: ${entry.leave_type}\nThis request is ready for same-team PM final approval.\n\n---\nAutomated message — TFS Hours Report. Please do not reply to this email.`,
    headers,
  });
}

async function sendPtoFinalApprovalEmail(entry, rows, actorLabel) {
  const transporter = createMailTransporter();
  const dateLabel = ptoRowsDateRange(rows);
  const displayName = entry.user_name || entry.user_upn;
  const totalHours = ptoRowsTotalHours(rows);
  const headers = ptoReplyHeaders(entry, dateLabel);
  const totalDays = rows.length;

  let leadEmails = [];
  const leadApprovalDetails = await buildLeadApprovalEmailDetails(
    entry.approved_by_lead,
    entry.lead_actioned_at,
  );
  if (['dev', 'qa'].includes(entry.filer_role) || entry.filer_role === 'lead') {
    leadEmails = await getApproverEmails('dev', entry.filer_team, null);
  }

  const toList = dedupeEmails([entry.user_upn, ...leadEmails]);
  if (!toList.length) return;

  let attachments = [];
  try {
    const submittedAt = entry.created_at
      ? new Date(entry.created_at).toUTCString()
      : new Date().toUTCString();
    const pdfBuf = await generatePtoPdf({
      userName: displayName,
      entryDate: rows[0].entry_date,
      entryDateTo: totalDays > 1 ? rows[rows.length - 1].entry_date : null,
      totalDays,
      hours: entry.hours,
      dayPart: entry.day_part,
      leaveType: entry.leave_type,
      notes: entry.notes || '',
      submittedAt,
    });
    attachments = [
      {
        filename: buildPtoPdfFilename(displayName, submittedAt),
        content: pdfBuf,
        contentType: 'application/pdf',
      },
    ];
  } catch (pdfErr) {
    console.error('PTO final approval PDF error:', pdfErr);
  }

  const ccList = EXTRA_CC_EMAILS.length ? EXTRA_CC_EMAILS : [];
  await transporter.sendMail({
    from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
    to: toList.join(', '),
    ...(ccList.length ? { cc: ccList.join(', ') } : {}),
    subject: `Re: ${ptoThreadTopic(entry, dateLabel)}`,
    html: `<p>Hi @Team,</p><p>The PTO request for <strong>${escapeEmailHtml(displayName)}</strong> (${escapeEmailHtml(dateLabel)}) has been <strong>fully approved by ${escapeEmailHtml(actorLabel || '')}</strong>.</p>
${leadApprovalDetails.html}
<p><strong>Leave Duration:</strong> ${fmtH(totalHours)} hrs<br>${buildPtoDayPartHtml(entry.day_part)}<strong>Leave Type:</strong> ${escapeEmailHtml(entry.leave_type)}</p>
<p>The approved request has been added to the team calendar.</p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report.</p>`,
    text: `PTO for ${displayName} (${dateLabel}) fully approved by ${actorLabel || ''}.${leadApprovalDetails.text}\nLeave Duration: ${fmtH(totalHours)} hrs${buildPtoDayPartText(entry.day_part)}\nLeave Type: ${entry.leave_type}.\n\n---\nAutomated message — TFS Hours Report.`,
    headers,
    attachments,
  });
}

async function sendPtoExternalDenialEmail(entry, rows, actorLabel, denialNote) {
  const transporter = createMailTransporter();
  const dateLabel = ptoRowsDateRange(rows);
  const displayName = entry.user_name || entry.user_upn;
  const totalHours = ptoRowsTotalHours(rows);
  const headers = ptoReplyHeaders(entry, dateLabel);
  const approverEmails = await getApproverEmails(
    entry.filer_role,
    entry.filer_team,
    entry.user_upn,
  );
  const ccEmails = dedupeEmails(approverEmails);

  await transporter.sendMail({
    from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
    to: entry.user_upn,
    ...(ccEmails.length ? { cc: ccEmails.join(', ') } : {}),
    subject: `Re: ${ptoThreadTopic(entry, dateLabel)}`,
    html: `<p>Hi @Team,</p><p>The PTO request for <strong>${escapeEmailHtml(displayName)}</strong> (${escapeEmailHtml(dateLabel)}) has been <strong>denied by ${escapeEmailHtml(actorLabel || '')}</strong>.</p>
<p><strong>Leave Duration:</strong> ${fmtH(totalHours)} hrs<br>${buildPtoDayPartHtml(entry.day_part)}<strong>Leave Type:</strong> ${escapeEmailHtml(entry.leave_type)}</p>
${denialNote ? `<p><strong>Reason:</strong> ${escapeEmailHtml(denialNote)}</p>` : ''}
<p>You may resubmit a new PTO request if needed.</p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report. Please do not reply to this email.</p>`,
    text: `PTO for ${displayName} (${dateLabel}) was denied by ${actorLabel || ''}.\nLeave Duration: ${fmtH(totalHours)} hrs${buildPtoDayPartText(entry.day_part)}\nLeave Type: ${entry.leave_type}.${denialNote ? ' Reason: ' + denialNote : ''}\n\n---\nAutomated message — TFS Hours Report. Please do not reply to this email.`,
    headers,
  });
}

function getExternalTokenApprovalTransition(entry) {
  if (!isSpecialPtoWorkflow(entry))
    return { error: 'entry is not in the special PTO workflow' };
  if (entry?.status !== 'external_pending')
    return { error: 'entry is not awaiting external approval' };
  if (entry.filer_role === 'qa')
    return { nextStatus: 'lead_approved', notification: 'lead_approved' };
  if (entry.filer_role === 'lead' || entry.filer_role === 'pm')
    return { nextStatus: 'approved', notification: 'final_approved' };
  return { error: 'entry is not in the special PTO workflow' };
}

async function fetchExternalPtoRowsForToken(
  tokenRow,
  client = pool,
  forUpdate = false,
) {
  const lockSql = forUpdate ? ' FOR UPDATE OF p' : '';
  if (tokenRow.batch_id) {
    const r = await client.query(
      `SELECT p.id, p.user_upn, p.user_name, p.entry_date::text, p.hours, p.leave_type,
              p.day_part, p.status, p.filer_role, COALESCE(p.filer_team, u.team) AS filer_team,
              p.approved_by_lead, p.lead_actioned_at, p.approved_by_pm, p.pm_actioned_at,
              p.email_message_id, p.batch_id, p.notes, p.created_at
       FROM public.pto_entries p
       LEFT JOIN public.users u ON LOWER(u.email) = LOWER(p.user_upn)
       WHERE p.batch_id = $1
       ORDER BY p.entry_date ASC${lockSql}`,
      [tokenRow.batch_id],
    );
    return r.rows;
  }

  const r = await client.query(
    `SELECT p.id, p.user_upn, p.user_name, p.entry_date::text, p.hours, p.leave_type,
            p.day_part, p.status, p.filer_role, COALESCE(p.filer_team, u.team) AS filer_team,
            p.approved_by_lead, p.lead_actioned_at, p.approved_by_pm, p.pm_actioned_at,
            p.email_message_id, p.batch_id, p.notes, p.created_at
     FROM public.pto_entries p
     LEFT JOIN public.users u ON LOWER(u.email) = LOWER(p.user_upn)
     WHERE p.id = $1
     ORDER BY p.entry_date ASC${lockSql}`,
    [tokenRow.pto_entry_id],
  );
  return r.rows;
}

function isExternalTokenUsable(tokenRow) {
  if (!tokenRow) return false;
  if (tokenRow.used_at || tokenRow.revoked_at) return false;
  return new Date(tokenRow.expires_at).getTime() > Date.now();
}

function buildExternalPtoSummary(tokenRow, rows) {
  const entry = rows[0];
  return {
    recipientEmail: tokenRow.recipient_email,
    employee: entry.user_name || entry.user_upn,
    userUpn: entry.user_upn,
    dateLabel: ptoRowsDateRange(rows),
    totalHours: ptoRowsTotalHours(rows),
    dayCount: rows.length,
    dayPart: formatPtoDayPart(entry.day_part),
    leaveType: entry.leave_type,
    reason: entry.notes || '',
    status: entry.status,
    filerRole: entry.filer_role,
    filerTeam: entry.filer_team,
    expiresAt: tokenRow.expires_at,
  };
}

async function loadExternalPtoTokenContext(
  token,
  client = pool,
  forUpdate = false,
) {
  const tokenHash = hashExternalPtoToken(token);
  const tokenSql = `SELECT token_hash, request_key, pto_entry_id, batch_id, recipient_email,
                          expires_at, used_at, revoked_at, action, note
                   FROM public.pto_external_approval_tokens
                   WHERE token_hash = $1${forUpdate ? ' FOR UPDATE' : ''}`;
  const tokenRes = await client.query(tokenSql, [tokenHash]);
  const tokenRow = tokenRes.rows[0];
  if (!isExternalTokenUsable(tokenRow)) return null;

  const rows = await fetchExternalPtoRowsForToken(tokenRow, client, forUpdate);
  if (!rows.length) return null;
  const batchError =
    rows.length > 1 ? validatePtoBatchTransitionRows(rows) : null;
  if (batchError) return null;

  const entry = rows[0];
  if (entry.status !== 'external_pending' || !isSpecialPtoWorkflow(entry))
    return null;
  return { tokenRow, rows, entry };
}

async function processExternalPtoDecision(req, res, action) {
  const token = normText(req.params.token);
  const note = normText(req.body?.note) || null;
  if (!token)
    return res
      .status(404)
      .json({ ok: false, error: 'invalid or expired link' });
  if (action === 'denied' && !note)
    return res
      .status(400)
      .json({ ok: false, error: 'denial reason is required' });

  const client = await pool.connect();
  let notification = null;
  let tokenRow = null;
  let rows = [];
  let entry = null;
  try {
    await client.query('BEGIN');
    const ctx = await loadExternalPtoTokenContext(token, client, true);
    if (!ctx) {
      await client.query('ROLLBACK');
      return res
        .status(410)
        .json({ ok: false, error: 'invalid or expired link' });
    }
    ({ tokenRow, rows, entry } = ctx);

    let nextStatus;
    if (action === 'approved') {
      const transition = getExternalTokenApprovalTransition(entry);
      if (transition.error) {
        await client.query('ROLLBACK');
        return res.status(409).json({ ok: false, error: transition.error });
      }
      nextStatus = transition.nextStatus;
      notification = transition.notification;
    } else {
      nextStatus = 'denied';
      notification = 'denied';
    }

    if (tokenRow.batch_id) {
      await client.query(
        action === 'approved'
          ? `UPDATE public.pto_entries
             SET status = $1,
                 external_received_at = NOW(),
                 external_received_by = $2,
                 external_received_note = $3
             WHERE batch_id = $4`
          : `UPDATE public.pto_entries
             SET status = 'denied',
                 denied_by = $1,
                 denied_at = NOW(),
                 denial_note = $2,
                 external_received_at = NOW(),
                 external_received_by = $1,
                 external_received_note = $2
             WHERE batch_id = $3`,
        action === 'approved'
          ? [nextStatus, tokenRow.recipient_email, note, tokenRow.batch_id]
          : [tokenRow.recipient_email, note, tokenRow.batch_id],
      );
    } else {
      await client.query(
        action === 'approved'
          ? `UPDATE public.pto_entries
             SET status = $1,
                 external_received_at = NOW(),
                 external_received_by = $2,
                 external_received_note = $3
             WHERE id = $4`
          : `UPDATE public.pto_entries
             SET status = 'denied',
                 denied_by = $1,
                 denied_at = NOW(),
                 denial_note = $2,
                 external_received_at = NOW(),
                 external_received_by = $1,
                 external_received_note = $2
             WHERE id = $3`,
        action === 'approved'
          ? [nextStatus, tokenRow.recipient_email, note, tokenRow.pto_entry_id]
          : [tokenRow.recipient_email, note, tokenRow.pto_entry_id],
      );
    }

    await client.query(
      `UPDATE public.pto_external_approval_tokens
       SET used_at = NOW(), action = $1, note = $2
       WHERE token_hash = $3`,
      [action, note, tokenRow.token_hash],
    );
    await client.query(
      `UPDATE public.pto_external_approval_tokens
       SET revoked_at = NOW()
       WHERE request_key = $1
         AND token_hash <> $2
         AND used_at IS NULL
         AND revoked_at IS NULL`,
      [tokenRow.request_key, tokenRow.token_hash],
    );

    await client.query('COMMIT');
    res.json({
      ok: true,
      action,
      status: nextStatus,
      message:
        action === 'approved'
          ? 'External approval recorded.'
          : 'External denial recorded.',
    });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {
      /* ignore */
    }
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  } finally {
    client.release();
  }

  if (BREVO_API_KEY && NOTIFY_FROM_EMAIL && notification) {
    (async () => {
      try {
        if (notification === 'lead_approved') {
          await sendSpecialExternalReceivedLeadApprovedEmail(
            entry,
            rows,
            tokenRow.recipient_email,
          );
        } else if (notification === 'final_approved') {
          await sendPtoFinalApprovalEmail(
            entry,
            rows,
            tokenRow.recipient_email,
          );
        } else if (notification === 'denied') {
          await sendPtoExternalDenialEmail(
            entry,
            rows,
            tokenRow.recipient_email,
            note,
          );
        }
      } catch (emailErr) {
        console.error('External PTO decision email error:', emailErr);
      }
    })();
  }
}

app.post('/api/pto', requireAuth, async (req, res) => {
  const {
    user_name: unRaw,
    user_upn: uupnRaw,
    // Range fields (preferred); fall back to single entry_date for backward compat
    entry_date_from: edFromRaw,
    entry_date_to: edToRaw,
    entry_date: edRaw,
    hours: hoursRaw,
    day_part: dpRaw,
    leave_type: ltRaw,
    notes: notesRaw,
  } = req.body || {};
  let user_name = normText(unRaw);
  let user_upn = normText(uupnRaw) || user_name;

  // Resolve date(s): prefer entry_date_from/to, fall back to entry_date
  const dateFrom = validateDateStr(edFromRaw || edRaw);
  const dateTo = validateDateStr(edToRaw || edFromRaw || edRaw);

  const hours = validatePtoHours(hoursRaw ?? 8);
  const rawDayPart = normText(dpRaw);
  const validatedDayPart = validatePtoDayPart(rawDayPart);
  const leave_type_raw = normText(ltRaw);
  const leave_type =
    leave_type_raw && VALID_LEAVE_TYPES.has(leave_type_raw)
      ? leave_type_raw
      : 'Personal';
  const notes = normText(notesRaw);

  // Own-data roles: dev, qa — can only file PTO for themselves
  if (OWN_DATA_ROLES.has(req.userRole)) {
    user_upn = req.userEmail;
    if (!user_name) user_name = req.userName;
  }

  if (!user_name && !user_upn)
    return res
      .status(400)
      .json({ ok: false, error: 'user_name or user_upn is required' });
  if (!dateFrom)
    return res.status(400).json({
      ok: false,
      error: 'entry_date (or entry_date_from) is required (YYYY-MM-DD)',
    });
  if (dateTo < dateFrom)
    return res
      .status(400)
      .json({ ok: false, error: 'entry_date_to must be >= entry_date_from' });
  if (hours === null)
    return res
      .status(400)
      .json({ ok: false, error: 'hours must be 4 or 8 for PTO' });
  if (rawDayPart && !validatedDayPart)
    return res.status(400).json({
      ok: false,
      error: 'day_part must be first_half or second_half',
    });
  if (requiresPtoDayPart(hours) && !validatedDayPart)
    return res.status(400).json({
      ok: false,
      error: 'day_part is required when hours is 4',
    });
  if (!requiresPtoDayPart(hours) && validatedDayPart)
    return res.status(400).json({
      ok: false,
      error: 'day_part is only allowed when hours is 4',
    });

  const isRange = dateTo !== dateFrom;
  const weekdays = isRange ? weekdaysInRange(dateFrom, dateTo) : [dateFrom];
  const day_part = requiresPtoDayPart(hours) ? validatedDayPart : null;

  if (weekdays.length === 0)
    return res.status(400).json({
      ok: false,
      error: 'date range contains no working days (Mon–Fri)',
    });
  if (weekdays.length > PTO_MAX_WORKING_DAYS)
    return res.status(400).json({
      ok: false,
      error: `date range exceeds the maximum of ${PTO_MAX_WORKING_DAYS} working days`,
    });

  // admin PTOs are auto-approved; workflow can also be disabled globally via PTO_APPROVAL_ENABLED=false
  const initialStatus =
    req.userRole === 'admin' || !PTO_APPROVAL_ENABLED ? 'approved' : 'pending';
  const filerRole = req.userRole;
  let filerTeam = req.userTeam || null;
  const batchId = isRange ? crypto.randomUUID() : null;

  try {
    if (req.userRole === 'admin' || req.userRole === 'pm') {
      const resolved = await resolveRegisteredPtoUser(user_name, user_upn);
      if (resolved.error) {
        return res.status(400).json({ ok: false, error: resolved.error });
      }
      user_upn = resolved.user.email;
      user_name = resolved.user.name;
      filerTeam = resolved.user.team || null;
    }
    filerTeam = await resolvePtoFilerTeam(user_upn, filerTeam);

    let savedRows;

    if (isRange) {
      // Insert all weekday rows in a single transaction
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const insertedRows = [];
        for (const d of weekdays) {
          const r = await client.query(
            `INSERT INTO public.pto_entries
               (user_upn, user_name, entry_date, hours, day_part, leave_type, notes, status, filer_role, filer_team, batch_id)
             VALUES ($1, $2, $3::date, $4, $5, $6, $7, $8, $9, $10, $11)
             RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes, status, filer_role, filer_team, batch_id`,
            [
              user_upn,
              user_name,
              d,
              hours,
              day_part,
              leave_type,
              notes,
              initialStatus,
              filerRole,
              filerTeam,
              batchId,
            ],
          );
          insertedRows.push(r.rows[0]);
        }
        await client.query('COMMIT');
        savedRows = insertedRows;
      } catch (txErr) {
        await client.query('ROLLBACK');
        throw txErr;
      } finally {
        client.release();
      }
    } else {
      // Single-date: existing behaviour, batch_id = NULL
      const r = await pool.query(
        `INSERT INTO public.pto_entries
           (user_upn, user_name, entry_date, hours, day_part, leave_type, notes, status, filer_role, filer_team)
         VALUES ($1, $2, $3::date, $4, $5, $6, $7, $8, $9, $10)
         RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes, status, filer_role, filer_team`,
        [
          user_upn,
          user_name,
          dateFrom,
          hours,
          day_part,
          leave_type,
          notes,
          initialStatus,
          filerRole,
          filerTeam,
        ],
      );
      savedRows = [r.rows[0]];
    }

    const savedRow = savedRows[0];
    const totalHours = hours * savedRows.length;

    // Send email (async, non-blocking to response):
    //   - pending:  approval notification with PDF attached → to approvers, CC filer
    //   - approved: auto-approve receipt with PDF attached  → to filer only
    if (BREVO_API_KEY && NOTIFY_FROM_EMAIL) {
      (async () => {
        try {
          const submittedAt = new Date().toUTCString();
          const pdfBuf = await generatePtoPdf({
            userName: user_name || user_upn,
            entryDate: dateFrom,
            entryDateTo: isRange ? dateTo : null,
            totalDays: savedRows.length,
            hours,
            dayPart: day_part,
            leaveType: leave_type,
            notes: notes || '',
            submittedAt,
          });
          const pdfAttachment = {
            filename: buildPtoPdfFilename(user_name || user_upn, submittedAt),
            content: pdfBuf,
            contentType: 'application/pdf',
          };
          const transporter = createMailTransporter();

          // Human-readable date label for email body
          const _fmtDateLabel = (() => {
            if (isRange) {
              const f = new Date(dateFrom + 'T00:00:00');
              const t = new Date(dateTo + 'T00:00:00');
              const fmtShort = (d) =>
                d.toLocaleDateString('en-US', {
                  month: 'short',
                  day: 'numeric',
                  year: 'numeric',
                });
              return `${fmtShort(f)} \u2013 ${fmtShort(t)} (${savedRows.length} day${savedRows.length !== 1 ? 's' : ''}, ${fmtH(totalHours)} hrs)`;
            }
            const d = new Date(dateFrom + 'T00:00:00');
            const day = String(d.getDate()).padStart(2, '0');
            const month = d.toLocaleDateString('en-US', { month: 'long' });
            const year = d.getFullYear();
            const weekday = d.toLocaleDateString('en-US', { weekday: 'long' });
            return `${day} ${month} ${year}, ${weekday}`;
          })();

          if (initialStatus === 'pending') {
            // Approval notification with PDF → approvers + CC filer
            const approverEmails = await getApproverEmails(
              filerRole,
              filerTeam,
              req.userEmail,
            );
            if (approverEmails.length) {
              const dateLabel = fmtSubjectDateRange(
                dateFrom,
                isRange ? dateTo : dateFrom,
              );
              // Pre-generate Message-ID; also build Thread-Index for Outlook conversation grouping
              // (Brevo rewrites Message-Id on delivery, but Thread-Topic/Thread-Index are preserved)
              const generatedUuid = crypto.randomUUID();
              const generatedMsgId = `<${generatedUuid}@tfs-hours>`;
              const threadTopic = `LEAVE REQUEST \u2013 ${user_name || user_upn} \u2013 ${leave_type} Leave on ${dateLabel}`;
              await transporter.sendMail({
                from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
                messageId: generatedMsgId,
                to: approverEmails.join(', '),
                cc: req.userEmail,
                subject: `LEAVE REQUEST \u2013 ${user_name || user_upn} \u2013 ${leave_type} Leave on ${dateLabel}`,
                headers: {
                  'Thread-Topic': threadTopic,
                  'Thread-Index': buildThreadIndex(generatedUuid),
                },
                html: `<p>Hi @Team,</p>
<p><strong>${escapeEmailHtml(user_name || user_upn)}</strong> has filed a Leave Request and it needs your approval.</p>
<p style="font-family:sans-serif;font-size:13px;line-height:1.8">
  <strong>Leave Date:</strong> ${escapeEmailHtml(_fmtDateLabel)}<br>
  <strong>Leave Duration:</strong> ${fmtH(totalHours)} hrs<br>
  ${buildPtoDayPartHtml(day_part)}
  <strong>Leave Type:</strong> ${escapeEmailHtml(leave_type)}<br>
  <strong>Reason for Leave:</strong> ${escapeEmailHtml(notes || '—')}
</p>
<p>Please see the attached Leave Request form for reference.</p>
<p>Thank you for your review.</p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report. Please do not reply to this email.</p>`,
                text: `Hi @Team,\n\n${user_name || user_upn} has filed a Leave Request and it needs your approval.\n\nLeave Date: ${_fmtDateLabel}\nLeave Duration: ${fmtH(totalHours)} hrs${buildPtoDayPartText(day_part)}\nLeave Type: ${leave_type}\nReason for Leave: ${notes || '—'}\n\nPlease see the attached Leave Request form for reference.\n\nThank you for your review.\n\n---\nAutomated message — TFS Hours Report. Please do not reply to this email.`,
                attachments: [pdfAttachment],
              });
              // Store the pre-generated message-id on all rows for reply threading
              if (batchId) {
                await pool.query(
                  `UPDATE public.pto_entries SET email_message_id = $1 WHERE batch_id = $2`,
                  [generatedMsgId, batchId],
                );
              } else {
                await pool.query(
                  `UPDATE public.pto_entries SET email_message_id = $1 WHERE id = $2`,
                  [generatedMsgId, savedRow.id],
                );
              }
            }
          } else {
            // Auto-approved (admin or PTO_APPROVAL_ENABLED=false) → receipt to filer only
            const dateLabel = fmtSubjectDateRange(
              dateFrom,
              isRange ? dateTo : dateFrom,
            );
            await transporter.sendMail({
              from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
              to: req.userEmail,
              subject: `PTO Approved \u2013 ${user_name || user_upn} \u2013 ${dateLabel}`,
              html: `<p>Hi ${escapeEmailHtml(user_name || user_upn)},</p>
<p>Your Leave Request for <strong>${escapeEmailHtml(_fmtDateLabel)}</strong> has been <strong>automatically approved</strong>.</p>
<p style="font-family:sans-serif;font-size:13px;line-height:1.8">
  <strong>Leave Duration:</strong> ${fmtH(totalHours)} hrs<br>
  ${buildPtoDayPartHtml(day_part)}
  <strong>Leave Type:</strong> ${escapeEmailHtml(leave_type)}<br>
  <strong>Reason for Leave:</strong> ${escapeEmailHtml(notes || '—')}
</p>
<p>Please see the attached Leave Request form for your records.</p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report. Please do not reply to this email.</p>`,
              text: `Hi ${user_name || user_upn},\n\nYour Leave Request for ${_fmtDateLabel} has been automatically approved.\n\nLeave Duration: ${fmtH(totalHours)} hrs${buildPtoDayPartText(day_part)}\nLeave Type: ${leave_type}\nReason for Leave: ${notes || '—'}\n\n---\nAutomated message — TFS Hours Report. Please do not reply to this email.`,
              attachments: [pdfAttachment],
            });
          }
        } catch (emailErr) {
          console.error('PTO email error:', emailErr);
        }
      })();
    }

    res.status(201).json({
      ok: true,
      row: savedRow,
      rows: savedRows,
      batchId,
      submissionStatus: savedRow.status || initialStatus,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- PTO Batch Approval / Denial / Delete ----------
// NOTE: these routes MUST remain above /api/pto/:id/approve|deny|DELETE so Express
// does not treat "batch" as an :id value.

app.patch(
  '/api/pto/batch/:batchId/approve',
  requireAuth,
  requireLeadOrPm,
  async (req, res) => {
    if (!PTO_APPROVAL_ENABLED)
      return res
        .status(503)
        .json({ ok: false, error: 'approval workflow is disabled' });
    const { batchId } = req.params;
    if (!batchId)
      return res.status(400).json({ ok: false, error: 'invalid batchId' });
    try {
      const batchRes = await pool.query(
        `SELECT p.id, p.user_upn, p.user_name, p.entry_date::text, p.hours, p.leave_type,
                p.day_part, p.status, p.filer_role, COALESCE(p.filer_team, u.team) AS filer_team,
                p.approved_by_lead, p.lead_actioned_at, p.batch_id,
                p.email_message_id, p.notes, p.created_at
         FROM public.pto_entries p
         LEFT JOIN public.users u ON LOWER(u.email) = LOWER(p.user_upn)
         WHERE p.batch_id = $1
         ORDER BY p.entry_date ASC`,
        [batchId],
      );
      if (!batchRes.rows.length)
        return res.status(404).json({ ok: false, error: 'batch not found' });
      const batchError = validatePtoBatchTransitionRows(batchRes.rows);
      if (batchError)
        return res
          .status(batchError.status)
          .json({ ok: false, error: batchError.error });

      const entry = batchRes.rows[0]; // use first row for access checks
      const accessError = checkApprovalAccess(
        entry,
        req.userRole,
        req.userEmail,
        req.userTeam,
      );
      if (accessError)
        return res.status(403).json({ ok: false, error: accessError });

      const transition = getInternalApprovalTransition(entry, {
        role: req.userRole,
        email: req.userEmail,
        team: req.userTeam,
      });
      const externalRecipients =
        SPECIAL_PTO_EXTERNAL_APPROVER_EMAILS.join(', ');
      let updatedRows;
      if (transition.approvalStage === 'lead') {
        const r = await pool.query(
          transition.notification === 'external_request'
            ? `UPDATE public.pto_entries
           SET status = $1, approved_by_lead = $2, lead_actioned_at = NOW(),
               external_requested_at = NOW(), external_requested_by = $2, external_request_recipients = $3
           WHERE batch_id = $4
           RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes,
                     status, filer_role, filer_team, email_message_id, batch_id`
            : `UPDATE public.pto_entries
           SET status = $1, approved_by_lead = $2, lead_actioned_at = NOW()
           WHERE batch_id = $3
           RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes,
                     status, filer_role, filer_team, email_message_id, batch_id`,
          transition.notification === 'external_request'
            ? [
                transition.nextStatus,
                req.userEmail,
                externalRecipients,
                batchId,
              ]
            : [transition.nextStatus, req.userEmail, batchId],
        );
        updatedRows = r.rows;
      } else {
        const r = await pool.query(
          transition.notification === 'external_request'
            ? `UPDATE public.pto_entries
           SET status = $1, approved_by_pm = $2, pm_actioned_at = NOW(),
               external_requested_at = NOW(), external_requested_by = $2, external_request_recipients = $3
           WHERE batch_id = $4
           RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes,
                     status, filer_role, filer_team, email_message_id, batch_id`
            : `UPDATE public.pto_entries
           SET status = $1, approved_by_pm = $2, pm_actioned_at = NOW()
           WHERE batch_id = $3
           RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes,
                     status, filer_role, filer_team, email_message_id, batch_id`,
          transition.notification === 'external_request'
            ? [
                transition.nextStatus,
                req.userEmail,
                externalRecipients,
                batchId,
              ]
            : [transition.nextStatus, req.userEmail, batchId],
        );
        updatedRows = r.rows;
      }

      // Send one notification email covering the whole range (non-blocking)
      if (BREVO_API_KEY && NOTIFY_FROM_EMAIL) {
        (async () => {
          try {
            const transporter = createMailTransporter();
            const _batchDateRange = fmtSubjectDateRange(
              batchRes.rows[0].entry_date,
              batchRes.rows[batchRes.rows.length - 1].entry_date,
            );
            const _batchReplyThreadIdx = buildReplyThreadIndex(
              entry.email_message_id,
            );
            const replyHeaders = {
              ...(entry.email_message_id
                ? {
                    'In-Reply-To': entry.email_message_id,
                    References: entry.email_message_id,
                  }
                : {}),
              ...(_batchReplyThreadIdx
                ? {
                    'Thread-Topic': `LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${_batchDateRange}`,
                    'Thread-Index': _batchReplyThreadIdx,
                  }
                : {}),
            };
            const displayName = escapeEmailHtml(
              entry.user_name || entry.user_upn,
            );
            const dateRange = _batchDateRange;

            if (transition.notification === 'external_request') {
              await sendSpecialExternalApprovalRequestEmail(
                entry,
                batchRes.rows,
                req.userName || req.userEmail,
                getRequestBaseUrl(req),
              );
            } else if (transition.notification === 'lead_approved') {
              const pmEmails = await getApproverEmails('lead', null, null);
              const toList = [...new Set([...pmEmails, entry.user_upn])].filter(
                Boolean,
              );
              if (toList.length) {
                await transporter.sendMail({
                  from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
                  to: toList.join(', '),
                  subject: `Re: LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${dateRange}`,
                  html: `<p>Hi @Team,</p><p>The PTO request for <strong>${displayName}</strong> (${escapeEmailHtml(dateRange)}) has been <strong>approved by ${escapeEmailHtml(req.userName || req.userEmail)}</strong> (lead).</p>
<p><strong>Leave Duration:</strong> ${fmtH(batchRes.rows.length * Number(entry.hours || 0))} hrs<br>${buildPtoDayPartHtml(entry.day_part)}<strong>Leave Type:</strong> ${escapeEmailHtml(entry.leave_type)}</p>
<p>This request is still pending final Manager's review and approval.</p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report. Please do not reply to this email.</p>`,
                  text: `PTO for ${entry.user_name || entry.user_upn} (${dateRange}) approved by lead ${req.userName || req.userEmail}.\nLeave Duration: ${fmtH(batchRes.rows.length * Number(entry.hours || 0))} hrs${buildPtoDayPartText(entry.day_part)}\nLeave Type: ${entry.leave_type}\nAwaiting PM final approval.\n\n---\nAutomated message — TFS Hours Report. Please do not reply to this email.`,
                  headers: replyHeaders,
                });
              }
            } else {
              let leadEmails = [];
              const leadApprovalDetails = await buildLeadApprovalEmailDetails(
                entry.approved_by_lead,
                entry.lead_actioned_at,
              );
              if (
                ['dev', 'qa'].includes(entry.filer_role) ||
                entry.filer_role === 'lead'
              ) {
                const filerUser = await pool.query(
                  `SELECT team FROM public.users WHERE LOWER(email) = LOWER($1)`,
                  [entry.user_upn],
                );
                const filerTeam = filerUser.rows[0]?.team || null;
                leadEmails = await getApproverEmails('dev', filerTeam, null);
              }
              const toList = [
                ...new Set([entry.user_upn, ...leadEmails]),
              ].filter(Boolean);
              if (toList.length) {
                // Regenerate PDF for attachment (totalDays = batch row count)
                let pmBatchAttachments = [];
                try {
                  const totalDays = batchRes.rows.length;
                  const submittedAt = entry.created_at
                    ? new Date(entry.created_at).toUTCString()
                    : new Date().toUTCString();
                  const pdfBuf = await generatePtoPdf({
                    userName: entry.user_name || entry.user_upn,
                    entryDate: batchRes.rows[0].entry_date,
                    entryDateTo:
                      totalDays > 1
                        ? batchRes.rows[batchRes.rows.length - 1].entry_date
                        : null,
                    totalDays,
                    hours: entry.hours,
                    dayPart: entry.day_part,
                    leaveType: entry.leave_type,
                    notes: entry.notes || '',
                    submittedAt,
                  });
                  pmBatchAttachments = [
                    {
                      filename: buildPtoPdfFilename(
                        entry.user_name || entry.user_upn,
                        submittedAt,
                      ),
                      content: pdfBuf,
                      contentType: 'application/pdf',
                    },
                  ];
                } catch (pdfErr) {
                  console.error('PM batch approval PDF error:', pdfErr);
                }
                const ccList = EXTRA_CC_EMAILS.length ? EXTRA_CC_EMAILS : [];
                await transporter.sendMail({
                  from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
                  to: toList.join(', '),
                  ...(ccList.length ? { cc: ccList.join(', ') } : {}),
                  subject: `Re: LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${dateRange}`,
                  html: `<p>Hi @Team,</p><p>The PTO request for <strong>${displayName}</strong> (${escapeEmailHtml(dateRange)}) has been <strong>fully approved by ${escapeEmailHtml(req.userName || req.userEmail)}</strong>.</p>
${leadApprovalDetails.html}
<p><strong>Leave Duration:</strong> ${fmtH(batchRes.rows.length * Number(entry.hours || 0))} hrs<br>${buildPtoDayPartHtml(entry.day_part)}<strong>Leave Type:</strong> ${escapeEmailHtml(entry.leave_type)}</p>
<p>The approved request has been added to the team calendar.</p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report. </p>`,
                  text: `PTO for ${entry.user_name || entry.user_upn} (${dateRange}) fully approved by ${req.userName || req.userEmail}.${leadApprovalDetails.text}\nLeave Duration: ${fmtH(batchRes.rows.length * Number(entry.hours || 0))} hrs${buildPtoDayPartText(entry.day_part)}\nLeave Type: ${entry.leave_type}.\n\n---\nAutomated message — TFS Hours Report.`,
                  headers: replyHeaders,
                  attachments: pmBatchAttachments,
                });
              }
            }
          } catch (emailErr) {
            console.error('PTO batch approval email error:', emailErr);
          }
        })();
      }

      res.json({ ok: true, rows: updatedRows });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  },
);

app.patch(
  '/api/pto/batch/:batchId/deny',
  requireAuth,
  requireLeadOrPm,
  async (req, res) => {
    if (!PTO_APPROVAL_ENABLED)
      return res
        .status(503)
        .json({ ok: false, error: 'approval workflow is disabled' });
    const { batchId } = req.params;
    if (!batchId)
      return res.status(400).json({ ok: false, error: 'invalid batchId' });
    const denialNote = normText(req.body?.note) || null;
    try {
      const batchRes = await pool.query(
        `SELECT p.id, p.user_upn, p.user_name, p.entry_date::text, p.hours, p.leave_type,
                p.day_part, p.status, p.filer_role, COALESCE(p.filer_team, u.team) AS filer_team,
                p.email_message_id
         FROM public.pto_entries p
         LEFT JOIN public.users u ON LOWER(u.email) = LOWER(p.user_upn)
         WHERE p.batch_id = $1
         ORDER BY p.entry_date ASC`,
        [batchId],
      );
      if (!batchRes.rows.length)
        return res.status(404).json({ ok: false, error: 'batch not found' });
      const batchError = validatePtoBatchTransitionRows(batchRes.rows);
      if (batchError)
        return res
          .status(batchError.status)
          .json({ ok: false, error: batchError.error });

      const entry = batchRes.rows[0];
      const accessError = checkApprovalAccess(
        entry,
        req.userRole,
        req.userEmail,
        req.userTeam,
        'deny',
      );
      if (accessError)
        return res.status(403).json({ ok: false, error: accessError });

      const r = await pool.query(
        `UPDATE public.pto_entries
         SET status = 'denied', denied_by = $1, denied_at = NOW(), denial_note = $2
         WHERE batch_id = $3
         RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes,
                   status, filer_role, filer_team, email_message_id, denied_by, denial_note, batch_id`,
        [req.userEmail, denialNote, batchId],
      );
      const updatedRows = r.rows;

      // One denial email covering the whole range (non-blocking)
      if (BREVO_API_KEY && NOTIFY_FROM_EMAIL) {
        (async () => {
          try {
            const transporter = createMailTransporter();
            const _batchDenyDateRange = fmtSubjectDateRange(
              batchRes.rows[0].entry_date,
              batchRes.rows[batchRes.rows.length - 1].entry_date,
            );
            const _batchDenyReplyThreadIdx = buildReplyThreadIndex(
              entry.email_message_id,
            );
            const replyHeaders = {
              ...(entry.email_message_id
                ? {
                    'In-Reply-To': entry.email_message_id,
                    References: entry.email_message_id,
                  }
                : {}),
              ...(_batchDenyReplyThreadIdx
                ? {
                    'Thread-Topic': `LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${_batchDenyDateRange}`,
                    'Thread-Index': _batchDenyReplyThreadIdx,
                  }
                : {}),
            };
            const dateRange = _batchDenyDateRange;
            const approverEmails = await getApproverEmails(
              entry.filer_role,
              entry.filer_team,
              entry.user_upn,
            );
            const ccEmails = approverEmails.filter(
              (e) => e.toLowerCase() !== req.userEmail.toLowerCase(),
            );
            await transporter.sendMail({
              from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
              to: entry.user_upn,
              ...(ccEmails.length ? { cc: ccEmails.join(', ') } : {}),
              subject: `Re: LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${dateRange}`,
              html: `<p>Hi @Team,</p><p>The PTO request for <strong>${entry.user_name || entry.user_upn}</strong> on <strong>${escapeEmailHtml(dateRange)}</strong> has been <strong>denied by ${escapeEmailHtml(req.userName || req.userEmail)}</strong>.</p>
<p><strong>Leave Duration:</strong> ${fmtH(batchRes.rows.length * Number(entry.hours || 0))} hrs<br>${buildPtoDayPartHtml(entry.day_part)}<strong>Leave Type:</strong> ${escapeEmailHtml(entry.leave_type)}</p>
${denialNote ? `<p><strong>Reason:</strong> ${escapeEmailHtml(denialNote)}</p>` : ''}
<p>You may resubmit a new PTO request if needed.</p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report. Please do not reply to this email.</p>`,
              text: `Your PTO for ${dateRange} was denied by ${req.userName || req.userEmail}.\nLeave Duration: ${fmtH(batchRes.rows.length * Number(entry.hours || 0))} hrs${buildPtoDayPartText(entry.day_part)}\nLeave Type: ${entry.leave_type}.${denialNote ? ' Reason: ' + denialNote : ''}\n\n---\nAutomated message — TFS Hours Report. Please do not reply to this email.`,
              headers: replyHeaders,
            });
          } catch (emailErr) {
            console.error('PTO batch denial email error:', emailErr);
          }
        })();
      }

      res.json({ ok: true, rows: updatedRows });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  },
);

app.patch(
  '/api/pto/batch/:batchId/external-approve',
  requireAuth,
  requireLeadOrPm,
  async (req, res) => {
    if (!PTO_APPROVAL_ENABLED)
      return res
        .status(503)
        .json({ ok: false, error: 'approval workflow is disabled' });
    const { batchId } = req.params;
    if (!batchId)
      return res.status(400).json({ ok: false, error: 'invalid batchId' });
    const note = normText(req.body?.note) || null;

    try {
      const batchRes = await pool.query(
        `SELECT p.id, p.user_upn, p.user_name, p.entry_date::text, p.hours, p.leave_type,
                p.day_part, p.status, p.filer_role, COALESCE(p.filer_team, u.team) AS filer_team,
                p.approved_by_lead, p.lead_actioned_at, p.approved_by_pm, p.pm_actioned_at,
                p.email_message_id, p.notes, p.created_at
         FROM public.pto_entries p
         LEFT JOIN public.users u ON LOWER(u.email) = LOWER(p.user_upn)
         WHERE p.batch_id = $1
         ORDER BY p.entry_date ASC`,
        [batchId],
      );
      if (!batchRes.rows.length)
        return res.status(404).json({ ok: false, error: 'batch not found' });
      const batchError = validatePtoBatchTransitionRows(batchRes.rows);
      if (batchError)
        return res
          .status(batchError.status)
          .json({ ok: false, error: batchError.error });

      const entry = batchRes.rows[0];
      const transition = getExternalReceivedTransition(entry, {
        role: req.userRole,
        email: req.userEmail,
        team: req.userTeam,
      });
      if (transition.error)
        return res.status(403).json({ ok: false, error: transition.error });

      const r = await pool.query(
        `UPDATE public.pto_entries
         SET status = $1,
             external_received_at = NOW(),
             external_received_by = $2,
             external_received_note = $3
         WHERE batch_id = $4
         RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes,
                   status, filer_role, filer_team, email_message_id, batch_id,
                   external_received_at, external_received_by, external_received_note`,
        [transition.nextStatus, req.userEmail, note, batchId],
      );
      const updatedRows = r.rows;

      if (BREVO_API_KEY && NOTIFY_FROM_EMAIL) {
        (async () => {
          try {
            if (transition.notification === 'lead_approved') {
              await sendSpecialExternalReceivedLeadApprovedEmail(
                entry,
                batchRes.rows,
                req.userName || req.userEmail,
              );
            } else if (transition.notification === 'final_approved') {
              await sendPtoFinalApprovalEmail(
                entry,
                batchRes.rows,
                req.userName || req.userEmail,
              );
            }
          } catch (emailErr) {
            console.error('PTO batch external approval email error:', emailErr);
          }
        })();
      }

      res.json({ ok: true, rows: updatedRows });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  },
);

app.patch('/api/pto/batch/:batchId/cancel', requireAuth, async (req, res) => {
  const { batchId } = req.params;
  if (!batchId)
    return res.status(400).json({ ok: false, error: 'invalid batchId' });
  const cancelNote = normText(req.body?.note) || null;
  if (!cancelNote)
    return res
      .status(400)
      .json({ ok: false, error: 'cancellation reason is required' });

  try {
    const batchRes = await pool.query(
      `SELECT p.id, p.user_upn, p.user_name, p.entry_date::text, p.hours, p.leave_type,
              p.day_part, p.status, p.filer_role, p.email_message_id, p.notes, p.created_at,
              COALESCE(p.filer_team, u.team) AS filer_team
       FROM public.pto_entries p
       LEFT JOIN public.users u ON LOWER(u.email) = LOWER(p.user_upn)
       WHERE p.batch_id = $1
       ORDER BY p.entry_date ASC`,
      [batchId],
    );
    if (!batchRes.rows.length)
      return res.status(404).json({ ok: false, error: 'batch not found' });

    const cancelAccess = checkCancelAccess(
      batchRes.rows,
      req.userRole,
      req.userEmail,
    );
    if (cancelAccess)
      return res
        .status(cancelAccess.status)
        .json({ ok: false, error: cancelAccess.error });

    const entry = batchRes.rows[0];

    const r = await pool.query(
      `UPDATE public.pto_entries
       SET status = 'cancelled', cancelled_by = $1, cancelled_at = NOW(), cancel_note = $2
       WHERE batch_id = $3
       RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes,
                 status, filer_role, email_message_id, cancelled_by, cancel_note, batch_id`,
      [req.userEmail, cancelNote, batchId],
    );
    const updatedRows = r.rows;

    // One cancellation email covering the whole range (non-blocking)
    if (BREVO_API_KEY && NOTIFY_FROM_EMAIL) {
      (async () => {
        try {
          const transporter = createMailTransporter();
          const batchDateRange = fmtSubjectDateRange(
            batchRes.rows[0].entry_date,
            batchRes.rows[batchRes.rows.length - 1].entry_date,
          );
          const batchReplyThreadIdx = buildReplyThreadIndex(
            entry.email_message_id,
          );
          const replyHeaders = {
            ...(entry.email_message_id
              ? {
                  'In-Reply-To': entry.email_message_id,
                  References: entry.email_message_id,
                }
              : {}),
            ...(batchReplyThreadIdx
              ? {
                  'Thread-Topic': `LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${batchDateRange}`,
                  'Thread-Index': batchReplyThreadIdx,
                }
              : {}),
          };
          const approverEmails = await getApproverEmails(
            entry.filer_role,
            entry.filer_team,
            entry.user_upn,
          );
          const toList = approverEmails.filter(
            (e) => e.toLowerCase() !== req.userEmail.toLowerCase(),
          );
          if (toList.length) {
            await transporter.sendMail({
              from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
              to: toList.join(', '),
              subject: `Re: LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${batchDateRange}`,
              html: `<p>Hi @Team,</p><p>The PTO request for <strong>${escapeEmailHtml(entry.user_name || entry.user_upn)}</strong> on <strong>${escapeEmailHtml(batchDateRange)}</strong> has been <strong>cancelled by ${escapeEmailHtml(req.userName || req.userEmail)}</strong>.</p>
<p><strong>Leave Duration:</strong> ${fmtH(batchRes.rows.length * Number(entry.hours || 0))} hrs<br>${buildPtoDayPartHtml(entry.day_part)}<strong>Leave Type:</strong> ${escapeEmailHtml(entry.leave_type)}</p>
${cancelNote ? `<p><strong>Reason:</strong> ${escapeEmailHtml(cancelNote)}</p>` : ''}
<p>No further action is needed.</p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report. Please do not reply to this email.</p>`,
              text: `PTO for ${entry.user_name || entry.user_upn} on ${batchDateRange} has been cancelled by ${req.userName || req.userEmail}.\nLeave Duration: ${fmtH(batchRes.rows.length * Number(entry.hours || 0))} hrs${buildPtoDayPartText(entry.day_part)}\nLeave Type: ${entry.leave_type}.${cancelNote ? ' Reason: ' + cancelNote : ''}\n\n---\nAutomated message — TFS Hours Report. Please do not reply to this email.`,
              headers: replyHeaders,
            });
          }
        } catch (emailErr) {
          console.error('PTO batch cancel email error:', emailErr);
        }
      })();
    }

    res.json({ ok: true, rows: updatedRows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.delete('/api/pto/batch/:batchId', requireAuth, async (req, res) => {
  const { batchId } = req.params;
  if (!batchId)
    return res.status(400).json({ ok: false, error: 'invalid batchId' });
  try {
    // Own-data roles: dev, qa — verify all batch rows belong to them
    if (OWN_DATA_ROLES.has(req.userRole)) {
      const ownerCheck = await pool.query(
        `SELECT COUNT(*) AS total,
                COUNT(*) FILTER (WHERE LOWER(COALESCE(user_upn,'')) = LOWER($2)) AS owned
         FROM public.pto_entries WHERE batch_id = $1`,
        [batchId, req.userEmail],
      );
      const { total, owned } = ownerCheck.rows[0];
      if (Number(total) === 0)
        return res.status(404).json({ ok: false, error: 'batch not found' });
      if (Number(owned) !== Number(total))
        return res.status(403).json({ ok: false, error: 'forbidden' });
    }
    // Only allow deletion when ALL rows are pending
    const statusCheck = await pool.query(
      `SELECT COUNT(*) AS total,
              COUNT(*) FILTER (WHERE status = 'pending') AS pending_count
       FROM public.pto_entries WHERE batch_id = $1`,
      [batchId],
    );
    const { total, pending_count } = statusCheck.rows[0];
    if (Number(total) === 0)
      return res.status(404).json({ ok: false, error: 'batch not found' });
    if (Number(pending_count) !== Number(total))
      return res
        .status(400)
        .json({ ok: false, error: 'only pending PTO batches can be deleted' });

    await pool.query('DELETE FROM public.pto_entries WHERE batch_id = $1', [
      batchId,
    ]);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- PTO Approval / Denial ----------

const SPECIAL_PTO_WORKFLOW_ROLES = new Set(['qa', 'lead', 'pm']);

function hasSameKnownTeam(entry, actorTeam) {
  const actorTeamKey = normIdentity(actorTeam);
  if (!actorTeamKey) return true;
  return normIdentity(entry?.filer_team) === actorTeamKey;
}

function hasStrictSameTeam(entry, actorTeam) {
  const actorTeamKey = normIdentity(actorTeam);
  return !!actorTeamKey && normIdentity(entry?.filer_team) === actorTeamKey;
}

function isSpecialPtoWorkflow(entry) {
  const filerRole = normIdentity(entry?.filer_role);
  return (
    SPECIAL_PTO_WORKFLOW_ROLES.has(filerRole) &&
    isSpecialPtoTeamName(entry?.filer_team)
  );
}

function isCurrentPtoReadyForLead(entry) {
  return (
    ['dev', 'qa'].includes(entry?.filer_role) && entry?.status === 'pending'
  );
}

function isCurrentPtoReadyForPm(entry, actorEmail) {
  const { status, filer_role, user_upn } = entry || {};
  const filerEmailKey = normIdentity(user_upn);
  const actorEmailKey = normIdentity(actorEmail);
  const devQaReady =
    ['dev', 'qa'].includes(filer_role) && status === 'lead_approved';
  const leadReady = filer_role === 'lead' && status === 'pending';
  const pmReady =
    filer_role === 'pm' &&
    status === 'pending' &&
    filerEmailKey !== actorEmailKey;
  const tsReady = filer_role === 'ts' && status === 'pending';
  return devQaReady || leadReady || pmReady || tsReady;
}

function getDefaultInternalApprovalTransition(entry, actor) {
  const actorRole = actor?.role;
  const actorEmail = actor?.email;
  const actorTeam = actor?.team;
  const { status, filer_role } = entry || {};
  if (actorRole === 'lead') {
    if (!['dev', 'qa'].includes(filer_role))
      return { error: 'leads can only action dev/qa PTO' };
    if (!isCurrentPtoReadyForLead(entry))
      return { error: 'entry is not pending lead approval' };
    if (!hasSameKnownTeam(entry, actorTeam))
      return { error: 'filer is not in your team' };
    return {
      nextStatus: 'lead_approved',
      approvalStage: 'lead',
      notification: 'lead_approved',
    };
  }
  if (actorRole === 'pm') {
    if (!isCurrentPtoReadyForPm(entry, actorEmail))
      return { error: 'entry is not in an approvable state for this PM' };
    return {
      nextStatus: 'approved',
      approvalStage: 'pm',
      notification: 'final_approved',
    };
  }
  return { error: 'only lead or pm can approve/deny PTO' };
}

function getSpecialInternalApprovalTransition(entry, actor) {
  const { status, filer_role, user_upn } = entry || {};
  const actorRole = actor?.role;
  const actorEmailKey = normIdentity(actor?.email);
  const filerEmailKey = normIdentity(user_upn);

  if (filer_role === 'qa') {
    if (status === 'pending') {
      if (actorRole !== 'lead')
        return {
          error: 'special-team QA PTO requires same-team lead approval',
        };
      if (!hasStrictSameTeam(entry, actor?.team))
        return { error: 'filer is not in your team' };
      return {
        nextStatus: 'external_pending',
        approvalStage: 'lead',
        notification: 'external_request',
      };
    }
    if (status === 'lead_approved') {
      if (actorRole !== 'pm')
        return {
          error: 'special-team QA PTO requires same-team PM final approval',
        };
      if (!hasStrictSameTeam(entry, actor?.team))
        return { error: 'filer is not in your team' };
      return {
        nextStatus: 'approved',
        approvalStage: 'pm',
        notification: 'final_approved',
      };
    }
    return {
      error:
        status === 'external_pending'
          ? 'entry is awaiting external approval confirmation'
          : 'entry is not in an approvable state',
    };
  }

  if (filer_role === 'lead') {
    if (status !== 'pending')
      return {
        error:
          status === 'external_pending'
            ? 'entry is awaiting external approval confirmation'
            : 'entry is not pending PM approval',
      };
    if (actorRole !== 'pm')
      return { error: 'special-team lead PTO requires same-team PM approval' };
    if (!hasStrictSameTeam(entry, actor?.team))
      return { error: 'filer is not in your team' };
    return {
      nextStatus: 'external_pending',
      approvalStage: 'pm',
      notification: 'external_request',
    };
  }

  if (filer_role === 'pm') {
    if (status !== 'pending')
      return {
        error:
          status === 'external_pending'
            ? 'entry is awaiting external approval confirmation'
            : 'entry is not pending PM approval',
      };
    if (actorRole !== 'pm')
      return { error: 'special-team PM PTO requires same-team PM approval' };
    if (filerEmailKey === actorEmailKey)
      return { error: 'PMs cannot action their own PTO' };
    if (!hasStrictSameTeam(entry, actor?.team))
      return { error: 'filer is not in your team' };
    return {
      nextStatus: 'external_pending',
      approvalStage: 'pm',
      notification: 'external_request',
    };
  }

  return getDefaultInternalApprovalTransition(entry, actor);
}

function getInternalApprovalTransition(entry, actor) {
  if (isSpecialPtoWorkflow(entry))
    return getSpecialInternalApprovalTransition(entry, actor);
  return getDefaultInternalApprovalTransition(entry, actor);
}

function getExternalReceivedTransition(entry, actor) {
  const { status, filer_role, user_upn } = entry || {};
  const actorRole = actor?.role;
  const actorEmailKey = normIdentity(actor?.email);
  const filerEmailKey = normIdentity(user_upn);

  if (!isSpecialPtoWorkflow(entry))
    return { error: 'entry is not in the special PTO workflow' };
  if (status !== 'external_pending')
    return { error: 'entry is not awaiting external approval confirmation' };

  if (filer_role === 'qa') {
    if (actorRole !== 'lead')
      return {
        error: 'special-team QA PTO requires same-team lead confirmation',
      };
    if (!hasStrictSameTeam(entry, actor?.team))
      return { error: 'filer is not in your team' };
    return {
      nextStatus: 'lead_approved',
      notification: 'lead_approved',
    };
  }

  if (filer_role === 'lead') {
    if (actorRole !== 'pm')
      return {
        error: 'special-team lead PTO requires same-team PM confirmation',
      };
    if (!hasStrictSameTeam(entry, actor?.team))
      return { error: 'filer is not in your team' };
    return {
      nextStatus: 'approved',
      notification: 'final_approved',
    };
  }

  if (filer_role === 'pm') {
    if (actorRole !== 'pm')
      return {
        error: 'special-team PM PTO requires same-team PM confirmation',
      };
    if (filerEmailKey === actorEmailKey)
      return { error: 'PMs cannot action their own PTO' };
    if (!hasStrictSameTeam(entry, actor?.team))
      return { error: 'filer is not in your team' };
    return {
      nextStatus: 'approved',
      notification: 'final_approved',
    };
  }

  return { error: 'entry is not in the special PTO workflow' };
}

function canApprovePto(entry, actorRole, actorEmail, actorTeam) {
  const transition = getInternalApprovalTransition(entry, {
    role: actorRole,
    email: actorEmail,
    team: actorTeam,
  });
  return transition.error || null;
}

function canDenyPto(entry, actorRole, actorEmail, actorTeam) {
  const actor = { role: actorRole, email: actorEmail, team: actorTeam };
  if (!isSpecialPtoWorkflow(entry))
    return canApprovePto(entry, actorRole, actorEmail, actorTeam);

  if (entry?.status === 'external_pending') {
    const transition = getExternalReceivedTransition(entry, actor);
    return transition.error || null;
  }

  const transition = getSpecialInternalApprovalTransition(entry, actor);
  return transition.error || null;
}

function checkApprovalAccess(
  entry,
  actorRole,
  actorEmail,
  actorTeam,
  action = 'approve',
) {
  if (action === 'deny')
    return canDenyPto(entry, actorRole, actorEmail, actorTeam);
  return canApprovePto(entry, actorRole, actorEmail, actorTeam);
}

function validatePtoBatchTransitionRows(rows) {
  if (!Array.isArray(rows) || !rows.length)
    return { status: 404, error: 'batch not found' };
  const first = rows[0];
  const firstFiler = normIdentity(first.user_upn);
  const firstRole = String(first.filer_role || '');
  const firstStatus = String(first.status || '');
  const firstTeam = normIdentity(first.filer_team);
  const isMixed = rows.some(
    (row) =>
      normIdentity(row.user_upn) !== firstFiler ||
      String(row.filer_role || '') !== firstRole ||
      String(row.status || '') !== firstStatus ||
      normIdentity(row.filer_team) !== firstTeam,
  );
  if (isMixed)
    return { status: 400, error: 'batch contains mixed PTO approval states' };
  return null;
}

function currentPtoCancelYmd() {
  // Mirror the current client-side UTC-style comparison used for cancel visibility.
  return new Date().toISOString().slice(0, 10);
}

/**
 * Checks whether the requesting user is allowed to cancel a PTO entry or batch.
 * Returns null if allowed, or an object with status/error if not.
 */
function checkCancelAccess(entries, actorRole, actorEmail) {
  if (!Array.isArray(entries) || !entries.length)
    return { status: 404, error: 'not found' };

  const representative = entries[0];
  const actorEmailKey = String(actorEmail || '')
    .trim()
    .toLowerCase();
  const filerEmailKey = String(representative.user_upn || '')
    .trim()
    .toLowerCase();
  const isBatch = entries.length > 1;

  if (isBatch) {
    const hasMixedFilers = entries.some(
      (row) =>
        String(row.user_upn || '')
          .trim()
          .toLowerCase() !== filerEmailKey,
    );
    if (hasMixedFilers)
      return { status: 400, error: 'batch contains mixed filers' };
  }

  const blockedStatus = entries.find((row) =>
    ['cancelled', 'denied'].includes(row.status),
  )?.status;
  if (blockedStatus) {
    return isBatch
      ? {
          status: 400,
          error: `cannot cancel a batch containing ${blockedStatus} entries`,
        }
      : { status: 400, error: `cannot cancel a ${blockedStatus} entry` };
  }

  const effectiveDate = String(representative.entry_date || '');
  if (!effectiveDate || effectiveDate < currentPtoCancelYmd()) {
    return isBatch
      ? { status: 400, error: 'cannot cancel a past PTO batch' }
      : { status: 400, error: 'cannot cancel a past PTO entry' };
  }

  if (actorRole === 'admin' || actorRole === 'pm') return null;

  if (actorRole === 'lead' || OWN_DATA_ROLES.has(actorRole)) {
    if (filerEmailKey && filerEmailKey === actorEmailKey) return null;
    return { status: 403, error: 'forbidden' };
  }

  return { status: 403, error: 'forbidden' };
}

app.patch(
  '/api/pto/:id/external-approve',
  requireAuth,
  requireLeadOrPm,
  async (req, res) => {
    if (!PTO_APPROVAL_ENABLED)
      return res
        .status(503)
        .json({ ok: false, error: 'approval workflow is disabled' });
    const id = normInt(req.params.id);
    if (!id || id < 1)
      return res.status(400).json({ ok: false, error: 'invalid id' });
    const note = normText(req.body?.note) || null;

    try {
      const entryRes = await pool.query(
        `SELECT p.id, p.user_upn, p.user_name, p.entry_date::text, p.hours, p.leave_type,
                p.day_part, p.status, p.filer_role, COALESCE(p.filer_team, u.team) AS filer_team,
                p.approved_by_lead, p.lead_actioned_at, p.approved_by_pm, p.pm_actioned_at,
                p.email_message_id, p.batch_id, p.notes, p.created_at
         FROM public.pto_entries p
         LEFT JOIN public.users u ON LOWER(u.email) = LOWER(p.user_upn)
         WHERE p.id = $1`,
        [id],
      );
      if (!entryRes.rows.length)
        return res.status(404).json({ ok: false, error: 'not found' });
      const entry = entryRes.rows[0];

      const transition = getExternalReceivedTransition(entry, {
        role: req.userRole,
        email: req.userEmail,
        team: req.userTeam,
      });
      if (transition.error)
        return res.status(403).json({ ok: false, error: transition.error });

      const r = await pool.query(
        `UPDATE public.pto_entries
         SET status = $1,
             external_received_at = NOW(),
             external_received_by = $2,
             external_received_note = $3
         WHERE id = $4
         RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes,
                   status, filer_role, filer_team, email_message_id,
                   external_received_at, external_received_by, external_received_note`,
        [transition.nextStatus, req.userEmail, note, id],
      );
      const updatedRow = r.rows[0];

      if (BREVO_API_KEY && NOTIFY_FROM_EMAIL) {
        (async () => {
          try {
            if (transition.notification === 'lead_approved') {
              await sendSpecialExternalReceivedLeadApprovedEmail(
                entry,
                [entry],
                req.userName || req.userEmail,
              );
            } else if (transition.notification === 'final_approved') {
              await sendPtoFinalApprovalEmail(
                entry,
                [entry],
                req.userName || req.userEmail,
              );
            }
          } catch (emailErr) {
            console.error('PTO external approval email error:', emailErr);
          }
        })();
      }

      res.json({ ok: true, row: updatedRow });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  },
);

app.patch(
  '/api/pto/:id/approve',
  requireAuth,
  requireLeadOrPm,
  async (req, res) => {
    if (!PTO_APPROVAL_ENABLED)
      return res
        .status(503)
        .json({ ok: false, error: 'approval workflow is disabled' });
    const id = normInt(req.params.id);
    if (!id || id < 1)
      return res.status(400).json({ ok: false, error: 'invalid id' });
    try {
      const entryRes = await pool.query(
        `SELECT p.id, p.user_upn, p.user_name, p.entry_date::text, p.hours, p.leave_type,
              p.day_part, p.status, p.filer_role, COALESCE(p.filer_team, u.team) AS filer_team,
              p.approved_by_lead, p.lead_actioned_at,
              p.email_message_id, p.batch_id, p.notes, p.created_at
       FROM public.pto_entries p
       LEFT JOIN public.users u ON LOWER(u.email) = LOWER(p.user_upn)
       WHERE p.id = $1`,
        [id],
      );
      if (!entryRes.rows.length)
        return res.status(404).json({ ok: false, error: 'not found' });
      const entry = entryRes.rows[0];

      const accessError = checkApprovalAccess(
        entry,
        req.userRole,
        req.userEmail,
        req.userTeam,
      );
      if (accessError)
        return res.status(403).json({ ok: false, error: accessError });

      const transition = getInternalApprovalTransition(entry, {
        role: req.userRole,
        email: req.userEmail,
        team: req.userTeam,
      });
      const externalRecipients =
        SPECIAL_PTO_EXTERNAL_APPROVER_EMAILS.join(', ');
      let updatedRow;
      if (transition.approvalStage === 'lead') {
        const r = await pool.query(
          transition.notification === 'external_request'
            ? `UPDATE public.pto_entries
         SET status = $1, approved_by_lead = $2, lead_actioned_at = NOW(),
             external_requested_at = NOW(), external_requested_by = $2, external_request_recipients = $3
         WHERE id = $4
         RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes,
                   status, filer_role, filer_team, email_message_id`
            : `UPDATE public.pto_entries
         SET status = $1, approved_by_lead = $2, lead_actioned_at = NOW()
         WHERE id = $3
         RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes,
                   status, filer_role, filer_team, email_message_id`,
          transition.notification === 'external_request'
            ? [transition.nextStatus, req.userEmail, externalRecipients, id]
            : [transition.nextStatus, req.userEmail, id],
        );
        updatedRow = r.rows[0];
      } else {
        // PM final approval
        const r = await pool.query(
          transition.notification === 'external_request'
            ? `UPDATE public.pto_entries
         SET status = $1, approved_by_pm = $2, pm_actioned_at = NOW(),
             external_requested_at = NOW(), external_requested_by = $2, external_request_recipients = $3
         WHERE id = $4
         RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes,
                   status, filer_role, filer_team, email_message_id`
            : `UPDATE public.pto_entries
         SET status = $1, approved_by_pm = $2, pm_actioned_at = NOW()
         WHERE id = $3
         RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes,
                   status, filer_role, filer_team, email_message_id`,
          transition.notification === 'external_request'
            ? [transition.nextStatus, req.userEmail, externalRecipients, id]
            : [transition.nextStatus, req.userEmail, id],
        );
        updatedRow = r.rows[0];
      }

      // Send notification emails (non-blocking)
      if (BREVO_API_KEY && NOTIFY_FROM_EMAIL) {
        (async () => {
          try {
            const transporter = createMailTransporter();
            const _approveReplyThreadIdx = buildReplyThreadIndex(
              entry.email_message_id,
            );
            const replyHeaders = {
              ...(entry.email_message_id
                ? {
                    'In-Reply-To': entry.email_message_id,
                    References: entry.email_message_id,
                  }
                : {}),
              ...(_approveReplyThreadIdx
                ? {
                    'Thread-Topic': `LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${fmtSubjectDate(entry.entry_date)}`,
                    'Thread-Index': _approveReplyThreadIdx,
                  }
                : {}),
            };
            const filerEmail = entry.user_upn;
            const displayName = escapeEmailHtml(
              entry.user_name || entry.user_upn,
            );
            const entryDate = escapeEmailHtml(fmtSubjectDate(entry.entry_date));

            if (transition.notification === 'external_request') {
              await sendSpecialExternalApprovalRequestEmail(
                entry,
                [entry],
                req.userName || req.userEmail,
                getRequestBaseUrl(req),
              );
            } else if (transition.notification === 'lead_approved') {
              // Notify all PMs + filer
              const pmEmails = await getApproverEmails('lead', null, null);
              const toList = [...new Set([...pmEmails, filerEmail])].filter(
                Boolean,
              );
              if (toList.length) {
                await transporter.sendMail({
                  from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
                  to: toList.join(', '),
                  subject: `Re: LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${fmtSubjectDate(entry.entry_date)}`,
                  html: `<p>The PTO request for <strong>${displayName}</strong> on <strong>${entryDate}</strong> has been <strong>approved by ${escapeEmailHtml(req.userName || req.userEmail)}</strong> (lead).</p>
<p><strong>Leave Duration:</strong> ${fmtH(entry.hours)} hrs<br>${buildPtoDayPartHtml(entry.day_part)}<strong>Leave Type:</strong> ${escapeEmailHtml(entry.leave_type)}</p>
<p>This request is still pending final Manager's review and approval.</p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report. Please do not reply to this email.</p>`,
                  text: `PTO for ${entry.user_name || entry.user_upn} on ${fmtSubjectDate(entry.entry_date)} approved by lead ${req.userName || req.userEmail}.\nLeave Duration: ${fmtH(entry.hours)} hrs${buildPtoDayPartText(entry.day_part)}\nLeave Type: ${entry.leave_type}\nAwaiting PM final approval.\n\n---\nAutomated message — TFS Hours Report. Please do not reply to this email.`,
                  headers: replyHeaders,
                });
              }
            } else {
              // PM final approval: notify filer + leads in filer's team
              let leadEmails = [];
              const leadApprovalDetails = await buildLeadApprovalEmailDetails(
                entry.approved_by_lead,
                entry.lead_actioned_at,
              );
              if (
                ['dev', 'qa'].includes(entry.filer_role) ||
                entry.filer_role === 'lead'
              ) {
                // Find filer's team
                const filerUser = await pool.query(
                  `SELECT team FROM public.users WHERE LOWER(email) = LOWER($1)`,
                  [filerEmail],
                );
                const filerTeam = filerUser.rows[0]?.team || null;
                leadEmails = await getApproverEmails('dev', filerTeam, null);
              }
              const toList = [...new Set([filerEmail, ...leadEmails])].filter(
                Boolean,
              );
              if (toList.length) {
                // Regenerate PDF for attachment
                let pmApprovalAttachments = [];
                try {
                  const submittedAt = entry.created_at
                    ? new Date(entry.created_at).toUTCString()
                    : new Date().toUTCString();
                  const pdfBuf = await generatePtoPdf({
                    userName: entry.user_name || entry.user_upn,
                    entryDate: entry.entry_date,
                    entryDateTo: null,
                    totalDays: 1,
                    hours: entry.hours,
                    dayPart: entry.day_part,
                    leaveType: entry.leave_type,
                    notes: entry.notes || '',
                    submittedAt,
                  });
                  pmApprovalAttachments = [
                    {
                      filename: buildPtoPdfFilename(
                        entry.user_name || entry.user_upn,
                        submittedAt,
                      ),
                      content: pdfBuf,
                      contentType: 'application/pdf',
                    },
                  ];
                } catch (pdfErr) {
                  console.error('PM approval PDF error:', pdfErr);
                }
                const ccList = EXTRA_CC_EMAILS.length ? EXTRA_CC_EMAILS : [];
                await transporter.sendMail({
                  from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
                  to: toList.join(', '),
                  ...(ccList.length ? { cc: ccList.join(', ') } : {}),
                  subject: `Re: LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${fmtSubjectDate(entry.entry_date)}`,
                  html: `<p>Hi @Team,</p><p>The PTO request for <strong>${displayName}</strong> on <strong>${entryDate}</strong> has been <strong>fully approved by ${escapeEmailHtml(req.userName || req.userEmail)}</strong>.</p>
                  ${leadApprovalDetails.html}
                  <p><strong>Leave Duration:</strong> ${fmtH(entry.hours)} hrs<br>${buildPtoDayPartHtml(entry.day_part)}<strong>Leave Type:</strong> ${escapeEmailHtml(entry.leave_type)}</p>
                  <p>The approved request has been added to the team calendar.</p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report.</p>`,
                  text: `PTO for ${entry.user_name || entry.user_upn} on ${fmtSubjectDate(entry.entry_date)} fully approved by ${req.userName || req.userEmail}.${leadApprovalDetails.text}\nLeave Duration: ${fmtH(entry.hours)} hrs${buildPtoDayPartText(entry.day_part)}\nLeave Type: ${entry.leave_type}.\n\n---\nAutomated message — TFS Hours Report.`,
                  headers: replyHeaders,
                  attachments: pmApprovalAttachments,
                });
              }
            }
          } catch (emailErr) {
            console.error('PTO approval email error:', emailErr);
          }
        })();
      }

      res.json({ ok: true, row: updatedRow });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  },
);

app.patch(
  '/api/pto/:id/deny',
  requireAuth,
  requireLeadOrPm,
  async (req, res) => {
    if (!PTO_APPROVAL_ENABLED)
      return res
        .status(503)
        .json({ ok: false, error: 'approval workflow is disabled' });
    const id = normInt(req.params.id);
    if (!id || id < 1)
      return res.status(400).json({ ok: false, error: 'invalid id' });
    const denialNote = normText(req.body?.note) || null;
    try {
      const entryRes = await pool.query(
        `SELECT p.id, p.user_upn, p.user_name, p.entry_date::text, p.hours, p.leave_type,
              p.day_part, p.status, p.filer_role, COALESCE(p.filer_team, u.team) AS filer_team,
              p.email_message_id
       FROM public.pto_entries p
       LEFT JOIN public.users u ON LOWER(u.email) = LOWER(p.user_upn)
       WHERE p.id = $1`,
        [id],
      );
      if (!entryRes.rows.length)
        return res.status(404).json({ ok: false, error: 'not found' });
      const entry = entryRes.rows[0];

      const accessError = checkApprovalAccess(
        entry,
        req.userRole,
        req.userEmail,
        req.userTeam,
        'deny',
      );
      if (accessError)
        return res.status(403).json({ ok: false, error: accessError });

      const r = await pool.query(
        `UPDATE public.pto_entries
       SET status = 'denied', denied_by = $1, denied_at = NOW(), denial_note = $2
       WHERE id = $3
       RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes,
                 status, filer_role, filer_team, email_message_id, denied_by, denial_note`,
        [req.userEmail, denialNote, id],
      );
      const updatedRow = r.rows[0];

      // Notify filer of denial; CC approvers (non-blocking)
      if (BREVO_API_KEY && NOTIFY_FROM_EMAIL) {
        (async () => {
          try {
            const transporter = createMailTransporter();
            const _denyReplyThreadIdx = buildReplyThreadIndex(
              entry.email_message_id,
            );
            const replyHeaders = {
              ...(entry.email_message_id
                ? {
                    'In-Reply-To': entry.email_message_id,
                    References: entry.email_message_id,
                  }
                : {}),
              ...(_denyReplyThreadIdx
                ? {
                    'Thread-Topic': `LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${fmtSubjectDate(entry.entry_date)}`,
                    'Thread-Index': _denyReplyThreadIdx,
                  }
                : {}),
            };
            const approverEmails = await getApproverEmails(
              entry.filer_role,
              entry.filer_team,
              entry.user_upn,
            );
            // CC approvers excluding the actor (they already know they denied it)
            const ccEmails = approverEmails.filter(
              (e) => e.toLowerCase() !== req.userEmail.toLowerCase(),
            );
            await transporter.sendMail({
              from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
              to: entry.user_upn,
              ...(ccEmails.length ? { cc: ccEmails.join(', ') } : {}),
              subject: `Re: LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${fmtSubjectDate(entry.entry_date)}`,
              html: `<p>Your PTO request for <strong>${escapeEmailHtml(fmtSubjectDate(entry.entry_date))}</strong> has been <strong>denied</strong> by ${escapeEmailHtml(req.userName || req.userEmail)}.</p>
<p><strong>Leave Duration:</strong> ${fmtH(entry.hours)} hrs<br>${buildPtoDayPartHtml(entry.day_part)}<strong>Leave Type:</strong> ${escapeEmailHtml(entry.leave_type)}</p>
${denialNote ? `<p><strong>Reason:</strong> ${escapeEmailHtml(denialNote)}</p>` : ''}
<p>You may resubmit a new PTO request if needed.</p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report. Please do not reply to this email.</p>`,
              text: `Your PTO for ${fmtSubjectDate(entry.entry_date)} was denied by ${req.userName || req.userEmail}.\nLeave Duration: ${fmtH(entry.hours)} hrs${buildPtoDayPartText(entry.day_part)}\nLeave Type: ${entry.leave_type}.${denialNote ? ' Reason: ' + denialNote : ''}\n\n---\nAutomated message — TFS Hours Report. Please do not reply to this email.`,
              headers: replyHeaders,
            });
          } catch (emailErr) {
            console.error('PTO denial email error:', emailErr);
          }
        })();
      }

      res.json({ ok: true, row: updatedRow });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  },
);

// ---------- PTO Cancel ----------
app.patch('/api/pto/:id/cancel', requireAuth, async (req, res) => {
  const id = normInt(req.params.id);
  if (!id || id < 1)
    return res.status(400).json({ ok: false, error: 'invalid id' });
  const cancelNote = normText(req.body?.note) || null;
  if (!cancelNote)
    return res
      .status(400)
      .json({ ok: false, error: 'cancellation reason is required' });

  try {
    const entryRes = await pool.query(
      `SELECT p.id, p.user_upn, p.user_name, p.entry_date::text, p.hours, p.leave_type,
              p.day_part, p.status, p.filer_role, p.email_message_id,
              COALESCE(p.filer_team, u.team) AS filer_team
       FROM public.pto_entries p
       LEFT JOIN public.users u ON LOWER(u.email) = LOWER(p.user_upn)
       WHERE p.id = $1`,
      [id],
    );
    if (!entryRes.rows.length)
      return res.status(404).json({ ok: false, error: 'not found' });
    const entry = entryRes.rows[0];

    const cancelAccess = checkCancelAccess(
      [entry],
      req.userRole,
      req.userEmail,
    );
    if (cancelAccess)
      return res
        .status(cancelAccess.status)
        .json({ ok: false, error: cancelAccess.error });

    const r = await pool.query(
      `UPDATE public.pto_entries
       SET status = 'cancelled', cancelled_by = $1, cancelled_at = NOW(), cancel_note = $2
       WHERE id = $3
       RETURNING id, user_upn, user_name, entry_date::text, hours, day_part, leave_type, notes,
                 status, filer_role, email_message_id, cancelled_by, cancel_note`,
      [req.userEmail, cancelNote, id],
    );
    const updatedRow = r.rows[0];

    // Notify approvers of cancellation (non-blocking)
    if (BREVO_API_KEY && NOTIFY_FROM_EMAIL) {
      (async () => {
        try {
          const transporter = createMailTransporter();
          const _cancelReplyThreadIdx = buildReplyThreadIndex(
            entry.email_message_id,
          );
          const replyHeaders = {
            ...(entry.email_message_id
              ? {
                  'In-Reply-To': entry.email_message_id,
                  References: entry.email_message_id,
                }
              : {}),
            ...(_cancelReplyThreadIdx
              ? {
                  'Thread-Topic': `LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${fmtSubjectDate(entry.entry_date)}`,
                  'Thread-Index': _cancelReplyThreadIdx,
                }
              : {}),
          };
          const approverEmails = await getApproverEmails(
            entry.filer_role,
            entry.filer_team,
            entry.user_upn,
          );
          // Notify approvers only (filer initiated the cancel; they know)
          const toList = approverEmails.filter(
            (e) => e.toLowerCase() !== req.userEmail.toLowerCase(),
          );
          if (toList.length) {
            await transporter.sendMail({
              from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
              to: toList.join(', '),
              subject: `Re: LEAVE REQUEST \u2013 ${entry.user_name || entry.user_upn} \u2013 ${entry.leave_type} Leave on ${fmtSubjectDate(entry.entry_date)}`,
              html: `<p>Hi @Team,</p><p>The PTO request for <strong>${escapeEmailHtml(entry.user_name || entry.user_upn)}</strong> on <strong>${escapeEmailHtml(fmtSubjectDate(entry.entry_date))}</strong> has been <strong>cancelled by ${escapeEmailHtml(req.userName || req.userEmail)}</strong>.</p>
<p><strong>Leave Duration:</strong> ${fmtH(entry.hours)} hrs<br>${buildPtoDayPartHtml(entry.day_part)}<strong>Leave Type:</strong> ${escapeEmailHtml(entry.leave_type)}</p>
${cancelNote ? `<p><strong>Reason:</strong> ${escapeEmailHtml(cancelNote)}</p>` : ''}
<p>No further action is needed.</p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report. Please do not reply to this email.</p>`,
              text: `PTO for ${entry.user_name || entry.user_upn} on ${fmtSubjectDate(entry.entry_date)} has been cancelled by ${req.userName || req.userEmail}.\nLeave Duration: ${fmtH(entry.hours)} hrs${buildPtoDayPartText(entry.day_part)}\nLeave Type: ${entry.leave_type}.${cancelNote ? ' Reason: ' + cancelNote : ''}\n\n---\nAutomated message — TFS Hours Report. Please do not reply to this email.`,
              headers: replyHeaders,
            });
          }
        } catch (emailErr) {
          console.error('PTO cancel email error:', emailErr);
        }
      })();
    }

    res.json({ ok: true, row: updatedRow });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- PTO Delete ----------
app.delete('/api/pto/:id', requireAuth, async (req, res) => {
  const id = normInt(req.params.id);
  if (!id || id < 1)
    return res.status(400).json({ ok: false, error: 'invalid id' });
  try {
    // Own-data roles: dev, qa — can only delete own PTO entries
    if (OWN_DATA_ROLES.has(req.userRole)) {
      const check = await pool.query(
        `SELECT id FROM public.pto_entries WHERE id = $1
         AND (LOWER(COALESCE(user_upn,'')) = LOWER($2) OR LOWER(COALESCE(user_name,'')) = LOWER($3))`,
        [id, req.userEmail, req.userName || ''],
      );
      if (!check.rows.length)
        return res.status(403).json({ ok: false, error: 'forbidden' });
    }
    // Only allow hard delete of cancelled or denied entries
    const statusCheck = await pool.query(
      `SELECT id, status FROM public.pto_entries WHERE id = $1`,
      [id],
    );
    if (!statusCheck.rows.length)
      return res.status(404).json({ ok: false, error: 'not found' });
    const deletableStatuses = ['cancelled', 'denied'];
    if (!deletableStatuses.includes(statusCheck.rows[0].status))
      return res.status(400).json({
        ok: false,
        error: 'only cancelled or denied PTO entries can be deleted',
      });

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

// ---------- PTO PDF receipt ----------
function generatePtoPdf({
  userName,
  entryDate,
  entryDateTo, // null for single-day
  totalDays, // number of working days (for range)
  hours,
  dayPart,
  leaveType,
  notes,
  submittedAt,
}) {
  return new Promise((resolve, reject) => {
    const margin = 60;
    const doc = new PDFDocument({ size: 'LETTER', margin });
    const chunks = [];
    doc.on('data', (ch) => chunks.push(ch));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);

    const pageWidth = doc.page.width;

    // --- Logo (optional) ---
    const logoPath = path.join(__dirname, 'public', 'company-logo.png');
    if (fs.existsSync(logoPath)) {
      const logoW = 180;
      const logoX = (pageWidth - logoW) / 2;
      const logoY = doc.y;
      doc.image(logoPath, logoX, logoY, { fit: [logoW, 90] });
      // pdfkit does not advance doc.y when explicit x/y are given — move past image manually
      doc.y = logoY + 90 + 16;
    } else {
      doc.moveDown(0.5);
    }

    // --- Title ---
    doc
      .fontSize(13)
      .font('Helvetica-Bold')
      .fillColor('#000000')
      .text('APPLICATION FOR LEAVE OF ABSENCE', { align: 'center' });
    doc.moveDown(1.8);

    // --- Date formatters ---
    function fmtDate(ds) {
      const d = new Date(ds + 'T00:00:00');
      if (isNaN(d)) return String(ds);
      return (
        d.toLocaleDateString('en-US', {
          month: 'long',
          day: 'numeric',
          year: 'numeric',
        }) + ` (${REPORT_TZ_LABEL})`
      );
    }
    function fmtSubmitted(s) {
      const d = new Date(s);
      if (isNaN(d)) return String(s);
      return `${fmtReportCalendarDateFromTimestamp(d)} (${REPORT_TZ_LABEL || 'UTC'})`;
    }

    // --- Days / hours calculation ---
    const nDays = totalDays || 1;
    const totalHoursForPdf = Number(hours) * nDays;
    const rawDays = totalHoursForPdf / 8;
    const daysStr = Number.isInteger(rawDays)
      ? String(rawDays)
      : rawDays.toFixed(1);

    // --- Date of Leave(s) label ---
    const dateOfLeaveLabel =
      entryDateTo && entryDateTo !== entryDate
        ? `${fmtDate(entryDate)} \u2013 ${fmtDate(entryDateTo)}`
        : fmtDate(entryDate);

    // --- Fields ---
    const fields = [
      ['Employee Name', userName || '—'],
      ['Date Requested', fmtSubmitted(submittedAt)],
      ['Date of Leave(s)', dateOfLeaveLabel],
      ['Leave Type', leaveType || '—'],
      ...(formatPtoDayPart(dayPart)
        ? [['Day Part', formatPtoDayPart(dayPart)]]
        : []),
      ['Total Number of Days Applied', daysStr],
      ['Reason for Leave', notes || '—'],
    ];

    doc.fontSize(11).fillColor('#000000');
    for (const [label, value] of fields) {
      doc
        .font('Helvetica-Bold')
        .text(`${label}: `, { continued: true })
        .font('Helvetica')
        .text(value);
      doc.moveDown(0.4);
    }

    doc.moveDown(1.8);

    // --- Dashed divider ---
    doc
      .moveTo(margin, doc.y)
      .lineTo(pageWidth - margin, doc.y)
      .strokeColor('#888888')
      .lineWidth(0.5)
      .dash(4, { space: 3 })
      .stroke()
      .undash();

    doc.moveDown(1);

    // --- Signature line ---
    doc
      .fontSize(11)
      .font('Helvetica-Bold')
      .fillColor('#000000')
      .text('Checked and approved by:');

    doc.end();
  });
}

// ---------- Hours metrics ----------
app.get('/api/hours/metrics', requireAuth, async (req, res) => {
  const fromStr = validateDateStr(req.query.from);
  const toStr = validateDateStr(req.query.to);
  if (!fromStr || !toStr)
    return res
      .status(400)
      .json({ ok: false, error: 'from and to required (YYYY-MM-DD)' });
  const assignedTo = normText(req.query.assignedTo);
  const assignedToUPN = normText(req.query.assignedToUPN);
  const scope = resolveReportingScope(req, assignedTo, assignedToUPN);
  if (!scope.ok) return res.status(403).json({ ok: false, error: scope.error });
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
    let ptoWhere = `entry_date BETWEEN $1::date AND $2::date
      AND status = 'approved'`;
    if (!scope.broad) {
      const ownClause = buildOwnReportingAssigneeClause(
        ptoParams,
        'user_upn',
        'user_name',
        scope.self,
      );
      ptoWhere += ` AND ${ownClause}`;
    } else if (assignedTo || assignedToUPN) {
      const assignedFilter = assignedTo || assignedToUPN;
      const filterIdx = ptoParams.push(`%${assignedFilter}%`);
      ptoWhere += ` AND (COALESCE(user_name,'') ILIKE $${filterIdx} OR COALESCE(user_upn,'') ILIKE $${filterIdx})`;
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
// ---------- Missing-hours auto-notify: period resolver ----------
function resolveMissingHoursPeriod(period, todayYmd) {
  const p = parseYmd(todayYmd);
  if (!p) return null;

  const todayUtc = Date.UTC(p.y, p.mo - 1, p.d);
  const dow = new Date(todayUtc).getUTCDay(); // 0=Sun

  if (period === 'prev_week') {
    const mondayOffset = dow === 0 ? -6 : 1 - dow;
    const thisMonday = new Date(todayUtc + mondayOffset * 86400 * 1000);
    const lastMonday = new Date(thisMonday.getTime() - 7 * 86400 * 1000);
    const lastSunday = new Date(thisMonday.getTime() - 1 * 86400 * 1000);
    return {
      fromStr: lastMonday.toISOString().slice(0, 10),
      toStr: lastSunday.toISOString().slice(0, 10),
    };
  }

  if (period === 'prev_month') {
    const firstOfThisMonth = Date.UTC(p.y, p.mo - 1, 1);
    const lastOfPrevMonth = new Date(firstOfThisMonth - 86400 * 1000);
    const firstOfPrevMonth = new Date(
      Date.UTC(lastOfPrevMonth.getUTCFullYear(), lastOfPrevMonth.getUTCMonth(), 1),
    );
    return {
      fromStr: firstOfPrevMonth.toISOString().slice(0, 10),
      toStr: lastOfPrevMonth.toISOString().slice(0, 10),
    };
  }

  if (period === 'this_week') {
    const mondayOffset = dow === 0 ? -6 : 1 - dow;
    const thisMonday = new Date(todayUtc + mondayOffset * 86400 * 1000);
    return {
      fromStr: thisMonday.toISOString().slice(0, 10),
      toStr: todayYmd,
    };
  }

  // this_month (default)
  const firstOfMonth = `${String(p.y).padStart(4, '0')}-${String(p.mo).padStart(2, '0')}-01`;
  return { fromStr: firstOfMonth, toStr: todayYmd };
}

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
       AND status = 'approved'
     GROUP BY 1`,
    [fromStr, toStr],
  );
  const ptoByName = new Map(
    ptoR.rows.map((r) => [r.name_key, Number(r.pto_hours)]),
  );

  // Sum logged hour transactions, including negative corrections, for each
  // distinct (task, changed_date) snapshot within the range.
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
         AND s.task_assigned_to <> ''
       ORDER BY s.task_id,
                COALESCE(s.task_changed_date, s.snapshot_at),
                s.snapshot_at DESC,
                s.run_id DESC
     ) sub
     WHERE h <> 0
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
app.get(
  '/api/notifications/hours-preview',
  requireAuth,
  requireManagerOrAbove,
  async (req, res) => {
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
      } = await computeUserHours(
        fromStr,
        toStr,
        rng.fromUtc,
        rng.toExclusiveUtc,
      );

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
  },
);

// ---------- Missing hours notifications ----------
app.post(
  '/api/notifications/missing-hours',
  requireAuth,
  requireManagerOrAbove,
  async (req, res) => {
    if (!BREVO_API_KEY) {
      return res.status(503).json({
        ok: false,
        error: 'BREVO_API_KEY not configured on server.',
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
      } = await computeUserHours(
        fromStr,
        toStr,
        rng.fromUtc,
        rng.toExclusiveUtc,
      );

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
      const fmtPeriodDate = (s) => {
        const [y, m, d] = s.split('-');
        return new Date(+y, +m - 1, +d).toLocaleDateString('en-US', {
          month: 'long',
          day: '2-digit',
          year: 'numeric',
        });
      };
      const period = `${fmtPeriodDate(fromStr)} to ${fmtPeriodDate(toStr)}`;
      const lastSyncSuffix = buildLastSyncEmailSuffix(await fetchLastSyncAt());
      let sent = 0;
      const errors = [];

      for (const u of offenders) {
        const html = `
        <div style="font-family:sans-serif;font-size:14px;color:#1f2b2c;max-width:520px;">
          <p>Hi <strong>${escapeEmailHtml(u.name)}</strong>,</p>
          <p>This is a reminder that you have
             <strong style="color:#c8742b;">${fmtH(u.missing)} missing hours</strong>
             for the period <strong>${escapeEmailHtml(period)}</strong>${lastSyncSuffix}.</p>
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
          <p><strong>Please log your hours in TFS at your earliest convenience.</strong></p>
          <p>If there are approved exceptions, weekend deployment coverage, or manual adjustments that should be considered, please coordinate the necessary correction.</p>
          <p>Thank you for your attention to this matter.</p>
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
          <p>Hi @all,</p>
          <p>The following <strong>${offenders.length} team member(s)</strong> have <strong>more than
             ${fmtWorkingDaysFromHours(threshold)} working days of missing hours</strong> based on the logged hours for the period <strong>${escapeEmailHtml(period)}</strong>${lastSyncSuffix}:</p>
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
          <p>Please review and confirm whether the missing hours need correction or require follow-up with the team member.</p>
          <p>Thank you.</p>
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
  },
);

// ---------- Missing hours notifications (automated / cron-callable) ----------
app.post('/api/notifications/missing-hours-auto', async (req, res) => {
  if (
    !requireConfiguredApiKey(
      req,
      res,
      MISSING_HOURS_NOTIFY_API_KEY,
      'MISSING_HOURS_NOTIFY_API_KEY',
    )
  ) {
    return;
  }
  if (!BREVO_API_KEY) {
    return res
      .status(503)
      .json({ ok: false, error: 'BREVO_API_KEY not configured on server.' });
  }
  if (!NOTIFY_FROM_EMAIL) {
    return res.status(503).json({
      ok: false,
      error: 'NOTIFY_FROM_EMAIL not configured on server.',
    });
  }

  const {
    period: periodRaw,
    from: fromRaw,
    to: toRaw,
    threshold: thresholdRaw,
    managerEmail: managerEmailRaw,
    dry_run: dryRunRaw,
  } = req.body || {};

  const dryRun = dryRunRaw === true || dryRunRaw === 'true';
  const threshold =
    normNum(thresholdRaw) ?? MISSING_HOURS_THRESHOLD;
  const managerEmail = normText(managerEmailRaw) || NOTIFY_MANAGER_EMAIL;

  if (!Number.isFinite(threshold) || threshold < 0)
    return res
      .status(400)
      .json({ ok: false, error: 'threshold must be a positive number' });

  // Resolve date range: explicit from/to takes priority, then period, then default
  let fromStr = validateDateStr(fromRaw);
  let toStr = validateDateStr(toRaw);
  const todayYmd = currentReportYmd();

  const VALID_PERIODS = ['prev_week', 'prev_month', 'this_week', 'this_month'];
  let resolvedPeriod = normText(periodRaw) || null;
  if (resolvedPeriod && !VALID_PERIODS.includes(resolvedPeriod)) {
    return res.status(400).json({
      ok: false,
      error: `Invalid period. Must be one of: ${VALID_PERIODS.join(', ')}`,
    });
  }

  if (!fromStr || !toStr) {
    const periodToUse = resolvedPeriod || 'this_month';
    const range = resolveMissingHoursPeriod(periodToUse, todayYmd);
    if (!range)
      return res
        .status(500)
        .json({ ok: false, error: 'Could not resolve report period.' });
    fromStr = range.fromStr;
    toStr = range.toStr;
    resolvedPeriod = periodToUse;
  }

  const offsetMin = getReportOffsetMinutes();
  const tz = getReportTimeZone();
  const rng = rangeFromToUtc(fromStr, toStr, offsetMin, tz);
  if (!rng)
    return res
      .status(400)
      .json({ ok: false, error: 'invalid from/to date' });

  try {
    const {
      weekdayHours,
      sharedOffHours,
      requiredHours,
      ptoByName,
      loggedByName,
      users,
    } = await computeUserHours(fromStr, toStr, rng.fromUtc, rng.toExclusiveUtc);

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

    if (offenders.length === 0 || dryRun) {
      return res.json({
        ok: true,
        period: resolvedPeriod,
        from: fromStr,
        to: toStr,
        threshold,
        offenders: offenders.length,
        sent: 0,
        dry_run: dryRun,
        ...(offenders.length === 0
          ? {
              message: `No users have missing hours above ${threshold}h for this period.`,
            }
          : {}),
        ...(dryRun && offenders.length > 0
          ? { preview: offenders.map((u) => ({ name: u.name, email: u.email, missing: u.missing })) }
          : {}),
      });
    }

    const transporter = createMailTransporter();
    const fmtPeriodDate = (s) => {
      const [y, m, d] = s.split('-');
      return new Date(+y, +m - 1, +d).toLocaleDateString('en-US', {
        month: 'long',
        day: '2-digit',
        year: 'numeric',
      });
    };
    const period = `${fmtPeriodDate(fromStr)} to ${fmtPeriodDate(toStr)}`;
    const lastSyncSuffix = buildLastSyncEmailSuffix(await fetchLastSyncAt());
    let sent = 0;
    const errors = [];

    for (const u of offenders) {
      const html = `
      <div style="font-family:sans-serif;font-size:14px;color:#1f2b2c;max-width:520px;">
        <p>Hi <strong>${escapeEmailHtml(u.name)}</strong>,</p>
        <p>This is a reminder that you have
           <strong style="color:#c8742b;">${fmtH(u.missing)} missing hours</strong>
           for the period <strong>${escapeEmailHtml(period)}</strong>${lastSyncSuffix}.</p>
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
        <p><strong>Please log your hours in TFS at your earliest convenience.</strong></p>
        <p>If there are approved exceptions, weekend deployment coverage, or manual adjustments that should be considered, please coordinate the necessary correction.</p>
        <p>Thank you for your attention to this matter.</p>
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

    // Manager digest
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
        <p>Hi @all,</p>
        <p>The following <strong>${offenders.length} team member(s)</strong> have <strong>more than
           ${fmtWorkingDaysFromHours(threshold)} working days of missing hours</strong> based on the logged hours for the period <strong>${escapeEmailHtml(period)}</strong>${lastSyncSuffix}:</p>
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
        <p>Please review and confirm whether the missing hours need correction or require follow-up with the team member.</p>
        <p>Thank you.</p>
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
      period: resolvedPeriod,
      from: fromStr,
      to: toStr,
      threshold,
      offenders: offenders.length,
      sent,
      dry_run: false,
      ...(errors.length ? { errors } : {}),
    });
  } catch (e) {
    console.error('AUTO NOTIFY ERROR:', e);
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- PTO overdue reminder / escalation ----------
function getPendingPtoStageLabel(
  filerRole,
  status = 'pending',
  filerTeam = null,
) {
  if (status === 'external_pending')
    return 'Awaiting external approval confirmation';
  if (filerRole === 'dev' || filerRole === 'qa')
    return 'Awaiting lead approval';
  if (filerRole === 'lead') return 'Awaiting PM approval';
  if (filerRole === 'pm') return 'Awaiting approval from another PM';
  return 'Awaiting approval';
}

function buildPtoThreadHeaders(messageId, threadTopic) {
  if (!messageId) return {};
  const threadIndex = buildReplyThreadIndex(messageId);
  return {
    'In-Reply-To': messageId,
    References: messageId,
    'Thread-Topic': threadTopic,
    ...(threadIndex ? { 'Thread-Index': threadIndex } : {}),
  };
}

async function fetchPendingPtoReminderCandidates() {
  const r = await pool.query(
    `SELECT p.id,
            COALESCE(p.batch_id::text, '') AS batch_id,
            p.user_upn,
            p.user_name,
            p.entry_date::text,
            p.hours,
            p.day_part,
            p.leave_type,
            p.notes,
            p.status,
            p.filer_role,
            p.email_message_id,
            p.created_at,
            COALESCE(p.filer_team, u.team) AS filer_team
     FROM public.pto_entries p
     LEFT JOIN public.users u ON LOWER(u.email) = LOWER(p.user_upn)
     WHERE p.status IN ('pending','external_pending')
     ORDER BY COALESCE(p.batch_id::text, 'single:' || p.id::text), p.entry_date ASC, p.id ASC`,
  );

  const grouped = new Map();
  for (const row of r.rows) {
    const batchId = row.batch_id || null;
    const requestKey = batchId ? `batch:${batchId}` : `single:${row.id}`;
    if (!grouped.has(requestKey)) {
      grouped.set(requestKey, {
        requestKey,
        ptoEntryId: row.id,
        batchId,
        startDate: row.entry_date,
        endDate: row.entry_date,
        dayCount: 0,
        totalHours: 0,
        userUpn: row.user_upn || '',
        userName: row.user_name || row.user_upn || '',
        dayPart: row.day_part || null,
        leaveType: row.leave_type || '',
        notes: row.notes || '',
        status: row.status || '',
        filerRole: row.filer_role || '',
        filerTeam: row.filer_team || null,
        emailMessageId: row.email_message_id || '',
      });
    }
    const request = grouped.get(requestKey);
    request.endDate = row.entry_date;
    request.dayCount += 1;
    request.totalHours += Number(row.hours || 0);
    if (!request.emailMessageId && row.email_message_id) {
      request.emailMessageId = row.email_message_id;
    }
  }
  return [...grouped.values()];
}

async function fetchSharedOffDays(fromYmd, toYmd) {
  if (!fromYmd || !toYmd || fromYmd > toYmd) return new Set();
  const r = await pool.query(
    `SELECT holiday_date::text AS ymd
     FROM public.public_holidays
     WHERE holiday_date BETWEEN $1::date AND $2::date
     UNION
     SELECT entry_date::text AS ymd
     FROM public.team_off_entries
     WHERE entry_date BETWEEN $1::date AND $2::date`,
    [fromYmd, toYmd],
  );
  return new Set(r.rows.map((row) => row.ymd));
}

async function getExternalPendingPtoOwnerEmails(request) {
  const entry = {
    status: request.status,
    filer_role: request.filerRole,
    filer_team: request.filerTeam,
    user_upn: request.userUpn,
  };
  if (!isSpecialPtoWorkflow(entry)) return [];
  if (request.filerRole === 'qa') {
    return dedupeEmails(
      await getApproverEmails('qa', request.filerTeam, request.userUpn),
    );
  }
  if (request.filerRole === 'lead' || request.filerRole === 'pm') {
    return dedupeEmails(
      await getApproverEmails(
        request.filerRole,
        request.filerTeam,
        request.userUpn,
      ),
    );
  }
  return [];
}

async function getOverduePtoRecipients(request, notificationType) {
  if (request.status === 'external_pending') {
    return {
      to: await getExternalPendingPtoOwnerEmails(request),
      cc: [],
    };
  }

  const currentApprovers = dedupeEmails(
    await getApproverEmails(
      request.filerRole,
      request.filerTeam,
      request.userUpn || null,
    ),
  );

  if (notificationType === 'reminder') {
    return { to: currentApprovers, cc: [] };
  }

  const oversight = dedupeEmails(
    await getRoleEmails(
      ['pm', 'admin'],
      request.filerRole === 'pm' ? request.userUpn : null,
    ),
  );

  if (request.filerRole === 'dev' || request.filerRole === 'qa') {
    const cc = currentApprovers.filter(
      (email) =>
        !oversight.some(
          (oversightEmail) =>
            oversightEmail.toLowerCase() === String(email).toLowerCase(),
        ),
    );
    return { to: oversight, cc };
  }

  return { to: oversight, cc: [] };
}

async function reserveOverduePtoNotification(
  request,
  notificationType,
  overdueBusinessDays,
  toRecipients,
  ccRecipients,
) {
  const r = await pool.query(
    `INSERT INTO public.pto_overdue_notifications
       (request_key, pto_entry_id, batch_id, notification_type, overdue_business_days, to_recipients, cc_recipients)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (request_key, notification_type) DO NOTHING
     RETURNING id`,
    [
      request.requestKey,
      request.ptoEntryId,
      request.batchId,
      notificationType,
      overdueBusinessDays,
      toRecipients.join(', '),
      ccRecipients.join(', '),
    ],
  );
  return r.rows[0]?.id || null;
}

async function releaseOverduePtoNotification(reservationId) {
  if (!reservationId) return;
  await pool.query(
    'DELETE FROM public.pto_overdue_notifications WHERE id = $1',
    [reservationId],
  );
}

function buildOverduePtoEmail(request, notificationType, overdueBusinessDays) {
  const displayName = request.userName || request.userUpn || 'Unknown User';
  const dateLabel = fmtSubjectDateRange(request.startDate, request.endDate);
  const dayLabel =
    request.dayCount === 1
      ? `${fmtH(request.totalHours)} hrs`
      : `${request.dayCount} days, ${fmtH(request.totalHours)} hrs`;
  const pendingStage = getPendingPtoStageLabel(
    request.filerRole,
    request.status,
    request.filerTeam,
  );
  const isEscalation = notificationType === 'escalation';
  const subjectBase = `LEAVE REQUEST – ${displayName} – ${request.leaveType} Leave on ${dateLabel}`;
  const subject = `Re: ${subjectBase}`;
  const actionLabel = isEscalation ? 'Escalation' : 'Reminder';
  const intro = isEscalation
    ? 'This pending PTO request has reached the escalation threshold and still requires action.'
    : 'This pending PTO request is overdue and still requires action.';
  const html = `<div style="font-family:sans-serif;font-size:14px;color:#1f2b2c;max-width:560px;">
<p>Hi @Team,</p>
<p><strong>${escapeEmailHtml(actionLabel)}:</strong> ${escapeEmailHtml(intro)}</p>
<p style="font-family:sans-serif;font-size:13px;line-height:1.8">
  <strong>Employee:</strong> ${escapeEmailHtml(displayName)}<br>
  <strong>Leave Date:</strong> ${escapeEmailHtml(dateLabel)}<br>
  <strong>Leave Duration:</strong> ${escapeEmailHtml(dayLabel)}<br>
  ${buildPtoDayPartHtml(request.dayPart)}
  <strong>Leave Type:</strong> ${escapeEmailHtml(request.leaveType)}<br>
  <strong>Current Pending Stage:</strong> ${escapeEmailHtml(pendingStage)}<br>
  <strong>Overdue Business Days:</strong> ${escapeEmailHtml(String(overdueBusinessDays))}
</p>
${request.notes ? `<p><strong>Reason for Leave:</strong> ${escapeEmailHtml(request.notes)}</p>` : ''}
<p>Please review and process this PTO request in the TFS Hours app.</p>
<p style="color:#999;font-size:11px;">Automated message &mdash; TFS Hours Report</p>
</div>`;
  const text = `${actionLabel}: ${intro}\n\nEmployee: ${displayName}\nLeave Date: ${dateLabel}\nLeave Duration: ${dayLabel}${buildPtoDayPartText(request.dayPart)}\nLeave Type: ${request.leaveType}\nCurrent Pending Stage: ${pendingStage}\nOverdue Business Days: ${overdueBusinessDays}${request.notes ? `\nReason for Leave: ${request.notes}` : ''}\n\nPlease review and action this PTO request in the TFS Hours app.`;
  return { subjectBase, subject, html, text };
}

app.post('/api/pto/overdue-reminders', async (req, res) => {
  if (
    !requireConfiguredApiKey(
      req,
      res,
      PTO_REMINDER_API_KEY,
      'PTO_REMINDER_API_KEY',
    )
  ) {
    return;
  }
  if (!BREVO_API_KEY) {
    return res
      .status(503)
      .json({ ok: false, error: 'BREVO_API_KEY not configured on server.' });
  }
  if (!NOTIFY_FROM_EMAIL) {
    return res.status(503).json({
      ok: false,
      error: 'NOTIFY_FROM_EMAIL not configured on server.',
    });
  }

  try {
    const todayYmd = currentReportYmd();
    const requests = await fetchPendingPtoReminderCandidates();
    const earliestPendingStart = requests.reduce((minYmd, request) => {
      if (!request.startDate || request.startDate >= todayYmd) return minYmd;
      return !minYmd || request.startDate < minYmd ? request.startDate : minYmd;
    }, null);
    const sharedOffDays = earliestPendingStart
      ? await fetchSharedOffDays(
          addDaysToYmd(earliestPendingStart, 1),
          todayYmd,
        )
      : new Set();
    const transporter = createMailTransporter();

    let remindersSent = 0;
    let escalationsSent = 0;
    let skipped = 0;
    const errors = [];

    for (const request of requests) {
      const overdueBusinessDays = countOverdueBusinessDays(
        request.startDate,
        todayYmd,
        sharedOffDays,
      );

      let notificationType = null;
      if (overdueBusinessDays >= 3) notificationType = 'escalation';
      else if (overdueBusinessDays >= 2) notificationType = 'reminder';

      if (!notificationType) {
        skipped++;
        continue;
      }

      const recipients = await getOverduePtoRecipients(
        request,
        notificationType,
      );
      if (!recipients.to.length) {
        skipped++;
        errors.push({
          requestKey: request.requestKey,
          error: `no ${notificationType} recipients resolved`,
        });
        continue;
      }

      const reservationId = await reserveOverduePtoNotification(
        request,
        notificationType,
        overdueBusinessDays,
        recipients.to,
        recipients.cc,
      );
      if (!reservationId) {
        skipped++;
        continue;
      }

      try {
        const email = buildOverduePtoEmail(
          request,
          notificationType,
          overdueBusinessDays,
        );
        const headers = buildPtoThreadHeaders(
          request.emailMessageId,
          email.subjectBase,
        );
        await transporter.sendMail({
          from: `"${NOTIFY_FROM_NAME}" <${NOTIFY_FROM_EMAIL}>`,
          to: recipients.to.join(', '),
          ...(recipients.cc.length ? { cc: recipients.cc.join(', ') } : {}),
          subject: email.subject,
          html: email.html,
          text: email.text,
          ...(Object.keys(headers).length ? { headers } : {}),
        });
        if (notificationType === 'reminder') remindersSent++;
        else escalationsSent++;
      } catch (e) {
        await releaseOverduePtoNotification(reservationId);
        errors.push({
          requestKey: request.requestKey,
          error: String(e?.message || e),
        });
      }
    }

    res.json({
      ok: true,
      todayYmd,
      scannedRequests: requests.length,
      remindersSent,
      escalationsSent,
      skipped,
      ...(errors.length ? { errors } : {}),
    });
  } catch (e) {
    console.error('PTO overdue reminder error:', e);
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.get('/external/pto/:token', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'external-pto.html'));
});

app.get('/api/external/pto/:token', async (req, res) => {
  try {
    const token = normText(req.params.token);
    if (!token)
      return res
        .status(404)
        .json({ ok: false, error: 'invalid or expired link' });

    const ctx = await loadExternalPtoTokenContext(token);
    if (!ctx)
      return res
        .status(410)
        .json({ ok: false, error: 'invalid or expired link' });

    res.json({
      ok: true,
      request: buildExternalPtoSummary(ctx.tokenRow, ctx.rows),
    });
  } catch (e) {
    console.error('External PTO summary error:', e);
    res.status(500).json({ ok: false, error: 'Unable to load request.' });
  }
});

app.post('/api/external/pto/:token/approve', async (req, res) => {
  await processExternalPtoDecision(req, res, 'approved');
});

app.post('/api/external/pto/:token/deny', async (req, res) => {
  await processExternalPtoDecision(req, res, 'denied');
});

// ---------- Static UI ----------
app.use('/', express.static(path.join(__dirname, 'public')));

app.listen(PORT, () => {
  console.log(`tfs-hours-dashboard listening on :${PORT}`);
});
