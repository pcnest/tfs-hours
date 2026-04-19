let APP_CFG = null;
let LAST_ROWS = [];
let USERS_LOADED = false;
let COST_TYPES_LOADED = false;

function qs(id) {
  return document.getElementById(id);
}

function escapeHtml(v) {
  return String(v ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function csvEscape(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

async function loadUsers() {
  if (USERS_LOADED) return;
  try {
    const r = await fetch('/api/users');
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) return;
    const dl = qs('assignedToList');
    if (!dl) return;
    dl.innerHTML = j.users
      .map(
        (u) =>
          `<option value="${escapeHtml(u.name)}">${escapeHtml(u.name)}</option>`,
      )
      .join('');
    USERS_LOADED = true;
  } catch {}
}

async function loadCostTypes() {
  if (COST_TYPES_LOADED) return;
  try {
    const r = await fetch('/api/cost-types');
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) return;
    const sel = qs('costType');
    if (!sel) return;
    j.costTypes.forEach((ct) => {
      const opt = document.createElement('option');
      opt.value = ct;
      opt.textContent = ct;
      sel.appendChild(opt);
    });
    COST_TYPES_LOADED = true;
  } catch {}
}

async function loadConfig() {
  if (APP_CFG) return APP_CFG;
  try {
    const r = await fetch('/api/config');
    const j = await r.json().catch(() => ({}));
    APP_CFG = r.ok && j.ok ? j : {};
  } catch {
    APP_CFG = {};
  }
  return APP_CFG;
}

function workItemHref(id) {
  const tpl = APP_CFG?.tfsWorkItemUrlTemplate;
  if (!tpl) return null;
  return tpl.replace('{id}', encodeURIComponent(String(id)));
}

function renderIdTag(id) {
  if (id === null || id === undefined || id === '') return '-';
  const href = workItemHref(id);
  const label = escapeHtml(id);
  if (href) {
    return `<a class="tag" href="${escapeHtml(
      href,
    )}" target="_blank" rel="noopener noreferrer">${label}</a>`;
  }
  return `<span class="tag">${label}</span>`;
}

function buildReportParams(opts = {}) {
  const p = new URLSearchParams();
  const add = (k, v) => {
    if (v !== null && v !== undefined && String(v).trim() !== '') p.set(k, v);
  };

  add('from', qs('from').value);
  add('to', qs('to').value);
  add('assignedTo', qs('assignedTo').value);
  add('costType', qs('costType').value);
  add('limit', opts.limit ?? '2000');

  return p;
}

function tzOffsetMinutes() {
  const n = Number(APP_CFG?.reportTzOffsetMinutes ?? 0);
  return Number.isFinite(n) ? n : 0;
}

function tzLabel() {
  return APP_CFG?.reportTzLabel || 'UTC';
}

function reportTimeZone() {
  const tz = APP_CFG?.reportTzIana;
  return tz && String(tz).trim() ? String(tz).trim() : null;
}

function shiftDateByOffset(d, offsetMinutes) {
  return new Date(d.getTime() + offsetMinutes * 60 * 1000);
}

function formatPartsInZone(date, timeZone) {
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
  const parts = fmt.formatToParts(date);
  const map = {};
  for (const p of parts) {
    if (p.type !== 'literal') map[p.type] = p.value;
  }
  return map;
}

function formatDateTimeInZone(date, timeZone) {
  const p = formatPartsInZone(date, timeZone);
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}`;
}

function formatYmdInZone(date, timeZone) {
  const p = formatPartsInZone(date, timeZone);
  return `${p.year}-${p.month}-${p.day}`;
}

function setTzLabels() {
  const tz = tzLabel();
  const a = qs('tzLabel');
  if (a) a.textContent = tz;
}

function ymdTodayInReportTz() {
  const tz = reportTimeZone();
  if (tz) return formatYmdInZone(new Date(), tz);
  const off = tzOffsetMinutes();
  const shiftedNow = shiftDateByOffset(new Date(), off);
  return shiftedNow.toISOString().slice(0, 10);
}

function ymdAddDays(ymd, days) {
  const d = new Date(`${ymd}T00:00:00.000Z`);
  if (isNaN(d.getTime())) return ymd;
  return new Date(d.getTime() + days * 86400 * 1000).toISOString().slice(0, 10);
}

function applyPreset(preset) {
  const tz = reportTimeZone();
  const off = tzOffsetMinutes();
  const todayStr = tz
    ? formatYmdInZone(new Date(), tz)
    : shiftDateByOffset(new Date(), off).toISOString().slice(0, 10);

  let fromStr, toStr;
  const todayDate = new Date(`${todayStr}T00:00:00.000Z`);
  const dow = todayDate.getUTCDay(); // 0=Sun

  if (preset === 'today') {
    fromStr = toStr = todayStr;
  } else if (preset === 'thisweek') {
    // Mon–Sun week
    const mondayOffset = dow === 0 ? -6 : 1 - dow;
    fromStr = new Date(todayDate.getTime() + mondayOffset * 86400 * 1000)
      .toISOString()
      .slice(0, 10);
    toStr = todayStr;
  } else if (preset === 'lastweek') {
    const mondayOffset = dow === 0 ? -6 : 1 - dow;
    const thisMonday = new Date(
      todayDate.getTime() + mondayOffset * 86400 * 1000,
    );
    const lastMonday = new Date(thisMonday.getTime() - 7 * 86400 * 1000);
    const lastSunday = new Date(thisMonday.getTime() - 1 * 86400 * 1000);
    fromStr = lastMonday.toISOString().slice(0, 10);
    toStr = lastSunday.toISOString().slice(0, 10);
  } else if (preset === 'thismonth') {
    const [y, m] = todayStr.split('-').map(Number);
    fromStr = `${String(y).padStart(4, '0')}-${String(m).padStart(2, '0')}-01`;
    toStr = todayStr;
  } else if (preset === 'last30') {
    fromStr = ymdAddDays(todayStr, -29);
    toStr = todayStr;
  } else {
    return;
  }

  qs('from').value = fromStr;
  qs('to').value = toStr;
}

function fmtDateTime(v) {
  if (!v) return '-';
  const d = new Date(v);
  if (isNaN(d.getTime())) return '-';

  const tz = reportTimeZone();
  const s = tz
    ? formatDateTimeInZone(d, tz)
    : shiftDateByOffset(d, tzOffsetMinutes())
        .toISOString()
        .replace('T', ' ')
        .slice(0, 16);

  return `${s} ${tzLabel()}`;
}

function fmtHours(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return '-';
  return new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  }).format(n);
}

function setStatus(text) {
  const el = qs('status');
  if (el) el.textContent = text;
}

function updateStats(rows) {
  qs('m_rows').textContent = String(rows.length || 0);

  const totalHours = rows.reduce((acc, r) => {
    const n = Number(r.actual_hours || 0);
    return acc + (Number.isFinite(n) ? n : 0);
  }, 0);
  qs('m_hours').textContent = fmtHours(totalHours);

  const from = qs('from').value;
  const to = qs('to').value;
  qs('m_range').textContent =
    from && to ? `${from} to ${to} ${tzLabel()}` : '-';
}

function renderReportRows(rows) {
  if (!rows.length) {
    return `<tr><td colspan="12" class="muted">No rows in this range.</td></tr>`;
  }

  return rows
    .map(
      (x) => `
      <tr>
        <td>${renderIdTag(x.feature_id)}</td>
        <td class="title-cell">${escapeHtml(x.feature_title || '')}</td>
        <td>${escapeHtml(x.cost_type || '')}</td>
        <td>${escapeHtml(x.ticket_type || '')}</td>
        <td>${renderIdTag(x.ticket_id)}</td>
        <td class="title-cell">${escapeHtml(x.ticket_title || '')}</td>
        <td>${renderIdTag(x.task_id)}</td>
        <td class="title-cell">${escapeHtml(x.task_title || '')}</td>
        <td>${escapeHtml(x.task_activity || '')}</td>
        <td>${escapeHtml(fmtDateTime(x.changed_at))}</td>
        <td>${fmtHours(x.actual_hours)}</td>
        <td>${escapeHtml(x.task_assigned_to || '')}</td>
      </tr>
    `,
    )
    .join('');
}

async function loadReport() {
  qs('tbodyReport').innerHTML =
    `<tr><td colspan="12" class="muted">Loading.</td></tr>`;
  setStatus('Loading report.');

  const params = buildReportParams({ limit: '5000' });
  const endpoint = '/api/hours/entries';
  const r = await fetch(`${endpoint}?${params.toString()}`);
  const data = await r.json().catch(() => ({}));

  if (!r.ok || !data.ok) {
    qs('tbodyReport').innerHTML =
      `<tr><td colspan="12" class="muted">Error: ${escapeHtml(
        data.error || `HTTP ${r.status}`,
      )}</td></tr>`;
    setStatus('Failed to load report.');
    LAST_ROWS = [];
    updateStats(LAST_ROWS);
    qs('btnExport').disabled = true;
    return { ok: false };
  }

  const rows = data.rows || [];
  LAST_ROWS = rows;

  qs('tbodyReport').innerHTML = renderReportRows(rows);
  updateStats(rows);
  qs('btnExport').disabled = rows.length === 0;
  setStatus(`Loaded ${rows.length} rows.`);
  return { ok: true };
}

function exportCsv() {
  if (!LAST_ROWS.length) return;

  const headers = [
    'Feature ID',
    'Feature Name',
    'Cost Type',
    'Ticket Type',
    'Ticket ID',
    'Ticket Title',
    'Task ID',
    'Task Title',
    'Task Activity',
    'Changed Date',
    'Actual Hours',
    'Assigned To',
  ];

  const lines = [headers.join(',')];
  for (const x of LAST_ROWS) {
    const row = [
      x.feature_id,
      x.feature_title,
      x.cost_type,
      x.ticket_type,
      x.ticket_id,
      x.ticket_title,
      x.task_id,
      x.task_title,
      x.task_activity,
      fmtDateTime(x.changed_at),
      x.actual_hours,
      x.task_assigned_to,
    ];
    lines.push(row.map(csvEscape).join(','));
  }

  const from = (qs('from').value || 'all').replace(/[^a-zA-Z0-9_-]/g, '-');
  const to = (qs('to').value || 'all').replace(/[^a-zA-Z0-9_-]/g, '-');
  const who = qs('assignedTo').value.trim();
  const ct = qs('costType').value.trim();
  const suffix = [who, ct]
    .filter(Boolean)
    .map((s) => s.replace(/[^a-zA-Z0-9_-]/g, '_'))
    .join('_');
  const fileName = `tfs_hours_report_${from}_to_${to}${suffix ? '_' + suffix : ''}.csv`;

  const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

qs('btnLoad').addEventListener('click', async () => {
  await loadConfig();
  setTzLabels();
  await loadReport();
});

qs('btnExport').addEventListener('click', () => {
  exportCsv();
});

document.querySelectorAll('.preset-btn').forEach((btn) => {
  btn.addEventListener('click', () => {
    applyPreset(btn.dataset.preset);
  });
});

(async function boot() {
  await loadConfig();
  setTzLabels();

  const toStr = ymdTodayInReportTz();
  const fromStr = ymdAddDays(toStr, -6);

  qs('from').value = fromStr;
  qs('to').value = toStr;

  await Promise.all([loadUsers(), loadCostTypes()]);
  await loadReport();
})();
