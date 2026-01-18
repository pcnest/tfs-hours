let APP_CFG = null;
let LAST_ROWS = [];

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
      href
    )}" target="_blank" rel="noopener noreferrer">${label}</a>`;
  }
  return `<span class="tag">${label}</span>`;
}

function buildReportParams() {
  const p = new URLSearchParams();
  const add = (k, v) => {
    if (v !== null && v !== undefined && String(v).trim() !== '') p.set(k, v);
  };

  add('from', qs('from').value);
  add('to', qs('to').value);
  add('assignedTo', qs('assignedTo').value);
  add('limit', '2000');

  return p;
}

function tzOffsetMinutes() {
  const n = Number(APP_CFG?.reportTzOffsetMinutes ?? 0);
  return Number.isFinite(n) ? n : 0;
}

function tzLabel() {
  return APP_CFG?.reportTzLabel || 'UTC';
}

function shiftDateByOffset(d, offsetMinutes) {
  return new Date(d.getTime() + offsetMinutes * 60 * 1000);
}

function setTzLabels() {
  const tz = tzLabel();
  const a = qs('tzLabel');
  if (a) a.textContent = tz;
}

function ymdTodayInReportTz() {
  const off = tzOffsetMinutes();
  const shiftedNow = shiftDateByOffset(new Date(), off);
  return shiftedNow.toISOString().slice(0, 10);
}

function ymdAddDays(ymd, days) {
  const d = new Date(`${ymd}T00:00:00.000Z`);
  if (isNaN(d.getTime())) return ymd;
  return new Date(d.getTime() + days * 86400 * 1000).toISOString().slice(0, 10);
}

function fmtDateTime(v) {
  if (!v) return '-';
  const d = new Date(v);
  if (isNaN(d.getTime())) return '-';

  const off = tzOffsetMinutes();
  const shifted = shiftDateByOffset(d, off);
  const s = shifted.toISOString().replace('T', ' ').slice(0, 16);

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
    const raw = r.actual_hours ?? r.task_actual_hours;
    const n = Number(raw || 0);
    return acc + (Number.isFinite(n) ? n : 0);
  }, 0);
  qs('m_hours').textContent = fmtHours(totalHours);

  const from = qs('from').value;
  const to = qs('to').value;
  qs('m_range').textContent = from && to ? `${from} to ${to} ${tzLabel()}` : '-';
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
        <td>${escapeHtml(fmtDateTime(x.task_changed_date))}</td>
        <td>${fmtHours(x.actual_hours ?? x.task_actual_hours)}</td>
        <td>${escapeHtml(x.task_assigned_to || '')}</td>
      </tr>
    `
    )
    .join('');
}

async function loadReport() {
  qs(
    'tbodyReport'
  ).innerHTML = `<tr><td colspan="12" class="muted">Loading.</td></tr>`;
  setStatus('Loading report.');

  const params = buildReportParams();
  const r = await fetch(`/api/hours/latest?${params.toString()}`);
  const data = await r.json().catch(() => ({}));

  if (!r.ok || !data.ok) {
    qs(
      'tbodyReport'
    ).innerHTML = `<tr><td colspan="12" class="muted">Error: ${escapeHtml(
      data.error || `HTTP ${r.status}`
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
      fmtDateTime(x.task_changed_date),
      x.actual_hours ?? x.task_actual_hours,
      x.task_assigned_to,
    ];
    lines.push(row.map(csvEscape).join(','));
  }

  const from = (qs('from').value || 'all').replace(/[^a-zA-Z0-9_-]/g, '-');
  const to = (qs('to').value || 'all').replace(/[^a-zA-Z0-9_-]/g, '-');
  const fileName = `tfs_hours_report_${from}_to_${to}.csv`;

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

(async function boot() {
  await loadConfig();
  setTzLabels();

  const toStr = ymdTodayInReportTz();
  const fromStr = ymdAddDays(toStr, -29);

  qs('from').value = fromStr;
  qs('to').value = toStr;

  await loadReport();
})();
