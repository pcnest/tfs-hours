let APP_CFG = null;
let LAST_ROWS = [];
let USERS_LOADED = false;
let COST_TYPES_LOADED = false;
let USERS_CACHE = [];

function token() {
  return localStorage.getItem('tfsHoursToken');
}

async function apiFetch(url, opts = {}) {
  const t = token();
  const headers = { ...(opts.headers || {}) };
  if (t) headers['Authorization'] = `Bearer ${t}`;
  const res = await fetch(url, { ...opts, headers });
  if (res.status === 401) {
    localStorage.removeItem('tfsHoursToken');
    window.location.replace('/login.html');
  }
  return res;
}

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
    const r = await apiFetch('/api/users');
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) return;
    USERS_CACHE = j.users || [];
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
    const r = await apiFetch('/api/cost-types');
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
  const totalHours = rows.reduce((acc, r) => {
    const n = Number(r.actual_hours || 0);
    return acc + (Number.isFinite(n) ? n : 0);
  }, 0);
  qs('m_hours').textContent = fmtHours(totalHours);
  // m_required and m_missing are updated by loadMetrics()
}

function renderReportRows(rows) {
  if (!rows.length) {
    return `<tr><td colspan="10" class="muted">No rows in this range.</td></tr>`;
  }

  return rows
    .map(
      (x) => `
      <tr>
        <td>${escapeHtml(x.ticket_type || '')}</td>
        <td>${renderIdTag(x.ticket_id)}</td>
        <td class="title-cell">${escapeHtml(x.ticket_title || '')}</td>
        <td>${renderIdTag(x.task_id)}</td>
        <td class="title-cell">${escapeHtml(x.task_title || '')}</td>
        <td>${escapeHtml(x.task_activity || '')}</td>
        <td>${escapeHtml(fmtDateTime(x.changed_at))}</td>
        <td>${escapeHtml(x.cost_type || '')}</td>
        <td>${fmtHours(x.actual_hours)}</td>
        <td>${escapeHtml(x.task_assigned_to || '')}</td>
      </tr>
    `,
    )
    .join('');
}

async function fetchDailyAnnotations(fromYmd, toYmd, assignedTo) {
  const rangeP = new URLSearchParams({ from: fromYmd, to: toYmd });
  const ptoP = new URLSearchParams({ from: fromYmd, to: toYmd });
  if (assignedTo) ptoP.set('assignedTo', assignedTo);

  const [hData, tData, pData] = await Promise.all([
    apiFetch(`/api/holidays?${rangeP}`)
      .then((r) => r.json())
      .catch(() => ({})),
    apiFetch(`/api/team-off?${rangeP}`)
      .then((r) => r.json())
      .catch(() => ({})),
    apiFetch(`/api/pto?${ptoP}`)
      .then((r) => r.json())
      .catch(() => ({})),
  ]);

  // holiday_date -> name
  const holidays = new Map();
  for (const row of hData.rows || [])
    holidays.set(row.holiday_date, row.name || 'Holiday');

  // entry_date set
  const teamOff = new Set();
  for (const row of tData.rows || []) teamOff.add(row.entry_date);

  // entry_date -> total PTO hours
  const pto = new Map();
  for (const row of pData.rows || []) {
    const h = Number(row.hours || 0);
    pto.set(
      row.entry_date,
      (pto.get(row.entry_date) || 0) + (Number.isFinite(h) ? h : 0),
    );
  }

  return { holidays, teamOff, pto, singleUser: !!assignedTo };
}

function renderDailyHoursTable(rows, fromYmd, toYmd, annotations) {
  if (!fromYmd || !toYmd) {
    return `<tr><td colspan="3" class="muted">No date range selected.</td></tr>`;
  }

  // Build a map of localDate -> total actual_hours
  const tz = reportTimeZone();
  const off = tzOffsetMinutes();
  const dayTotals = new Map();
  for (const row of rows) {
    if (!row.changed_at) continue;
    const d = new Date(row.changed_at);
    if (isNaN(d.getTime())) continue;
    const localYmd = tz
      ? formatYmdInZone(d, tz)
      : shiftDateByOffset(d, off).toISOString().slice(0, 10);
    const h = Number(row.actual_hours || 0);
    dayTotals.set(
      localYmd,
      (dayTotals.get(localYmd) || 0) + (Number.isFinite(h) ? h : 0),
    );
  }

  const {
    holidays = new Map(),
    teamOff = new Set(),
    pto = new Map(),
    singleUser = false,
  } = annotations || {};

  const DAY_NAMES = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const html = [];
  const cur = new Date(`${fromYmd}T00:00:00.000Z`);
  const end = new Date(`${toYmd}T00:00:00.000Z`);

  while (cur <= end) {
    const ymd = cur.toISOString().slice(0, 10);
    const dow = cur.getUTCDay(); // 0=Sun, 6=Sat
    if (dow !== 0 && dow !== 6) {
      const total = dayTotals.get(ymd) || 0;
      let hoursLabel;
      let rowClass = ' class="day-zero"';
      if (total > 0) {
        hoursLabel = fmtHours(total);
        rowClass = '';
      } else if (holidays.has(ymd)) {
        hoursLabel = 'Holiday';
      } else if (teamOff.has(ymd)) {
        hoursLabel = 'Team Off';
      } else if (pto.has(ymd)) {
        hoursLabel = singleUser ? `PTO - ${fmtHours(pto.get(ymd))}` : 'PTO';
      } else {
        hoursLabel = fmtHours(0);
      }
      html.push(
        `<tr${rowClass}><td>${escapeHtml(ymd)}</td><td>${DAY_NAMES[dow]}</td><td>${escapeHtml(hoursLabel)}</td></tr>`,
      );
    }
    cur.setUTCDate(cur.getUTCDate() + 1);
  }

  return html.length
    ? html.join('')
    : `<tr><td colspan="3" class="muted">No working days in range.</td></tr>`;
}

async function loadReport() {
  qs('tbodyReport').innerHTML =
    `<tr><td colspan="10" class="muted">Loading.</td></tr>`;
  qs('tbodyDailyHours').innerHTML =
    `<tr><td colspan="3" class="muted">Loading.</td></tr>`;
  setStatus('Loading report.');

  const fromYmd = qs('from').value;
  const toYmd = qs('to').value;
  const assignedTo = qs('assignedTo').value.trim();

  const params = buildReportParams({ limit: '5000' });
  const endpoint = '/api/hours/entries';

  const [r, annotations] = await Promise.all([
    apiFetch(`${endpoint}?${params.toString()}`),
    fromYmd && toYmd
      ? fetchDailyAnnotations(fromYmd, toYmd, assignedTo)
      : Promise.resolve(null),
  ]);

  const data = await r.json().catch(() => ({}));

  if (!r.ok || !data.ok) {
    qs('tbodyReport').innerHTML =
      `<tr><td colspan="10" class="muted">Error: ${escapeHtml(
        data.error || `HTTP ${r.status}`,
      )}</td></tr>`;
    qs('tbodyDailyHours').innerHTML =
      `<tr><td colspan="3" class="muted">Error loading data.</td></tr>`;
    setStatus('Failed to load report.');
    LAST_ROWS = [];
    updateStats(LAST_ROWS);
    resetMetricCards();
    qs('btnExport').disabled = true;
    qs('btnDailyView').disabled = true;
    return { ok: false };
  }

  const rows = data.rows || [];
  LAST_ROWS = rows;

  qs('tbodyReport').innerHTML = renderReportRows(rows);
  qs('tbodyDailyHours').innerHTML = renderDailyHoursTable(
    rows,
    fromYmd,
    toYmd,
    annotations,
  );
  updateStats(rows);
  await loadMetrics(rows);
  qs('btnExport').disabled = rows.length === 0;
  qs('btnDailyView').disabled = rows.length === 0;
  setStatus(`Loaded ${rows.length} rows.`);
  return { ok: true };
}

function exportCsv() {
  if (!LAST_ROWS.length) return;

  const headers = [
    'Ticket Type',
    'Ticket ID',
    'Ticket Title',
    'Task ID',
    'Task Title',
    'Task Activity',
    'Changed Date',
    'Cost Type',
    'Actual Hours',
    'Assigned To',
  ];

  const lines = [headers.join(',')];
  for (const x of LAST_ROWS) {
    const row = [
      x.ticket_type,
      x.ticket_id,
      x.ticket_title,
      x.task_id,
      x.task_title,
      x.task_activity,
      fmtDateTime(x.changed_at),
      x.cost_type,
      x.actual_hours,
      x.task_assigned_to,
    ];
    lines.push(row.map(csvEscape).join(','));
  }

  const from = (qs('from').value || 'all').replace(/[^a-zA-Z0-9_-]/g, '-');
  const to = (qs('to').value || 'all').replace(/[^a-zA-Z0-9_-]/g, '-');
  const who = qs('assignedTo').value.trim();
  const suffix = who ? who.replace(/[^a-zA-Z0-9_-]/g, '_') : '';
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

// -------- Tab logic --------
function switchTab(name) {
  document
    .querySelectorAll('.tab-btn')
    .forEach((b) => b.classList.toggle('active', b.dataset.tab === name));
  qs('tabReport').hidden = name !== 'report';
  qs('tabPto').hidden = name !== 'pto';
  if (name === 'pto') {
    populatePtoUserList();
    loadHolidays();
    loadTeamOff();
    loadPtoEntries();
    // Non-privileged: pre-fill PTO user field with logged-in user
    const isPrivileged =
      window.CURRENT_USER?.role === 'admin' ||
      window.CURRENT_USER?.role === 'pm';
    if (!isPrivileged) {
      const ptoUser = qs('pto_user');
      if (ptoUser)
        ptoUser.value =
          window.CURRENT_USER?.name || window.CURRENT_USER?.email || '';
    }
  }
}

document.querySelectorAll('.tab-btn').forEach((b) => {
  b.addEventListener('click', () => switchTab(b.dataset.tab));
});

// -------- PTO user datalist --------
async function populatePtoUserList() {
  const dl = qs('ptoUserList');
  if (!dl || dl.dataset.loaded) return;
  try {
    const r = await apiFetch('/api/users');
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) return;
    USERS_CACHE = j.users || [];
    dl.innerHTML = j.users
      .map(
        (u) =>
          `<option value="${escapeHtml(u.name)}">${escapeHtml(u.name)}</option>`,
      )
      .join('');
    dl.dataset.loaded = '1';
  } catch {}
}

// -------- Public Holidays --------
async function loadHolidays() {
  const tbody = qs('tbodyHolidays');
  if (!tbody) return;
  try {
    const r = await apiFetch('/api/holidays');
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      tbody.innerHTML = `<tr><td colspan="4" class="muted">Failed to load.</td></tr>`;
      return;
    }
    renderHolidays(j.rows);
  } catch {
    tbody.innerHTML = `<tr><td colspan="4" class="muted">Error loading.</td></tr>`;
  }
}

function renderHolidays(rows) {
  const tbody = qs('tbodyHolidays');
  const isPrivileged =
    window.CURRENT_USER?.role === 'admin' || window.CURRENT_USER?.role === 'pm';
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="4" class="muted">No holidays defined.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows
    .map(
      (r) => `
      <tr>
        <td>${escapeHtml(r.holiday_date)}</td>
        <td>${escapeHtml(r.name || '')}</td>
        <td>${fmtHours(r.hours)}</td>
        <td>${isPrivileged ? `<button class="btn-del" data-id="${r.id}" data-type="holiday">Delete</button>` : ''}</td>
      </tr>`,
    )
    .join('');
}

qs('formHoliday')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const holiday_date = qs('hol_date').value;
  const name = qs('hol_name').value.trim();
  const hours = parseFloat(qs('hol_hours').value);
  if (!holiday_date || !name || !Number.isFinite(hours)) return;
  try {
    const r = await apiFetch('/api/holidays', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ holiday_date, name, hours }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      alert(`Error: ${j.error || r.status}`);
      return;
    }
    qs('formHoliday').reset();
    await loadHolidays();
  } catch (err) {
    alert(`Error: ${err.message}`);
  }
});

// -------- Team Off --------
async function loadTeamOff() {
  const tbody = qs('tbodyTeamOff');
  if (!tbody) return;
  try {
    const r = await apiFetch('/api/team-off');
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      tbody.innerHTML = `<tr><td colspan="4" class="muted">Failed to load.</td></tr>`;
      return;
    }
    renderTeamOff(j.rows);
  } catch {
    tbody.innerHTML = `<tr><td colspan="4" class="muted">Error loading.</td></tr>`;
  }
}

function renderTeamOff(rows) {
  const tbody = qs('tbodyTeamOff');
  const isPrivileged =
    window.CURRENT_USER?.role === 'admin' || window.CURRENT_USER?.role === 'pm';
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="4" class="muted">No team off days defined.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows
    .map(
      (r) => `
      <tr>
        <td>${escapeHtml(r.entry_date)}</td>
        <td>${fmtHours(r.hours)}</td>
        <td>${escapeHtml(r.notes || '')}</td>
        <td>${isPrivileged ? `<button class="btn-del" data-id="${r.id}" data-type="team-off">Delete</button>` : ''}</td>
      </tr>`,
    )
    .join('');
}

qs('formTeamOff')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const entry_date = qs('toff_date').value;
  const hours = parseFloat(qs('toff_hours').value);
  const notes = qs('toff_notes').value.trim();
  if (!entry_date || !Number.isFinite(hours)) return;
  try {
    const r = await apiFetch('/api/team-off', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ entry_date, hours, notes }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      alert(`Error: ${j.error || r.status}`);
      return;
    }
    qs('formTeamOff').reset();
    await loadTeamOff();
  } catch (err) {
    alert(`Error: ${err.message}`);
  }
});

// -------- Individual PTO --------
async function loadPtoEntries(userFilter = '') {
  const tbody = qs('tbodyPto');
  if (!tbody) return;
  try {
    const p = new URLSearchParams();
    if (userFilter) p.set('assignedTo', userFilter);
    const r = await apiFetch(
      `/api/pto${p.toString() ? '?' + p.toString() : ''}`,
    );
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      tbody.innerHTML = `<tr><td colspan="6" class="muted">Failed to load.</td></tr>`;
      return;
    }
    renderPtoEntries(j.rows);
  } catch {
    tbody.innerHTML = `<tr><td colspan="6" class="muted">Error loading.</td></tr>`;
  }
}

function renderPtoEntries(rows) {
  const tbody = qs('tbodyPto');
  const isPrivileged =
    window.CURRENT_USER?.role === 'admin' || window.CURRENT_USER?.role === 'pm';
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="6" class="muted">No PTO entries defined.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows
    .map((r) => {
      const delBtn = isPrivileged
        ? `<button class="btn-del" data-id="${r.id}" data-type="pto">Delete</button>`
        : '';
      return `
      <tr>
        <td>${escapeHtml(r.user_name || r.user_upn || '')}</td>
        <td>${escapeHtml(r.entry_date)}</td>
        <td>${fmtHours(r.hours)}</td>
        <td>${escapeHtml(r.leave_type || '')}</td>
        <td>${escapeHtml(r.notes || '')}</td>
        <td>${delBtn}</td>
      </tr>`;
    })
    .join('');
}

qs('formPto')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const isPrivileged =
    window.CURRENT_USER?.role === 'admin' || window.CURRENT_USER?.role === 'pm';
  const typed = isPrivileged
    ? qs('pto_user').value.trim()
    : window.CURRENT_USER?.name || window.CURRENT_USER?.email || '';
  const entry_date = qs('pto_date').value;
  const hours = parseFloat(qs('pto_hours').value);
  const leave_type = qs('pto_leave_type').value;
  const notes = qs('pto_notes').value.trim();
  if (!typed || !entry_date || !Number.isFinite(hours)) return;
  const match = USERS_CACHE.find(
    (u) =>
      u.name === typed ||
      u.upn === typed ||
      u.name?.toLowerCase() === typed.toLowerCase(),
  );
  const user_name = match?.name || typed;
  const user_upn = match?.upn || typed;
  try {
    const r = await apiFetch('/api/pto', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        user_name,
        user_upn,
        entry_date,
        hours,
        leave_type,
        notes,
      }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      alert(`Error: ${j.error || r.status}`);
      return;
    }
    const receiptMsg = j.pdfEmailSent ? ' PDF receipt emailed to you.' : '';
    qs('formPto').reset();
    if (!isPrivileged) {
      const ptoUser = qs('pto_user');
      if (ptoUser)
        ptoUser.value =
          window.CURRENT_USER?.name || window.CURRENT_USER?.email || '';
    }
    if (receiptMsg) alert(`PTO entry saved.${receiptMsg}`);
    await loadPtoEntries();
  } catch (err) {
    alert(`Error: ${err.message}`);
  }
});

qs('btnPtoView')?.addEventListener('click', () => {
  const user = qs('pto_user').value.trim();
  loadPtoEntries(user);
});

// -------- Delete delegation (all three tables) --------
document.addEventListener('click', async (e) => {
  const btn = e.target.closest('.btn-del');
  if (!btn) return;
  const id = btn.dataset.id;
  const type = btn.dataset.type;
  if (!id || !type) return;
  let url;
  if (type === 'holiday') url = `/api/holidays/${id}`;
  else if (type === 'team-off') url = `/api/team-off/${id}`;
  else if (type === 'pto') url = `/api/pto/${id}`;
  else return;
  try {
    const r = await apiFetch(url, { method: 'DELETE' });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      alert(`Error: ${j.error || r.status}`);
      return;
    }
    if (type === 'holiday') await loadHolidays();
    else if (type === 'team-off') await loadTeamOff();
    else await loadPtoEntries();
  } catch (err) {
    alert(`Error: ${err.message}`);
  }
});

// -------- Metrics --------
function resetMetricCards() {
  qs('m_required').textContent = '-';
  qs('m_pto').textContent = '-';
  qs('m_holiday').textContent = '-';
  qs('m_missing').textContent = '-';
  qs('m_missing').style.color = 'inherit';
}

async function loadMetrics(rows) {
  const from = qs('from').value;
  const to = qs('to').value;
  if (!from || !to) {
    resetMetricCards();
    return;
  }
  const assignedTo = qs('assignedTo').value.trim();
  const p = new URLSearchParams({ from, to });
  if (assignedTo) p.set('assignedTo', assignedTo);
  try {
    const r = await apiFetch(`/api/hours/metrics?${p.toString()}`);
    const m = await r.json().catch(() => ({}));
    if (!r.ok || !m.ok) {
      resetMetricCards();
      return;
    }

    const totalActual = rows.reduce((acc, row) => {
      const n = Number(row.actual_hours || 0);
      return acc + (Number.isFinite(n) ? n : 0);
    }, 0);

    let workingHours = m.weekdayHours;
    let teamOffHours = m.teamOffHours;
    const individualPtoHours = m.individualPtoHours;

    if (!assignedTo) {
      const numPeople = new Set(
        rows
          .map((row) => row.task_assigned_upn || row.task_assigned_to)
          .filter(Boolean),
      ).size;
      if (numPeople > 0) {
        workingHours = m.weekdayHours * numPeople;
        teamOffHours = m.teamOffHours * numPeople;
      }
    }

    const missing =
      workingHours - teamOffHours - individualPtoHours - totalActual;

    qs('m_required').textContent = fmtHours(workingHours);
    qs('m_pto').textContent = fmtHours(individualPtoHours);
    qs('m_holiday').textContent = fmtHours(teamOffHours);
    const missingEl = qs('m_missing');
    missingEl.textContent = fmtHours(missing);
    missingEl.style.color =
      missing > 0.5
        ? 'var(--accent-2)'
        : missing < -0.5
          ? 'var(--accent)'
          : 'inherit';
  } catch {
    resetMetricCards();
  }
}

qs('btnLoad').addEventListener('click', async () => {
  await loadConfig();
  setTzLabels();
  await loadReport();
});

qs('btnExport').addEventListener('click', () => {
  exportCsv();
});

qs('btnDailyView').addEventListener('click', () => {
  qs('dailyModal').showModal();
});

qs('btnDailyClose').addEventListener('click', () => {
  qs('dailyModal').close();
});

qs('dailyModal').addEventListener('click', (e) => {
  if (e.target === e.currentTarget) e.currentTarget.close();
});

document.querySelectorAll('.preset-btn').forEach((btn) => {
  btn.addEventListener('click', () => {
    applyPreset(btn.dataset.preset);
  });
});

// -------- Role UI --------
function setRoleUI(role) {
  const isPrivileged = role === 'admin' || role === 'pm';

  // Non-privileged: hide Send Missing Hours Notifications panel
  const notifyPanel = qs('notifyDetails')?.closest('.notify-panel');
  if (notifyPanel) notifyPanel.hidden = !isPrivileged;

  // Non-privileged: hide Export CSV button
  const btnExport = qs('btnExport');
  if (btnExport) btnExport.hidden = !isPrivileged;

  // Non-privileged: hide Assigned To filter (report is always own data)
  const assignedToWrap = qs('assignedTo')?.closest('div');
  if (assignedToWrap) assignedToWrap.hidden = !isPrivileged;

  // Non-privileged: hide add forms in Work/Federal Holidays and Team Off (tables remain visible)
  if (!isPrivileged) {
    const formHoliday = qs('formHoliday');
    if (formHoliday) formHoliday.hidden = true;
    const formTeamOff = qs('formTeamOff');
    if (formTeamOff) formTeamOff.hidden = true;
  }

  // Non-privileged: hide PTO User field and View button (own data only)
  const ptoUserWrap = qs('pto_user_wrap');
  if (ptoUserWrap) ptoUserWrap.hidden = !isPrivileged;
  const ptoViewWrap = qs('pto_view_wrap');
  if (ptoViewWrap) ptoViewWrap.hidden = !isPrivileged;
}

(async function boot() {
  // Auth gate: verify session before rendering the app
  const t = token();
  if (!t) {
    window.location.replace('/login.html');
    return;
  }

  let me;
  try {
    const meRes = await fetch('/api/auth/me', {
      headers: { Authorization: `Bearer ${t}` },
    });
    if (!meRes.ok) {
      localStorage.removeItem('tfsHoursToken');
      window.location.replace('/login.html');
      return;
    }
    me = await meRes.json();
    if (!me.ok) {
      localStorage.removeItem('tfsHoursToken');
      window.location.replace('/login.html');
      return;
    }
  } catch {
    window.location.replace('/login.html');
    return;
  }

  window.CURRENT_USER = { email: me.email, name: me.name, role: me.role };

  // Populate user chip in header
  const chip = qs('userChip');
  const chipName = qs('userChipName');
  const chipRole = qs('userChipRole');
  if (chip) chip.hidden = false;
  if (chipName) chipName.textContent = me.name || me.email;
  if (chipRole) chipRole.textContent = me.role;

  await loadConfig();
  setTzLabels();
  setRoleUI(me.role);

  // Non-privileged: lock Assigned To filter to logged-in user
  if (me.role !== 'admin' && me.role !== 'pm') {
    const assignedToEl = qs('assignedTo');
    if (assignedToEl) assignedToEl.value = me.name || me.email;
  }

  // Pre-fill manager email from server config
  if (APP_CFG?.notifyManagerEmail) {
    const el = qs('notify_manager');
    if (el && !el.value) el.value = APP_CFG.notifyManagerEmail;
  }
  if (APP_CFG?.smtpConfigured === false) {
    const s = qs('notifyStatus');
    if (s) s.textContent = 'Warning: email service not configured on server.';
  }

  const toStr = ymdTodayInReportTz();
  const fromStr = ymdAddDays(toStr, -6);

  qs('from').value = fromStr;
  qs('to').value = toStr;

  await loadUsers();
  await loadReport();
})();

qs('btnLogout')?.addEventListener('click', async () => {
  try {
    await apiFetch('/api/auth/logout', { method: 'POST' });
  } catch {
    /* ignore */
  }
  localStorage.removeItem('tfsHoursToken');
  window.location.replace('/login.html');
});

// -------- Notify preview --------
function renderNotifyPreview(rows, threshold) {
  const wrap = qs('notifyPreviewWrap');
  const tbody = qs('tbodyNotifyPreview');
  if (!wrap || !tbody) return;

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="7" class="muted">No users found.</td></tr>`;
    wrap.hidden = false;
    return;
  }

  tbody.innerHTML = rows
    .map((r, i) => {
      const over = r.missing > threshold;
      const missCls = over ? 'over' : r.missing <= 0 ? 'ok' : '';
      return `
      <tr>
        <td class="muted">${i + 1}</td>
        <td>${escapeHtml(r.name)}</td>
        <td style="text-align:right;">${fmtHours(r.weekdayHours)}</td>
        <td style="text-align:right;">${fmtHours(r.sharedOffHours)}</td>
        <td style="text-align:right;">${fmtHours(r.ptoHours)}</td>
        <td style="text-align:right;">${fmtHours(r.loggedHours)}</td>
        <td style="text-align:right;" class="${escapeHtml(missCls)}">${fmtHours(r.missing)}</td>
      </tr>`;
    })
    .join('');

  wrap.hidden = false;
}

qs('btnNotifyPreview')?.addEventListener('click', async () => {
  const from = qs('from').value;
  const to = qs('to').value;
  const threshold = Number(qs('notify_threshold').value);
  const statusEl = qs('notifyStatus');

  if (!from || !to) {
    statusEl.textContent = 'Please select a date range first.';
    return;
  }

  statusEl.textContent = 'Loading preview\u2026';
  qs('btnNotifyPreview').disabled = true;

  try {
    const p = new URLSearchParams({ from, to, threshold });
    const r = await apiFetch(`/api/notifications/hours-preview?${p}`);
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      statusEl.textContent = `Error: ${j.error || `HTTP ${r.status}`}`;
    } else {
      const over = (j.rows || []).filter((x) => x.overThreshold).length;
      statusEl.textContent = over
        ? `${over} user(s) exceed the ${threshold}h threshold.`
        : `All users are within the ${threshold}h threshold.`;
      renderNotifyPreview(j.rows || [], threshold);
    }
  } catch (err) {
    statusEl.textContent = `Error: ${err.message}`;
  } finally {
    qs('btnNotifyPreview').disabled = false;
  }
});

qs('btnNotify')?.addEventListener('click', async () => {
  const from = qs('from').value;
  const to = qs('to').value;
  const threshold = qs('notify_threshold').value;
  const manager = qs('notify_manager').value.trim();
  const statusEl = qs('notifyStatus');

  if (!from || !to) {
    statusEl.textContent = 'Please select a date range first.';
    return;
  }

  statusEl.textContent = 'Sending notifications\u2026';
  qs('btnNotify').disabled = true;

  try {
    const r = await apiFetch('/api/notifications/missing-hours', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from,
        to,
        threshold: Number(threshold),
        managerEmail: manager,
      }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      statusEl.textContent = `Error: ${j.error || `HTTP ${r.status}`}`;
    } else if (j.sent === 0) {
      statusEl.textContent =
        j.message || 'No users exceed the threshold \u2014 no emails sent.';
    } else {
      statusEl.textContent =
        `Done \u2014 ${j.sent} of ${j.offenders} email(s) sent.` +
        (j.errors?.length ? ` (${j.errors.length} failed)` : '');
    }
  } catch (err) {
    statusEl.textContent = `Error: ${err.message}`;
  } finally {
    qs('btnNotify').disabled = false;
  }
});
