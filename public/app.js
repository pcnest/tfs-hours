let APP_CFG = null;
let LAST_ROWS = [];
let LAST_FILTERED = [];
let CURRENT_PAGE = 1;
let CURRENT_PAGE_SIZE = 50;
let REPORT_USERS_LOADED = false;
let REPORT_REGISTERED_USERS_LOADED = false;
let PTO_USERS_LOADED = false;
let COST_TYPES_LOADED = false;
let REPORT_USERS_CACHE = [];
let REPORT_REGISTERED_USERS_CACHE = [];
let PTO_USERS_CACHE = [];

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
  if (REPORT_USERS_LOADED) return;
  try {
    const role = window.CURRENT_USER?.role;
    const isPrivileged = role === 'admin' || role === 'pm';
    const reportReq = apiFetch('/api/users');
    const registeredReq = isPrivileged
      ? loadReportRegisteredUsers()
      : Promise.resolve();

    const r = await reportReq;
    await registeredReq;
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) return;
    REPORT_USERS_CACHE = j.users || [];
    const dl = qs('assignedToList');
    if (!dl) return;
    const datalistUsers =
      isPrivileged && REPORT_REGISTERED_USERS_LOADED
      ? intersectReportUsers(REPORT_USERS_CACHE, REPORT_REGISTERED_USERS_CACHE)
      : REPORT_USERS_CACHE;
    dl.innerHTML = datalistUsers
      .map(
        (u) =>
          `<option value="${escapeHtml(u.name)}">${escapeHtml(u.name)}</option>`,
      )
      .join('');
    REPORT_USERS_LOADED = true;
  } catch {}
}

async function loadReportRegisteredUsers() {
  if (REPORT_REGISTERED_USERS_LOADED) return;
  try {
    const r = await apiFetch('/api/report-registered-users');
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) return;
    REPORT_REGISTERED_USERS_CACHE = j.users || [];
    REPORT_REGISTERED_USERS_LOADED = true;
  } catch {}
}

async function loadPtoUsers() {
  if (PTO_USERS_LOADED) return;
  try {
    const r = await apiFetch('/api/pto-users');
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) return;
    PTO_USERS_CACHE = j.users || [];
    PTO_USERS_LOADED = true;
  } catch {}
}

function intersectReportUsers(reportUsers, registeredUsers) {
  const registeredUpns = new Set(
    registeredUsers
      .map((u) => String(u?.upn || '').trim().toLowerCase())
      .filter(Boolean),
  );
  const uniqueRegisteredNames = new Set();
  const duplicateRegisteredNames = new Set();
  registeredUsers.forEach((u) => {
    const key = String(u?.name || '').trim().toLowerCase();
    if (!key) return;
    if (uniqueRegisteredNames.has(key)) duplicateRegisteredNames.add(key);
    else uniqueRegisteredNames.add(key);
  });

  const seenNames = new Set();
  return reportUsers.filter((u) => {
    const upnKey = String(u?.upn || '').trim().toLowerCase();
    const nameKey = String(u?.name || '').trim().toLowerCase();
    const include =
      (upnKey && registeredUpns.has(upnKey)) ||
      (nameKey &&
        uniqueRegisteredNames.has(nameKey) &&
        !duplicateRegisteredNames.has(nameKey));
    if (!include || !nameKey || seenNames.has(nameKey)) return false;
    seenNames.add(nameKey);
    return true;
  });
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

const COST_TYPE_LABELS = { 1: 'Capitalized', 2: 'Expense' };
function fmtCostType(v) {
  const key = String(v ?? '').trim();
  return COST_TYPE_LABELS[key] ?? key;
}

function setStatus(text) {
  const el = qs('status');
  if (el) el.textContent = text;
}

function setLastSyncStatus(text) {
  const el = qs('lastSyncStatus');
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
        <td>${escapeHtml(fmtCostType(x.cost_type))}</td>
        <td>${fmtHours(x.actual_hours)}</td>
        <td>${escapeHtml(x.task_assigned_to || '')}</td>
      </tr>
    `,
    )
    .join('');
}

// -------- Pagination --------

function renderPage() {
  const filtered = LAST_FILTERED;
  const total = filtered.length;
  const totalPages = Math.max(1, Math.ceil(total / CURRENT_PAGE_SIZE));
  CURRENT_PAGE = Math.min(CURRENT_PAGE, totalPages);
  const start = (CURRENT_PAGE - 1) * CURRENT_PAGE_SIZE;
  const pageRows = filtered.slice(start, start + CURRENT_PAGE_SIZE);
  qs('tbodyReport').innerHTML = renderReportRows(pageRows);
  renderPagination(total);
}

function renderPagination(total) {
  const el = qs('tablePagination');
  if (!el) return;
  if (total === 0) {
    el.hidden = true;
    return;
  }
  el.hidden = false;

  const totalPages = Math.max(1, Math.ceil(total / CURRENT_PAGE_SIZE));
  const page = CURRENT_PAGE;
  const start = (page - 1) * CURRENT_PAGE_SIZE + 1;
  const end = Math.min(page * CURRENT_PAGE_SIZE, total);

  // Build page number buttons with ellipsis
  const pages = new Set([1, totalPages, page]);
  if (page > 1) pages.add(page - 1);
  if (page < totalPages) pages.add(page + 1);
  const sortedPages = [...pages].sort((a, b) => a - b);

  let pageButtons = '';
  let prev = null;
  for (const p of sortedPages) {
    if (prev !== null && p - prev > 1) {
      pageButtons += `<span class="pg-ellipsis">&hellip;</span>`;
    }
    pageButtons += `<button class="pg-btn${p === page ? ' pg-btn-active' : ''}" data-page="${p}" aria-label="Page ${p}"${p === page ? ' aria-current="page"' : ''}>${p}</button>`;
    prev = p;
  }

  const pageSizeOptions = [50, 100, 200]
    .map(
      (n) =>
        `<option value="${n}"${n === CURRENT_PAGE_SIZE ? ' selected' : ''}>${n}</option>`,
    )
    .join('');

  el.innerHTML = `
    <div class="pg-info">Showing ${start}&ndash;${end} of ${total} rows</div>
    <div class="pg-controls">
      <button class="pg-btn pg-nav" data-page="${page - 1}" aria-label="Previous page"${page === 1 ? ' disabled' : ''}>&lsaquo;</button>
      ${pageButtons}
      <button class="pg-btn pg-nav" data-page="${page + 1}" aria-label="Next page"${page === totalPages ? ' disabled' : ''}>&rsaquo;</button>
    </div>
    <div class="pg-size">
      <label for="pgPageSize">Rows</label>
      <select id="pgPageSize" aria-label="Rows per page">${pageSizeOptions}</select>
    </div>`;

  qs('pgPageSize')?.addEventListener('change', (e) => {
    CURRENT_PAGE_SIZE = Number(e.target.value);
    CURRENT_PAGE = 1;
    renderPage();
  });
}

// -------- Table filter bar --------

function populateTableFilters(rows) {
  const ticketTypes = [
    ...new Set(rows.map((r) => r.ticket_type || '').filter(Boolean)),
  ].sort();
  const activities = [
    ...new Set(rows.map((r) => r.task_activity || '').filter(Boolean)),
  ].sort();

  const selType = qs('tf_ticket_type');
  if (selType) {
    const prev = selType.value;
    selType.innerHTML =
      '<option value="">All</option>' +
      ticketTypes
        .map(
          (t) => `<option value="${escapeHtml(t)}">${escapeHtml(t)}</option>`,
        )
        .join('');
    if (prev && ticketTypes.includes(prev)) selType.value = prev;
  }

  const selActivity = qs('tf_activity');
  if (selActivity) {
    const prev = selActivity.value;
    selActivity.innerHTML =
      '<option value="">All</option>' +
      activities
        .map(
          (a) => `<option value="${escapeHtml(a)}">${escapeHtml(a)}</option>`,
        )
        .join('');
    if (prev && activities.includes(prev)) selActivity.value = prev;
  }

  const bar = qs('tableFilterBar');
  if (bar) bar.hidden = rows.length === 0;
}

function applyTableFilters() {
  const filterType = (qs('tf_ticket_type')?.value || '').toLowerCase();
  const filterCost = (qs('tf_cost_type')?.value || '').toLowerCase();
  const filterActivity = (qs('tf_activity')?.value || '').toLowerCase();
  const filterDate = (qs('tf_date')?.value || '').trim();
  const filterWorkItemId = (qs('tf_work_item_id')?.value || '')
    .toLowerCase()
    .trim();
  const filterSearch = (qs('tf_search')?.value || '').toLowerCase().trim();

  const tz = reportTimeZone();
  const off = tzOffsetMinutes();

  const filtered = LAST_ROWS.filter((x) => {
    if (filterType && (x.ticket_type || '').toLowerCase() !== filterType)
      return false;
    if (filterCost && fmtCostType(x.cost_type).toLowerCase() !== filterCost)
      return false;
    if (
      filterActivity &&
      (x.task_activity || '').toLowerCase() !== filterActivity
    )
      return false;
    if (filterDate) {
      if (!x.changed_at) return false;
      const d = new Date(x.changed_at);
      if (isNaN(d.getTime())) return false;
      const localYmd = tz
        ? formatYmdInZone(d, tz)
        : shiftDateByOffset(d, off).toISOString().slice(0, 10);
      if (localYmd !== filterDate) return false;
    }
    if (filterWorkItemId) {
      const ticketId = String(x.ticket_id ?? '').toLowerCase();
      const taskId = String(x.task_id ?? '').toLowerCase();
      if (
        !ticketId.includes(filterWorkItemId) &&
        !taskId.includes(filterWorkItemId)
      ) {
        return false;
      }
    }
    if (filterSearch) {
      const haystack =
        `${x.ticket_title || ''} ${x.task_title || ''}`.toLowerCase();
      if (!haystack.includes(filterSearch)) return false;
    }
    return true;
  });

  LAST_FILTERED = filtered;
  CURRENT_PAGE = 1;
  updateStats(filtered);
  renderPage();

  // Update row count label (reflects filter state; pagination detail is in the pagination bar)
  let countEl = qs('tableFilterCount');
  if (!countEl) {
    countEl = document.createElement('span');
    countEl.id = 'tableFilterCount';
    countEl.className = 'table-filter-count';
    const bar = qs('tableFilterBar');
    if (bar) bar.appendChild(countEl);
  }
  const isFiltered =
    filterType ||
    filterCost ||
    filterActivity ||
    filterDate ||
    filterWorkItemId ||
    filterSearch;
  countEl.textContent = isFiltered
    ? `${filtered.length} of ${LAST_ROWS.length} rows`
    : `${LAST_ROWS.length} rows`;
}

function clearTableFilters() {
  const ids = [
    'tf_ticket_type',
    'tf_cost_type',
    'tf_activity',
    'tf_date',
    'tf_work_item_id',
    'tf_search',
  ];
  ids.forEach((id) => {
    const el = qs(id);
    if (!el) return;
    if (el.tagName === 'SELECT') el.value = '';
    else el.value = '';
  });
  applyTableFilters();
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
  setLastSyncStatus('Last sync: loading.');

  const fromYmd = qs('from').value;
  const toYmd = qs('to').value;
  const assignedTo = qs('assignedTo').value.trim();

  const params = buildReportParams({ limit: '5000' });
  const endpoint = '/api/hours/entries';

  const [r, annotations, metaResult] = await Promise.all([
    apiFetch(`${endpoint}?${params.toString()}`),
    fromYmd && toYmd
      ? fetchDailyAnnotations(fromYmd, toYmd, assignedTo)
      : Promise.resolve(null),
    apiFetch('/api/hours/meta')
      .then(async (res) => ({
        ok: res.ok,
        data: await res.json().catch(() => ({})),
      }))
      .catch(() => null),
  ]);

  if (metaResult?.ok && metaResult.data?.ok) {
    setLastSyncStatus(
      metaResult.data.lastSyncAt
        ? `Last sync: ${fmtDateTime(metaResult.data.lastSyncAt)}`
        : 'Last sync: Never',
    );
  } else {
    setLastSyncStatus('Last sync: unavailable');
  }

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

  clearTableFilters(); // reset filters whenever a new report loads
  populateTableFilters(rows); // rebuild dropdowns from new data
  applyTableFilters(); // renders tbodyReport + updates stats

  qs('tbodyDailyHours').innerHTML = renderDailyHoursTable(
    rows,
    fromYmd,
    toYmd,
    annotations,
  );
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
      fmtCostType(x.cost_type),
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
    // Apply role-based visibility each time the tab opens
    const role = window.CURRENT_USER?.role;
    const isPrivileged = role === 'admin' || role === 'pm';
    const canAction = role === 'lead' || role === 'pm';
    const ptoUserWrap = qs('pto_user_wrap');
    if (ptoUserWrap) ptoUserWrap.hidden = !isPrivileged;
    const ptoViewWrap = qs('pto_view_wrap');
    if (ptoViewWrap) ptoViewWrap.hidden = !isPrivileged;
    const ptoActionFilterWrap = qs('ptoActionFilterWrap');
    if (ptoActionFilterWrap) ptoActionFilterWrap.hidden = !canAction;
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
    await loadPtoUsers();
    if (!PTO_USERS_CACHE.length) return;
    dl.innerHTML = PTO_USERS_CACHE
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
        <td>${escapeHtml(r.notes || '')}</td>
        <td>${fmtHours(r.hours)}</td>
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

const PTO_STATUS_LABELS = {
  pending: 'Pending',
  lead_approved: 'Lead Approved',
  approved: 'Approved',
  denied: 'Denied',
  cancelled: 'Cancelled',
};

function ptoBadge(status) {
  const label = PTO_STATUS_LABELS[status] || status || '';
  const cls = `pto-status pto-status-${CSS.escape ? CSS.escape(status || '') : (status || '').replace(/[^a-z_]/gi, '')}`;
  return `<span class="${cls}">${escapeHtml(label)}</span>`;
}

async function loadPtoEntries(userFilter = '') {
  const tbody = qs('tbodyPto');
  if (!tbody) return;
  const actionRequired = qs('chkActionRequired')?.checked || false;
  try {
    const p = new URLSearchParams();
    if (userFilter) p.set('assignedTo', userFilter);
    if (actionRequired) p.set('actionRequired', 'true');
    const r = await apiFetch(
      `/api/pto${p.toString() ? '?' + p.toString() : ''}`,
    );
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      tbody.innerHTML = `<tr><td colspan="7" class="muted">Failed to load.</td></tr>`;
      return;
    }
    renderPtoEntries(j.rows);
  } catch {
    tbody.innerHTML = `<tr><td colspan="7" class="muted">Error loading.</td></tr>`;
  }
}

function renderPtoEntries(rows) {
  const tbody = qs('tbodyPto');
  const role = window.CURRENT_USER?.role;
  const myEmail = (window.CURRENT_USER?.email || '').toLowerCase();
  const isPrivileged = role === 'admin' || role === 'pm';
  const isLead = role === 'lead';

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="7" class="muted">No PTO entries defined.</td></tr>`;
    return;
  }

  // Group rows: batch rows (shared batch_id) render as a single combined row.
  // Preserve server order (entry_date ASC); batch rows will always be contiguous.
  const items = [];
  const batchMap = new Map();
  for (const r of rows) {
    if (r.batch_id) {
      if (!batchMap.has(r.batch_id)) {
        const group = [];
        batchMap.set(r.batch_id, group);
        items.push({ type: 'batch', batchId: r.batch_id, rows: group });
      }
      batchMap.get(r.batch_id).push(r);
    } else {
      items.push({ type: 'single', row: r });
    }
  }

  tbody.innerHTML = items
    .map((item) => {
      if (item.type === 'batch') {
        const br = item.rows;
        const first = br[0];
        const last = br[br.length - 1];
        const totalHours = br.reduce((s, x) => s + Number(x.hours), 0);
        const dateLabel =
          first.entry_date === last.entry_date
            ? escapeHtml(first.entry_date)
            : `${escapeHtml(first.entry_date)} \u2013 ${escapeHtml(last.entry_date)}`;
        const daysLabel = `${br.length} day${br.length !== 1 ? 's' : ''}`;

        const filerUpn = (first.user_upn || '').toLowerCase();
        let actionBtns = '';
        if (
          isLead &&
          ['dev', 'qa'].includes(first.filer_role) &&
          first.status === 'pending' &&
          filerUpn !== myEmail
        ) {
          actionBtns =
            `<button class="btn-approve" data-batch-id="${item.batchId}" data-action="approve">Approve</button>` +
            `<button class="btn-deny"    data-batch-id="${item.batchId}" data-action="deny">Deny</button>`;
        } else if (role === 'pm') {
          const devQaReady =
            ['dev', 'qa'].includes(first.filer_role) &&
            first.status === 'lead_approved';
          const leadReady =
            first.filer_role === 'lead' && first.status === 'pending';
          const pmReady =
            first.filer_role === 'pm' &&
            first.status === 'pending' &&
            filerUpn !== myEmail;
          if (devQaReady || leadReady || pmReady) {
            actionBtns =
              `<button class="btn-approve" data-batch-id="${item.batchId}" data-action="approve">Approve</button>` +
              `<button class="btn-deny"    data-batch-id="${item.batchId}" data-action="deny">Deny</button>`;
          }
        }

        const cancellable = !['cancelled', 'denied'].includes(first.status);
        const ptoNotPast =
          first.entry_date >= new Date().toISOString().slice(0, 10);
        const canCancel =
          cancellable && ptoNotPast && (isPrivileged || filerUpn === myEmail);
        const canDelete =
          ['cancelled', 'denied'].includes(first.status) &&
          (isPrivileged || filerUpn === myEmail);
        const cancelBtn = canCancel
          ? `<button class="btn-cancel" data-batch-id="${item.batchId}" data-action="cancel">Cancel</button>`
          : '';
        const delBtn = canDelete
          ? `<button class="btn-del" data-batch-id="${item.batchId}" data-type="pto-batch">Delete</button>`
          : '';

        const statusNote = first.denial_note || first.cancel_note || '';
        const statusNoteHtml = statusNote
          ? ` <span class="pto-status-note">${escapeHtml(statusNote)}</span>`
          : '';

        return `
      <tr>
        <td>${escapeHtml(first.user_name || first.user_upn || '')}</td>
        <td>${dateLabel} <span class="muted" style="font-size:.85em">(${daysLabel})</span></td>
        <td>${fmtHours(totalHours)}</td>
        <td>${escapeHtml(first.leave_type || '')}</td>
        <td>${escapeHtml(first.notes || '')}</td>
        <td>${ptoBadge(first.status)}${statusNoteHtml}</td>
        <td>${actionBtns}${cancelBtn}${delBtn}</td>
      </tr>`;
      }

      // Single row
      const r = item.row;
      const filerUpn = (r.user_upn || '').toLowerCase();
      const cancellable = !['cancelled', 'denied'].includes(r.status);
      const ptoNotPast = r.entry_date >= new Date().toISOString().slice(0, 10);
      const canCancel =
        cancellable && ptoNotPast && (isPrivileged || filerUpn === myEmail);
      const canDelete =
        ['cancelled', 'denied'].includes(r.status) &&
        (isPrivileged || filerUpn === myEmail);
      const cancelBtn = canCancel
        ? `<button class="btn-cancel" data-id="${r.id}" data-action="cancel">Cancel</button>`
        : '';
      const delBtn = canDelete
        ? `<button class="btn-del" data-id="${r.id}" data-type="pto">Delete</button>`
        : '';

      let actionBtns = '';
      if (
        isLead &&
        ['dev', 'qa'].includes(r.filer_role) &&
        r.status === 'pending' &&
        filerUpn !== myEmail
      ) {
        actionBtns =
          `<button class="btn-approve" data-id="${r.id}" data-action="approve">Approve</button>` +
          `<button class="btn-deny"    data-id="${r.id}" data-action="deny">Deny</button>`;
      } else if (role === 'pm') {
        const devQaReady =
          ['dev', 'qa'].includes(r.filer_role) && r.status === 'lead_approved';
        const leadReady = r.filer_role === 'lead' && r.status === 'pending';
        const pmReady =
          r.filer_role === 'pm' &&
          r.status === 'pending' &&
          filerUpn !== myEmail;
        if (devQaReady || leadReady || pmReady) {
          actionBtns =
            `<button class="btn-approve" data-id="${r.id}" data-action="approve">Approve</button>` +
            `<button class="btn-deny"    data-id="${r.id}" data-action="deny">Deny</button>`;
        }
      }

      const actionCell = `${actionBtns}${cancelBtn}${delBtn}`;
      const statusNote = r.denial_note || r.cancel_note || '';
      const statusNoteHtml = statusNote
        ? ` <span class="pto-status-note">${escapeHtml(statusNote)}</span>`
        : '';

      return `
      <tr>
        <td>${escapeHtml(r.user_name || r.user_upn || '')}</td>
        <td>${escapeHtml(r.entry_date)}</td>
        <td>${fmtHours(r.hours)}</td>
        <td>${escapeHtml(r.leave_type || '')}</td>
        <td>${escapeHtml(r.notes || '')}</td>
        <td>${ptoBadge(r.status)}${statusNoteHtml}</td>
        <td>${actionCell}</td>
      </tr>`;
    })
    .join('');
}

async function ptoAction(id, batchId, action) {
  let note = null;
  if (action === 'deny') {
    note = window.prompt('Reason for denial (optional):') ?? '';
    if (note === null) return;
  }
  if (action === 'cancel') {
    note = window.prompt('Reason for cancellation (required):');
    if (note === null) return; // user dismissed
    note = (note || '').trim();
    if (!note) {
      alert('A cancellation reason is required.');
      return;
    }
  }
  const url = batchId
    ? `/api/pto/batch/${batchId}/${action}`
    : `/api/pto/${id}/${action}`;
  try {
    const r = await apiFetch(url, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ note }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      alert(`Error: ${j.error || r.status}`);
      return;
    }
    await loadPtoEntries();
  } catch (err) {
    alert(`Error: ${err.message}`);
  }
}

// Delegated click for approve/deny/cancel buttons (single and batch)
document.addEventListener('click', async (e) => {
  const btn = e.target.closest(
    '[data-action="approve"],[data-action="deny"],[data-action="cancel"]',
  );
  if (!btn) return;
  const id = btn.dataset.id || null;
  const batchId = btn.dataset.batchId || null;
  const action = btn.dataset.action;
  if ((!id && !batchId) || !action) return;
  await ptoAction(id, batchId, action);
});

qs('chkActionRequired')?.addEventListener('change', () => loadPtoEntries());

qs('formPto')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const isPrivileged =
    window.CURRENT_USER?.role === 'admin' || window.CURRENT_USER?.role === 'pm';
  const typed = isPrivileged
    ? qs('pto_user').value.trim()
    : window.CURRENT_USER?.name || window.CURRENT_USER?.email || '';
  const entry_date_from = qs('pto_date').value;
  const entry_date_to_raw = qs('pto_date_to')?.value || '';
  const entry_date_to =
    entry_date_to_raw && entry_date_to_raw >= entry_date_from
      ? entry_date_to_raw
      : entry_date_from;
  const isRange = entry_date_to !== entry_date_from;
  const hours = parseFloat(qs('pto_hours').value);
  const leave_type = qs('pto_leave_type').value;
  const notes = qs('pto_notes').value.trim();
  if (!typed || !entry_date_from || !Number.isFinite(hours)) return;
  const match = PTO_USERS_CACHE.find(
    (u) =>
      u.name === typed ||
      u.upn === typed ||
      u.name?.toLowerCase() === typed.toLowerCase(),
  );
  const user_name = match?.name || typed;
  const user_upn = match?.upn || typed;
  try {
    const body = isRange
      ? {
          user_name,
          user_upn,
          entry_date_from,
          entry_date_to,
          hours,
          leave_type,
          notes,
        }
      : {
          user_name,
          user_upn,
          entry_date: entry_date_from,
          hours,
          leave_type,
          notes,
        };
    const r = await apiFetch('/api/pto', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      alert(`Error: ${j.error || r.status}`);
      return;
    }
    qs('formPto').reset();
    if (!isPrivileged) {
      const ptoUser = qs('pto_user');
      if (ptoUser)
        ptoUser.value =
          window.CURRENT_USER?.name || window.CURRENT_USER?.email || '';
    }
    const rangeMsg =
      isRange && j.rows?.length
        ? ` (${j.rows.length} working day${j.rows.length !== 1 ? 's' : ''})`
        : '';
    const submissionStatus = j.submissionStatus || j.row?.status || 'pending';
    const statusMsg =
      submissionStatus === 'approved'
        ? `submitted${rangeMsg} and approved.`
        : `submitted${rangeMsg} and pending approval.`;
    alert(`PTO ${statusMsg}`);
    await loadPtoEntries();
  } catch (err) {
    alert(`Error: ${err.message}`);
  }
});

qs('btnPtoView')?.addEventListener('click', () => {
  const user = qs('pto_user').value.trim();
  loadPtoEntries(user);
  qs('pto_user').value = '';
});

// -------- Delete delegation (all three tables) --------
document.addEventListener('click', async (e) => {
  const btn = e.target.closest('.btn-del');
  if (!btn) return;
  const id = btn.dataset.id;
  const batchId = btn.dataset.batchId;
  const type = btn.dataset.type;
  if (!type) return;
  let url;
  if (type === 'holiday') url = `/api/holidays/${id}`;
  else if (type === 'team-off') url = `/api/team-off/${id}`;
  else if (type === 'pto') url = `/api/pto/${id}`;
  else if (type === 'pto-batch') url = `/api/pto/batch/${batchId}`;
  else return;
  if (!url || (!id && !batchId)) return;
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

// Table filter bar — live filtering on every input/change
[
  'tf_ticket_type',
  'tf_cost_type',
  'tf_activity',
  'tf_date',
  'tf_work_item_id',
  'tf_search',
].forEach((id) => {
  qs(id)?.addEventListener('input', applyTableFilters);
  qs(id)?.addEventListener('change', applyTableFilters);
});
qs('btnClearFilters')?.addEventListener('click', clearTableFilters);

// Pagination — delegated click on page buttons
document.addEventListener('click', (e) => {
  const btn = e.target.closest('.pg-btn[data-page]');
  if (!btn || btn.disabled) return;
  const p = Number(btn.dataset.page);
  const totalPages = Math.max(
    1,
    Math.ceil(LAST_FILTERED.length / CURRENT_PAGE_SIZE),
  );
  if (!p || p < 1 || p > totalPages) return;
  CURRENT_PAGE = p;
  renderPage();
  qs('tbodyReport')?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
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
  const isLead = role === 'lead';
  const canAction = isLead || role === 'pm'; // roles that can approve/deny

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

  // Lead and PM: show "action items" filter checkbox
  const ptoActionFilterWrap = qs('ptoActionFilterWrap');
  if (ptoActionFilterWrap) ptoActionFilterWrap.hidden = !canAction;
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

  window.CURRENT_USER = {
    email: me.email,
    name: me.name,
    role: me.role,
    team: me.team || null,
  };

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
      const errDetail = j.errors?.length
        ? ` Errors: ${j.errors.map((e) => e.error).join('; ')}`
        : '';
      statusEl.textContent =
        j.message ||
        `0 of ${j.offenders} email(s) sent \u2014 all failed.${errDetail}`;
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
