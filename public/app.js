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
let PTO_LIST_VIEW_MODE = 'active';
let PTO_LIST_USER_FILTER = '';
let PTO_LAST_ROWS = [];
let PTO_CALENDAR_MONTH = '';
let PTO_CALENDAR_ITEM_MAP = new Map();
let PTO_CALENDAR_OFF_DAYS = { holidays: [], teamOff: [] };
let OFFSET_CURRENT_ID = null;
let OFFSET_CURRENT_ROW = null;
let OFFSET_LAST_ROWS = [];
let OFFSET_CANDIDATES = [];
let OFFSET_CURRENT_VALIDATION = null;
let OFFSET_CURRENT_ACTION_EVENTS = [];
let OFFSET_CURRENT_VALIDATION_EVENTS = [];
let OFFSET_LAST_EVIDENCE_FILTER_SUMMARY = null;
let OFFSET_CREATE_IDEMPOTENCY_KEY = null;
let OFFSET_SAVE_IN_FLIGHT = false;
const OFFSET_UI_ENABLED = true; // set to false to hide the "Offset Requests" tab and related UI
const PTO_ARCHIVE_EXPANDED = new Set();
const PTO_FINAL_STATUSES = new Set(['approved', 'denied', 'cancelled']);

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
        ? intersectReportUsers(
            REPORT_USERS_CACHE,
            REPORT_REGISTERED_USERS_CACHE,
          )
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
      .map((u) =>
        String(u?.upn || '')
          .trim()
          .toLowerCase(),
      )
      .filter(Boolean),
  );
  const uniqueRegisteredNames = new Set();
  const duplicateRegisteredNames = new Set();
  registeredUsers.forEach((u) => {
    const key = String(u?.name || '')
      .trim()
      .toLowerCase();
    if (!key) return;
    if (uniqueRegisteredNames.has(key)) duplicateRegisteredNames.add(key);
    else uniqueRegisteredNames.add(key);
  });

  const seenNames = new Set();
  return reportUsers.filter((u) => {
    const upnKey = String(u?.upn || '')
      .trim()
      .toLowerCase();
    const nameKey = String(u?.name || '')
      .trim()
      .toLowerCase();
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

function rowLoggedHours(row) {
  const n = Number(
    row?.logged_hours ?? row?.delta_hours ?? row?.actual_hours ?? 0,
  );
  return Number.isFinite(n) ? n : 0;
}

function hasNonZeroHours(v) {
  const n = Number(v);
  return Number.isFinite(n) && Math.abs(n) > 0.000001;
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
    return acc + rowLoggedHours(r);
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
        <td data-label="Ticket Type">${escapeHtml(x.ticket_type || '')}</td>
        <td data-label="Ticket ID">${renderIdTag(x.ticket_id)}</td>
        <td class="title-cell" data-label="Ticket Title">${escapeHtml(x.ticket_title || '')}</td>
        <td data-label="Task ID">${renderIdTag(x.task_id)}</td>
        <td class="title-cell" data-label="Task Title">${escapeHtml(x.task_title || '')}</td>
        <td data-label="Task Activity">${escapeHtml(x.task_activity || '')}</td>
        <td data-label="Changed Date">${escapeHtml(fmtDateTime(x.changed_at))}</td>
        <td data-label="Cost Type">${escapeHtml(fmtCostType(x.cost_type))}</td>
        <td data-label="Logged Hours">${fmtHours(rowLoggedHours(x))}</td>
        <td data-label="Assigned To">${escapeHtml(x.task_assigned_to || '')}</td>
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
    if (row.status !== 'approved') continue;
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

  // Build a map of localDate -> total logged hours.
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
    dayTotals.set(
      localYmd,
      (dayTotals.get(localYmd) || 0) + rowLoggedHours(row),
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
      if (hasNonZeroHours(total)) {
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
        `<tr${rowClass}><td data-label="Date">${escapeHtml(ymd)}</td><td data-label="Day">${DAY_NAMES[dow]}</td><td data-label="Total Hours">${escapeHtml(hoursLabel)}</td></tr>`,
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
    'Logged Hours',
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
      rowLoggedHours(x),
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
  if (name === 'offset' && !OFFSET_UI_ENABLED) name = 'report';
  document
    .querySelectorAll('.tab-btn')
    .forEach((b) => b.classList.toggle('active', b.dataset.tab === name));
  qs('tabReport').hidden = name !== 'report';
  const tabOffset = qs('tabOffset');
  if (tabOffset) tabOffset.hidden = !OFFSET_UI_ENABLED || name !== 'offset';
  qs('tabPto').hidden = name !== 'pto';
  if (name === 'offset' && OFFSET_UI_ENABLED) {
    updateOffsetEvidenceSection(OFFSET_CURRENT_ROW);
    populatePtoUserList();
    loadOffsetRequests();
  }
  if (name === 'pto') {
    populatePtoUserList();
    loadHolidays();
    loadTeamOff();
    syncPtoViewButtons();
    updatePtoListControlVisibility();
    loadPtoEntries();
    // Apply role-based visibility each time the tab opens
    const role = window.CURRENT_USER?.role;
    const isPrivileged = role === 'admin' || role === 'pm';
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
    dl.innerHTML = PTO_USERS_CACHE.map(
      (u) =>
        `<option value="${escapeHtml(u.name)}">${escapeHtml(u.name)}</option>`,
    ).join('');
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
        <td data-label="Date">${escapeHtml(r.holiday_date)}</td>
        <td data-label="Holiday">${escapeHtml(r.name || '')}</td>
        <td data-label="Hours">${fmtHours(r.hours)}</td>
        <td class="cell-actions" data-label="Actions"${isPrivileged ? '' : ' data-empty="true"'}>${isPrivileged ? `<button class="btn-del" data-id="${r.id}" data-type="holiday">Delete</button>` : ''}</td>
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
    await refreshPtoCalendarIfVisible();
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
        <td data-label="Date">${escapeHtml(r.entry_date)}</td>
        <td data-label="Notes">${escapeHtml(r.notes || '')}</td>
        <td data-label="Hours">${fmtHours(r.hours)}</td>
        <td class="cell-actions" data-label="Actions"${isPrivileged ? '' : ' data-empty="true"'}>${isPrivileged ? `<button class="btn-del" data-id="${r.id}" data-type="team-off">Delete</button>` : ''}</td>
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
    await refreshPtoCalendarIfVisible();
  } catch (err) {
    alert(`Error: ${err.message}`);
  }
});

// -------- Individual PTO --------

const PTO_STATUS_LABELS = {
  pending: 'Pending',
  lead_approved: 'Lead Approved',
  external_pending: 'External Pending',
  approved: 'Approved',
  denied: 'Denied',
  cancelled: 'Cancelled',
};
const PTO_DAY_PART_LABELS = {
  first_half: 'First half',
  second_half: 'Second half',
};

function syncPtoRangeValidation(report = false) {
  const fromInput = qs('pto_date');
  const toInput = qs('pto_date_to');
  if (!fromInput || !toInput) return true;

  const fromValue = fromInput.value || '';
  const toValue = toInput.value || '';

  if (fromValue) toInput.min = fromValue;
  else toInput.removeAttribute('min');

  const msg =
    fromValue && toValue && toValue < fromValue
      ? 'To date must be on or after From date.'
      : '';
  toInput.setCustomValidity(msg);

  if (report && msg) toInput.reportValidity();
  return !msg;
}

function ptoBadge(status) {
  const label = PTO_STATUS_LABELS[status] || status || '';
  const cls = `pto-status pto-status-${CSS.escape ? CSS.escape(status || '') : (status || '').replace(/[^a-z_]/gi, '')}`;
  return `<span class="${cls}">${escapeHtml(label)}</span>`;
}

function formatPtoDayPart(dayPart) {
  return PTO_DAY_PART_LABELS[dayPart] || '';
}

function ptoKey(v) {
  return String(v || '')
    .trim()
    .toLowerCase();
}

function specialPtoWorkflowTeamKey() {
  return ptoKey(APP_CFG?.specialPtoWorkflowTeam);
}

function isExternalManualFallbackVisible() {
  return APP_CFG?.ptoExternalManualFallbackVisible === true;
}

function isSpecialPtoItem(item) {
  const specialTeam = specialPtoWorkflowTeamKey();
  return (
    !!specialTeam &&
    ['qa', 'lead', 'pm'].includes(ptoKey(item?.filerRole)) &&
    ptoKey(item?.filerTeam) === specialTeam
  );
}

function hasSamePtoTeam(item, team) {
  const actorTeam = ptoKey(team);
  return !!actorTeam && actorTeam === ptoKey(item?.filerTeam);
}

function syncPtoDayPartVisibility(report = false) {
  const wrap = qs('pto_day_part_wrap');
  const input = qs('pto_day_part');
  const hours = Number(qs('pto_hours')?.value || '');
  const requiresDayPart = Number.isFinite(hours) && hours === 4;

  if (wrap) wrap.hidden = !requiresDayPart;
  if (!input) return !requiresDayPart;

  if (!requiresDayPart) {
    input.required = false;
    input.value = '';
    input.setCustomValidity('');
    return true;
  }

  input.required = true;
  const msg = input.value
    ? ''
    : 'Select first half or second half for 4-hour PTO.';
  input.setCustomValidity(msg);
  if (report && msg) input.reportValidity();
  return !msg;
}

function currentPtoMonthStartYmd() {
  const today = ymdTodayInReportTz();
  return `${today.slice(0, 7)}-01`;
}

function ptoMonthLabel(monthKey) {
  const [year, month] = String(monthKey || '')
    .split('-')
    .map((v) => Number(v));
  if (!Number.isFinite(year) || !Number.isFinite(month)) return monthKey || '';
  return new Intl.DateTimeFormat('en-US', {
    month: 'long',
    year: 'numeric',
  }).format(new Date(Date.UTC(year, month - 1, 1)));
}

function isFinalPtoStatus(status) {
  return PTO_FINAL_STATUSES.has(String(status || '').toLowerCase());
}

function isOverduePendingPto(item, todayYmd) {
  return item?.status === 'pending' && item?.sortDate < todayYmd;
}

function isPtoCalendarRole(role = window.CURRENT_USER?.role) {
  return role === 'admin' || role === 'pm';
}

function refreshPtoCalendarIfVisible() {
  if (isPtoCalendarRole() && qs('tabPto') && !qs('tabPto').hidden) {
    return loadPtoEntries();
  }
  return Promise.resolve();
}

function ptoRenderContext() {
  const role = window.CURRENT_USER?.role;
  return {
    role,
    myEmail: (window.CURRENT_USER?.email || '').toLowerCase(),
    myTeam: window.CURRENT_USER?.team || '',
    isPrivileged: role === 'admin' || role === 'pm',
    isLead: role === 'lead',
    todayYmd: ymdTodayInReportTz(),
  };
}

function ptoCalendarMonthKey() {
  if (!PTO_CALENDAR_MONTH) PTO_CALENDAR_MONTH = ymdTodayInReportTz().slice(0, 7);
  return PTO_CALENDAR_MONTH;
}

function ptoCalendarMonthDate(monthKey = ptoCalendarMonthKey()) {
  const [year, month] = String(monthKey || '')
    .split('-')
    .map((v) => Number(v));
  if (!Number.isFinite(year) || !Number.isFinite(month)) {
    const today = ymdTodayInReportTz();
    return new Date(`${today.slice(0, 7)}-01T00:00:00.000Z`);
  }
  return new Date(Date.UTC(year, month - 1, 1));
}

function ptoYmdFromUtcDate(date) {
  return date.toISOString().slice(0, 10);
}

function ptoCalendarShiftMonth(delta) {
  const d = ptoCalendarMonthDate();
  d.setUTCMonth(d.getUTCMonth() + delta);
  PTO_CALENDAR_MONTH = ptoYmdFromUtcDate(d).slice(0, 7);
}

function ptoCalendarVisibleRange(monthKey = ptoCalendarMonthKey()) {
  const first = ptoCalendarMonthDate(monthKey);
  const last = new Date(Date.UTC(first.getUTCFullYear(), first.getUTCMonth() + 1, 0));
  const leadingDays = first.getUTCDay();
  const trailingDays = 6 - last.getUTCDay();
  return {
    startYmd: ptoYmdFromUtcDate(new Date(first.getTime() - leadingDays * 86400000)),
    endYmd: ptoYmdFromUtcDate(new Date(last.getTime() + trailingDays * 86400000)),
  };
}

function ptoCalendarDates(range) {
  const dates = [];
  const cur = new Date(`${range.startYmd}T00:00:00.000Z`);
  const end = new Date(`${range.endYmd}T00:00:00.000Z`);
  while (cur <= end) {
    dates.push(ptoYmdFromUtcDate(cur));
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return dates;
}

function ptoSafeStatusClass(status) {
  return String(status || '').replace(/[^a-z0-9_-]/gi, '');
}

function ptoItemDateText(item) {
  if (item.type === 'batch' && item.sortDate !== item.endDate) {
    return `${item.sortDate} - ${item.endDate} (${item.dayCount} day${item.dayCount !== 1 ? 's' : ''})`;
  }
  if (item.type === 'batch') return `${item.sortDate} (${item.dayCount} day)`;
  return item.sortDate;
}

function ptoItemStatusNotes(item, todayYmd) {
  const notes = [];
  if (isOverduePendingPto(item, todayYmd)) notes.push('Overdue - awaiting approval.');
  if (item.statusNote) notes.push(item.statusNote);
  return notes;
}

function ptoItemCalendarEntries(item) {
  if (item.type === 'batch') {
    return item.rows.map((row) => ({
      date: row.entry_date,
      hours: Number(row.hours || 0),
      dayPart: row.day_part || item.dayPart || '',
      item,
    }));
  }
  return [{ date: item.sortDate, hours: item.hours, dayPart: item.dayPart, item }];
}

function ptoCalendarDurationLabel(hours, dayPart) {
  const n = Number(hours || 0);
  if (n === 8) return 'Full';
  if (n === 4) return formatPtoDayPart(dayPart) || 'Half day';
  return fmtHours(n);
}

function normalizePtoCalendarOffDays({ holidays = [], teamOff = [] } = {}) {
  const entries = [];
  for (const row of holidays || []) {
    const date = row?.holiday_date || '';
    if (!date) continue;
    entries.push({
      type: 'holiday',
      date,
      title: row.name || 'Public Holiday',
      meta: 'Work holiday',
    });
  }
  for (const row of teamOff || []) {
    const date = row?.entry_date || '';
    if (!date) continue;
    entries.push({
      type: 'team-off',
      date,
      title: row.notes || 'Team Off',
      meta: `Team off - ${fmtHours(row.hours)}`,
    });
  }
  return entries.sort((a, b) => {
    const dateDiff = a.date.localeCompare(b.date);
    if (dateDiff !== 0) return dateDiff;
    const typeDiff = a.type.localeCompare(b.type);
    if (typeDiff !== 0) return typeDiff;
    return a.title.localeCompare(b.title);
  });
}

function renderPtoCalendarOffChip(entry) {
  const cls = entry.type === 'holiday' ? 'pto-calendar-chip-holiday' : 'pto-calendar-chip-team-off';
  return `<div class="pto-calendar-chip pto-calendar-chip-static ${cls}">
    <span class="pto-calendar-chip-main">${escapeHtml(entry.title)}</span>
    <span class="pto-calendar-chip-meta">${escapeHtml(entry.meta)}</span>
  </div>`;
}

function renderPtoDetailField(label, value, allowHtml = false) {
  return `<div class="pto-detail-field"><div class="k">${escapeHtml(label)}</div><div class="v">${allowHtml ? value : escapeHtml(value || '-')}</div></div>`;
}

function renderPtoDetail(item, context) {
  const notes = ptoItemStatusNotes(item, context.todayYmd);
  const noteHtml = notes.length
    ? notes.map((note) => `<span class="pto-status-note">${escapeHtml(note)}</span>`).join('')
    : '';
  const dayPart = formatPtoDayPart(item.dayPart) || '-';
  const fields = [
    renderPtoDetailField('Date', ptoItemDateText(item)),
    renderPtoDetailField('Hours', fmtHours(item.hours)),
    renderPtoDetailField('Day Part', dayPart),
    renderPtoDetailField('Leave Type', item.leaveType || '-'),
    renderPtoDetailField('Status', `${ptoBadge(item.status)}${noteHtml}`, true),
    renderPtoDetailField('Reason', item.notes || '-'),
  ];

  return `
    <div class="pto-detail-summary">
      <strong>${escapeHtml(item.userName || item.userUpn || 'PTO')}</strong>
      <span class="muted">${escapeHtml(item.userUpn || '')}</span>
    </div>
    <div class="pto-detail-grid">${fields.join('')}</div>`;
}

function openPtoDetailModal(itemKey) {
  const item = PTO_CALENDAR_ITEM_MAP.get(itemKey);
  const modal = qs('ptoDetailModal');
  const body = qs('ptoDetailBody');
  const actions = qs('ptoDetailActions');
  if (!item || !modal || !body || !actions) return;
  const context = ptoRenderContext();
  body.innerHTML = renderPtoDetail(item, context);
  actions.innerHTML = renderPtoActionCell(item, context);
  modal.showModal();
}

function closePtoDetailModal() {
  const modal = qs('ptoDetailModal');
  if (modal?.open) modal.close();
}

function renderPtoCalendar(rows, offDays = PTO_CALENDAR_OFF_DAYS) {
  const grid = qs('ptoCalendarGrid');
  const title = qs('ptoCalendarTitle');
  if (!grid) return;

  const monthKey = ptoCalendarMonthKey();
  const range = ptoCalendarVisibleRange(monthKey);
  const context = ptoRenderContext();
  if (title) title.textContent = ptoMonthLabel(monthKey);

  const items = normalizePtoItems(rows).sort((a, b) => {
    const actionDiff =
      (getPtoItemActions(a, context).isActionRequired ? 0 : 1) -
      (getPtoItemActions(b, context).isActionRequired ? 0 : 1);
    if (actionDiff !== 0) return actionDiff;
    const dateDiff = a.sortDate.localeCompare(b.sortDate);
    if (dateDiff !== 0) return dateDiff;
    return a.key.localeCompare(b.key);
  });
  PTO_CALENDAR_ITEM_MAP = new Map(items.map((item) => [item.key, item]));

  const entriesByDate = new Map();
  const hoursByDate = new Map();
  items.forEach((item) => {
    ptoItemCalendarEntries(item).forEach((entry) => {
      if (entry.date < range.startYmd || entry.date > range.endYmd) return;
      if (!entriesByDate.has(entry.date)) entriesByDate.set(entry.date, []);
      entriesByDate.get(entry.date).push(entry);
      const h = Number(entry.hours || 0);
      hoursByDate.set(entry.date, (hoursByDate.get(entry.date) || 0) + (Number.isFinite(h) ? h : 0));
    });
  });

  const offEntriesByDate = new Map();
  normalizePtoCalendarOffDays(offDays).forEach((entry) => {
    if (entry.date < range.startYmd || entry.date > range.endYmd) return;
    if (!offEntriesByDate.has(entry.date)) offEntriesByDate.set(entry.date, []);
    offEntriesByDate.get(entry.date).push(entry);
  });

  const todayYmd = context.todayYmd;
  const days = ptoCalendarDates(range);
  const html = days.map((ymd) => {
    const dayEntries = entriesByDate.get(ymd) || [];
    const totalHours = hoursByDate.get(ymd) || 0;
    const dayNum = Number(ymd.slice(8, 10));
    const cls = [
      'pto-calendar-day',
      ymd.slice(0, 7) === monthKey ? '' : 'is-outside-month',
      ymd === todayYmd ? 'is-today' : '',
    ]
      .filter(Boolean)
      .join(' ');
    const offChips = (offEntriesByDate.get(ymd) || [])
      .map((entry) => renderPtoCalendarOffChip(entry))
      .join('');
    const ptoChips = dayEntries
      .map(({ item, hours, dayPart }) => {
        const status = ptoSafeStatusClass(item.status);
        const duration = ptoCalendarDurationLabel(hours, dayPart);
        const leaveType = item.leaveType || 'PTO';
        const meta = `${duration}${item.type === 'batch' ? ' batch' : ''} - ${leaveType}`;
        return `<button type="button" class="pto-calendar-chip pto-calendar-status-${status}" data-pto-calendar-item="${escapeHtml(item.key)}">
          <span class="pto-calendar-chip-main">${escapeHtml(item.userName || item.userUpn || 'PTO')}</span>
          <span class="pto-calendar-chip-meta">${escapeHtml(meta)}</span>
        </button>`;
      })
      .join('');
    const chips = offChips + ptoChips;
    return `<div class="${cls}" data-date="${ymd}">
      <div class="pto-calendar-date"><span>${dayNum}</span>${totalHours ? `<span class="pto-calendar-total">${fmtHours(totalHours)}</span>` : ''}</div>
      <div class="pto-calendar-items">${chips}</div>
    </div>`;
  });

  grid.innerHTML = html.join('');
}

function syncPtoViewButtons() {
  document.querySelectorAll('[data-pto-view]').forEach((btn) => {
    const active = btn.dataset.ptoView === PTO_LIST_VIEW_MODE;
    btn.classList.toggle('is-active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
}

function updatePtoListControlVisibility() {
  const role = window.CURRENT_USER?.role;
  const isPrivileged = role === 'admin' || role === 'pm';
  const useCalendar = isPtoCalendarRole(role);
  const canAction = role === 'lead' || role === 'pm';
  const showActionFilter = canAction && (useCalendar || PTO_LIST_VIEW_MODE !== 'archive');

  const ptoUserWrap = qs('pto_user_wrap');
  if (ptoUserWrap) ptoUserWrap.hidden = !isPrivileged;
  const ptoUserInput = qs('pto_user');
  if (ptoUserInput) {
    ptoUserInput.required = isPrivileged;
    ptoUserInput.disabled = !isPrivileged;
    if (!isPrivileged) {
      ptoUserInput.value =
        window.CURRENT_USER?.name || window.CURRENT_USER?.email || '';
      ptoUserInput.setCustomValidity('');
    }
  }

  const ptoListUserWrap = qs('pto_list_user_wrap');
  if (ptoListUserWrap) ptoListUserWrap.hidden = !isPrivileged;

  const ptoViewModeWrap = qs('ptoViewModeWrap');
  if (ptoViewModeWrap) ptoViewModeWrap.hidden = useCalendar;

  const ptoCalendarNav = qs('ptoCalendarNav');
  if (ptoCalendarNav) ptoCalendarNav.hidden = !useCalendar;

  const ptoCalendarWrap = qs('ptoCalendarWrap');
  if (ptoCalendarWrap) ptoCalendarWrap.hidden = !useCalendar;

  const ptoPendingWrap = qs('ptoPendingWrap');
  if (ptoPendingWrap) ptoPendingWrap.hidden = !useCalendar;

  const ptoTableWrap = qs('ptoTableWrap');
  if (ptoTableWrap) ptoTableWrap.hidden = useCalendar;

  const ptoViewWrap = qs('pto_view_wrap');
  if (ptoViewWrap) ptoViewWrap.hidden = !isPrivileged;

  const ptoActionFilterWrap = qs('ptoActionFilterWrap');
  if (ptoActionFilterWrap) ptoActionFilterWrap.hidden = !showActionFilter;

  const chk = qs('chkActionRequired');
  if (chk) chk.disabled = !showActionFilter;
}

function normalizePtoItems(rows) {
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

  return items.map((item) => {
    if (item.type === 'batch') {
      const first = item.rows[0];
      const last = item.rows[item.rows.length - 1];
      return {
        type: 'batch',
        key: `batch:${item.batchId}`,
        batchId: item.batchId,
        rows: item.rows,
        sortDate: first.entry_date,
        endDate: last.entry_date,
        dayCount: item.rows.length,
        hours: item.rows.reduce((sum, row) => sum + Number(row.hours || 0), 0),
        userName: first.user_name || first.user_upn || '',
        userUpn: first.user_upn || '',
        filerRole: first.filer_role || '',
        filerTeam: first.filer_team || '',
        status: first.status || '',
        dayPart: first.day_part || '',
        leaveType: first.leave_type || '',
        notes: first.notes || '',
        statusNote: first.denial_note || first.cancel_note || '',
      };
    }

    const r = item.row;
    return {
      type: 'single',
      key: `single:${r.id}`,
      id: r.id,
      row: r,
      sortDate: r.entry_date,
      endDate: r.entry_date,
      dayCount: 1,
      hours: Number(r.hours || 0),
      userName: r.user_name || r.user_upn || '',
      userUpn: r.user_upn || '',
      filerRole: r.filer_role || '',
      filerTeam: r.filer_team || '',
      status: r.status || '',
      dayPart: r.day_part || '',
      leaveType: r.leave_type || '',
      notes: r.notes || '',
      statusNote: r.denial_note || r.cancel_note || '',
    };
  });
}

function getPtoItemActions(
  item,
  { role, myEmail, myTeam, isPrivileged, isLead, todayYmd },
) {
  const filerUpn = String(item.userUpn || '').toLowerCase();
  const sameTeam = hasSamePtoTeam(item, myTeam);
  const externalManualFallbackVisible = isExternalManualFallbackVisible();
  let canApprove = false;
  let canDeny = false;
  let canExternalApprove = false;

  if (isSpecialPtoItem(item)) {
    if (item.filerRole === 'qa') {
      if (
        item.status === 'pending' &&
        isLead &&
        sameTeam &&
        filerUpn !== myEmail
      ) {
        canApprove = true;
        canDeny = true;
      } else if (
        item.status === 'external_pending' &&
        isLead &&
        sameTeam &&
        filerUpn !== myEmail &&
        externalManualFallbackVisible
      ) {
        canExternalApprove = true;
        canDeny = true;
      } else if (item.status === 'lead_approved' && role === 'pm' && sameTeam) {
        canApprove = true;
        canDeny = true;
      }
    } else if (item.filerRole === 'lead') {
      if (role === 'pm' && sameTeam && item.status === 'pending') {
        canApprove = true;
        canDeny = true;
      } else if (
        role === 'pm' &&
        sameTeam &&
        item.status === 'external_pending' &&
        externalManualFallbackVisible
      ) {
        canExternalApprove = true;
        canDeny = true;
      }
    } else if (item.filerRole === 'pm') {
      const otherPm = role === 'pm' && sameTeam && filerUpn !== myEmail;
      if (otherPm && item.status === 'pending') {
        canApprove = true;
        canDeny = true;
      } else if (
        otherPm &&
        item.status === 'external_pending' &&
        externalManualFallbackVisible
      ) {
        canExternalApprove = true;
        canDeny = true;
      }
    }
  } else if (
    isLead &&
    ['dev', 'qa'].includes(item.filerRole) &&
    item.status === 'pending' &&
    filerUpn !== myEmail
  ) {
    canApprove = true;
    canDeny = true;
  } else if (role === 'pm') {
    const devQaReady =
      ['dev', 'qa'].includes(item.filerRole) && item.status === 'lead_approved';
    const leadReady = item.filerRole === 'lead' && item.status === 'pending';
    const pmReady =
      item.filerRole === 'pm' &&
      item.status === 'pending' &&
      filerUpn !== myEmail;
    const tsReady = item.filerRole === 'ts' && item.status === 'pending';
    canApprove = devQaReady || leadReady || pmReady || tsReady;
    canDeny = canApprove;
  }

  const cancellable = !['cancelled', 'denied'].includes(item.status);
  const canCancel =
    cancellable &&
    item.sortDate >= todayYmd &&
    (isPrivileged || filerUpn === myEmail);
  const canDelete =
    ['cancelled', 'denied'].includes(item.status) &&
    (isPrivileged || filerUpn === myEmail);

  return {
    filerUpn,
    canApprove,
    canDeny,
    canExternalApprove,
    canCancel,
    canDelete,
    isActionRequired: canApprove || canDeny || canExternalApprove,
  };
}

function renderPtoActionCell(item, context) {
  const actions = getPtoItemActions(item, context);
  let html = '';

  if (actions.canApprove) {
    if (item.type === 'batch') {
      html += `<button class="btn-approve" data-batch-id="${item.batchId}" data-action="approve">Approve</button>`;
    } else {
      html += `<button class="btn-approve" data-id="${item.id}" data-action="approve">Approve</button>`;
    }
  }

  if (actions.canExternalApprove) {
    if (item.type === 'batch') {
      html += `<button class="btn-external" data-batch-id="${item.batchId}" data-action="external-approve">Mark External Approval Received</button>`;
    } else {
      html += `<button class="btn-external" data-id="${item.id}" data-action="external-approve">Mark External Approval Received</button>`;
    }
  }

  if (actions.canDeny) {
    if (item.type === 'batch') {
      html += `<button class="btn-deny" data-batch-id="${item.batchId}" data-action="deny">Deny</button>`;
    } else {
      html += `<button class="btn-deny" data-id="${item.id}" data-action="deny">Deny</button>`;
    }
  }

  if (actions.canCancel) {
    if (item.type === 'batch') {
      html += `<button class="btn-cancel" data-batch-id="${item.batchId}" data-action="cancel">Cancel</button>`;
    } else {
      html += `<button class="btn-cancel" data-id="${item.id}" data-action="cancel">Cancel</button>`;
    }
  }

  if (actions.canDelete) {
    if (item.type === 'batch') {
      html += `<button class="btn-del" data-batch-id="${item.batchId}" data-type="pto-batch">Delete</button>`;
    } else {
      html += `<button class="btn-del" data-id="${item.id}" data-type="pto">Delete</button>`;
    }
  }

  return html ? `<div class="pto-table-actions">${html}</div>` : '';
}

function renderPtoItemRow(item, context, extraAttrs = '') {
  const statusNotes = [];
  if (isOverduePendingPto(item, context.todayYmd)) {
    statusNotes.push('Overdue - awaiting approval.');
  }
  if (item.statusNote) statusNotes.push(item.statusNote);
  const statusNoteHtml = statusNotes.length
    ? statusNotes
        .map(
          (note) => ` <span class="pto-status-note">${escapeHtml(note)}</span>`,
        )
        .join('')
    : '';
  const dateHtml =
    item.type === 'batch' && item.sortDate !== item.endDate
      ? `${escapeHtml(item.sortDate)} - ${escapeHtml(item.endDate)} <span class="muted" style="font-size:.85em">(${item.dayCount} day${item.dayCount !== 1 ? 's' : ''})</span>`
      : item.type === 'batch'
        ? `${escapeHtml(item.sortDate)} <span class="muted" style="font-size:.85em">(${item.dayCount} day)</span>`
        : escapeHtml(item.sortDate);
  const actionHtml = renderPtoActionCell(item, context);

  return `
      <tr${extraAttrs}>
        <td data-label="User">${escapeHtml(item.userName)}</td>
        <td class="pto-date-cell" data-label="Date">${dateHtml}</td>
        <td data-label="Hours">${fmtHours(item.hours)}</td>
        <td data-label="Day Part">${escapeHtml(formatPtoDayPart(item.dayPart))}</td>
        <td data-label="Leave Type">${escapeHtml(item.leaveType)}</td>
        <td class="pto-notes-cell" data-label="Notes">${escapeHtml(item.notes)}</td>
        <td class="pto-status-cell" data-label="Status">${ptoBadge(item.status)}${statusNoteHtml}</td>
        <td class="cell-actions" data-label="Actions"${actionHtml ? '' : ' data-empty="true"'}>${actionHtml}</td>
      </tr>`;
}

function renderPtoInfoRow(message, cls = 'pto-empty-row') {
  return `<tr class="${cls}"><td colspan="8">${escapeHtml(message)}</td></tr>`;
}

function renderPtoSectionRow(label) {
  return `<tr class="pto-section-row"><td colspan="8">${escapeHtml(label)}</td></tr>`;
}

function renderPtoPendingTable(rows) {
  const tbody = qs('tbodyPtoPending');
  if (!tbody) return;
  const context = ptoRenderContext();
  const items = normalizePtoItems(rows || [])
    .filter((item) => !isFinalPtoStatus(item.status))
    .sort((a, b) => {
      const actionDiff =
        (getPtoItemActions(a, context).isActionRequired ? 0 : 1) -
        (getPtoItemActions(b, context).isActionRequired ? 0 : 1);
      if (actionDiff !== 0) return actionDiff;
      const dateDiff = a.sortDate.localeCompare(b.sortDate);
      if (dateDiff !== 0) return dateDiff;
      return a.key.localeCompare(b.key);
    });

  tbody.innerHTML = items.length
    ? items.map((item) => renderPtoItemRow(item, context)).join('')
    : renderPtoInfoRow('No pending PTO requests.');
}

function renderArchiveGroups(items, context) {
  if (!items.length) {
    return renderPtoInfoRow(
      'No archived PTO yet. Prior finalized months will appear here.',
    );
  }

  const groups = new Map();
  items.forEach((item) => {
    const monthKey = item.sortDate.slice(0, 7);
    if (!groups.has(monthKey)) groups.set(monthKey, []);
    groups.get(monthKey).push(item);
  });

  return [...groups.keys()]
    .sort((a, b) => b.localeCompare(a))
    .map((monthKey) => {
      const monthItems = groups
        .get(monthKey)
        .slice()
        .sort((a, b) => a.sortDate.localeCompare(b.sortDate));
      const expanded = PTO_ARCHIVE_EXPANDED.has(monthKey);
      const totalHours = monthItems.reduce((sum, item) => sum + item.hours, 0);
      const meta = `${monthItems.length} entr${monthItems.length === 1 ? 'y' : 'ies'} &middot; ${fmtHours(totalHours)} hours`;

      return (
        `<tr class="pto-archive-month-row">
          <td colspan="8">
            <button type="button" class="pto-archive-toggle" data-archive-month="${monthKey}" aria-expanded="${expanded ? 'true' : 'false'}">
              <span class="pto-archive-label">${escapeHtml(ptoMonthLabel(monthKey))}</span>
              <span class="pto-archive-meta">${meta}</span>
              <span class="pto-archive-chevron" aria-hidden="true">
                <svg viewBox="0 0 12 12" focusable="false">
                  <path d="M2.25 4.5 6 8.25 9.75 4.5" />
                </svg>
              </span>
            </button>
          </td>
        </tr>` +
        monthItems
          .map((item) =>
            renderPtoItemRow(
              item,
              context,
              ` data-archive-group="${monthKey}"${expanded ? '' : ' hidden'}`,
            ),
          )
          .join('')
      );
    })
    .join('');
}

async function loadPtoEntries(userFilter = PTO_LIST_USER_FILTER) {
  const tbody = qs('tbodyPto');
  const calendarGrid = qs('ptoCalendarGrid');
  const pendingTbody = qs('tbodyPtoPending');
  if (!tbody && !calendarGrid) return;
  const useCalendar = isPtoCalendarRole();
  PTO_LIST_USER_FILTER = String(userFilter || '').trim();
  PTO_LAST_ROWS = [];
  const filterInput = qs('pto_list_user');
  if (filterInput && filterInput.value !== PTO_LIST_USER_FILTER) {
    filterInput.value = PTO_LIST_USER_FILTER;
  }

  if (useCalendar && calendarGrid) {
    calendarGrid.innerHTML = `<div class="pto-calendar-message muted">Loading...</div>`;
    if (pendingTbody) {
      pendingTbody.innerHTML = `<tr><td colspan="8" class="muted">Loading...</td></tr>`;
    }
  } else if (tbody) {
    tbody.innerHTML = `<tr><td colspan="8" class="muted">Loading...</td></tr>`;
  }

  const actionRequired =
    (useCalendar || PTO_LIST_VIEW_MODE !== 'archive') &&
    (qs('chkActionRequired')?.checked || false);
  try {
    if (useCalendar) {
      const range = ptoCalendarVisibleRange();
      const calendarParams = new URLSearchParams({
        from: ymdAddDays(range.startYmd, -31),
        to: ymdAddDays(range.endYmd, 31),
      });
      const offDayParams = new URLSearchParams({
        from: range.startYmd,
        to: range.endYmd,
      });
      const pendingParams = new URLSearchParams();
      if (PTO_LIST_USER_FILTER) {
        calendarParams.set('assignedTo', PTO_LIST_USER_FILTER);
        pendingParams.set('assignedTo', PTO_LIST_USER_FILTER);
      }
      if (actionRequired) {
        calendarParams.set('actionRequired', 'true');
        pendingParams.set('actionRequired', 'true');
      }

      const [calendarRes, holidayRes, teamOffRes, pendingRes] = await Promise.all([
        apiFetch(`/api/pto?${calendarParams.toString()}`),
        apiFetch(`/api/holidays?${offDayParams.toString()}`),
        apiFetch(`/api/team-off?${offDayParams.toString()}`),
        apiFetch(`/api/pto${pendingParams.toString() ? '?' + pendingParams.toString() : ''}`),
      ]);
      const [calendarJson, holidayJson, teamOffJson, pendingJson] = await Promise.all([
        calendarRes.json().catch(() => ({})),
        holidayRes.json().catch(() => ({})),
        teamOffRes.json().catch(() => ({})),
        pendingRes.json().catch(() => ({})),
      ]);

      if (!calendarRes.ok || !calendarJson.ok || !holidayRes.ok || !holidayJson.ok || !teamOffRes.ok || !teamOffJson.ok) {
        if (calendarGrid) {
          calendarGrid.innerHTML = `<div class="pto-calendar-message muted">Failed to load.</div>`;
        }
      } else {
        PTO_LAST_ROWS = Array.isArray(calendarJson.rows) ? calendarJson.rows : [];
        PTO_CALENDAR_OFF_DAYS = {
          holidays: Array.isArray(holidayJson.rows) ? holidayJson.rows : [],
          teamOff: Array.isArray(teamOffJson.rows) ? teamOffJson.rows : [],
        };
        renderPtoEntries(PTO_LAST_ROWS);
      }

      if (!pendingRes.ok || !pendingJson.ok) {
        if (pendingTbody) {
          pendingTbody.innerHTML = `<tr><td colspan="8" class="muted">Failed to load pending PTO.</td></tr>`;
        }
      } else {
        renderPtoPendingTable(Array.isArray(pendingJson.rows) ? pendingJson.rows : []);
      }
      return;
    }

    const p = new URLSearchParams();
    if (PTO_LIST_USER_FILTER) p.set('assignedTo', PTO_LIST_USER_FILTER);
    if (actionRequired) p.set('actionRequired', 'true');
    const r = await apiFetch(
      `/api/pto${p.toString() ? '?' + p.toString() : ''}`,
    );
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      if (tbody) {
        tbody.innerHTML = `<tr><td colspan="8" class="muted">Failed to load.</td></tr>`;
      }
      return;
    }
    PTO_LAST_ROWS = Array.isArray(j.rows) ? j.rows : [];
    renderPtoEntries(j.rows);
  } catch {
    if (useCalendar && calendarGrid) {
      calendarGrid.innerHTML = `<div class="pto-calendar-message muted">Error loading.</div>`;
      if (pendingTbody) {
        pendingTbody.innerHTML = `<tr><td colspan="8" class="muted">Error loading pending PTO.</td></tr>`;
      }
    } else if (tbody) {
      tbody.innerHTML = `<tr><td colspan="8" class="muted">Error loading.</td></tr>`;
    }
  }
}
function renderPtoEntries(rows) {
  const tbody = qs('tbodyPto');
  if (isPtoCalendarRole()) {
    renderPtoCalendar(rows || []);
    return;
  }
  const context = ptoRenderContext();

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="8" class="muted">No PTO entries defined.</td></tr>`;
    return;
  }

  const monthStart = currentPtoMonthStartYmd();
  const items = normalizePtoItems(rows);
  const activeItems = [];
  const archiveItems = [];
  items.forEach((item) => {
    if (isFinalPtoStatus(item.status) && item.sortDate < monthStart) {
      archiveItems.push(item);
    } else {
      activeItems.push(item);
    }
  });

  const actionRank = (item) =>
    getPtoItemActions(item, context).isActionRequired ? 0 : 1;
  const activeItemsForAll = activeItems.slice().sort((a, b) => {
    const rankDiff = actionRank(a) - actionRank(b);
    if (rankDiff !== 0) return rankDiff;
    const dateDiff = a.sortDate.localeCompare(b.sortDate);
    if (dateDiff !== 0) return dateDiff;
    return a.key.localeCompare(b.key);
  });

  const html = [];

  if (PTO_LIST_VIEW_MODE === 'archive') {
    html.push(renderArchiveGroups(archiveItems, context));
  } else if (PTO_LIST_VIEW_MODE === 'all') {
    if (activeItemsForAll.length) {
      html.push(
        activeItemsForAll
          .map((item) => renderPtoItemRow(item, context))
          .join(''),
      );
    } else {
      html.push(renderPtoInfoRow('No current or upcoming PTO entries.'));
    }
    html.push(renderPtoSectionRow('Archived PTO'));
    html.push(renderArchiveGroups(archiveItems, context));
  } else {
    if (activeItems.length) {
      html.push(
        activeItems.map((item) => renderPtoItemRow(item, context)).join(''),
      );
    } else {
      html.push(renderPtoInfoRow('No current or upcoming PTO entries.'));
    }
    html.push(renderPtoSectionRow('Archived PTO'));
    html.push(renderArchiveGroups(archiveItems, context));
  }

  tbody.innerHTML = html.join('');
}

async function ptoAction(id, batchId, action) {
  let note = null;
  if (action === 'deny') {
    note = window.prompt('Reason for denial (optional):') ?? '';
    if (note === null) return false;
  }
  if (action === 'cancel') {
    note = window.prompt('Reason for cancellation (required):');
    if (note === null) return false; // user dismissed
    note = (note || '').trim();
    if (!note) {
      alert('A cancellation reason is required.');
      return false;
    }
  }
  if (action === 'external-approve') {
    note = window.prompt('External approval note (optional):');
    if (note === null) return false;
    note = (note || '').trim();
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
      await loadPtoEntries();
      return false;
    }
    await loadPtoEntries();
    return true;
  } catch (err) {
    alert(`Error: ${err.message}`);
    return false;
  }
}

// Delegated click for approve/deny/cancel buttons (single and batch)
document.addEventListener('click', async (e) => {
  const btn = e.target.closest(
    '[data-action="approve"],[data-action="deny"],[data-action="cancel"],[data-action="external-approve"]',
  );
  if (!btn) return;
  const id = btn.dataset.id || null;
  const batchId = btn.dataset.batchId || null;
  const action = btn.dataset.action;
  if ((!id && !batchId) || !action) return;
  const fromPtoDetail = !!btn.closest('#ptoDetailModal');
  const ok = await ptoAction(id, batchId, action);
  if (ok && fromPtoDetail) closePtoDetailModal();
});

qs('chkActionRequired')?.addEventListener('change', () => loadPtoEntries());

document.querySelectorAll('[data-pto-view]').forEach((btn) => {
  btn.addEventListener('click', () => {
    const mode = btn.dataset.ptoView;
    if (!mode || mode === PTO_LIST_VIEW_MODE) return;
    PTO_LIST_VIEW_MODE = mode;
    syncPtoViewButtons();
    updatePtoListControlVisibility();
    loadPtoEntries();
  });
});

qs('pto_date')?.addEventListener('input', () => {
  syncPtoRangeValidation();
});

qs('pto_date_to')?.addEventListener('input', () => {
  syncPtoRangeValidation();
});

qs('pto_hours')?.addEventListener('change', () => {
  syncPtoDayPartVisibility();
});

qs('pto_day_part')?.addEventListener('change', () => {
  syncPtoDayPartVisibility();
});

document.addEventListener('click', (e) => {
  const btn = e.target.closest('[data-archive-month]');
  if (!btn) return;
  const monthKey = btn.dataset.archiveMonth;
  if (!monthKey) return;
  if (PTO_ARCHIVE_EXPANDED.has(monthKey)) PTO_ARCHIVE_EXPANDED.delete(monthKey);
  else PTO_ARCHIVE_EXPANDED.add(monthKey);
  renderPtoEntries(PTO_LAST_ROWS);
});

qs('formPto')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const isPrivileged =
    window.CURRENT_USER?.role === 'admin' || window.CURRENT_USER?.role === 'pm';
  const typed = isPrivileged
    ? qs('pto_user').value.trim()
    : window.CURRENT_USER?.name || window.CURRENT_USER?.email || '';
  const entry_date_from = qs('pto_date').value;
  const entry_date_to_raw = qs('pto_date_to')?.value || '';
  if (!syncPtoRangeValidation(true)) return;
  const entry_date_to = entry_date_to_raw || entry_date_from;
  const isRange = entry_date_to !== entry_date_from;
  const hours = Number(qs('pto_hours').value);
  if (!syncPtoDayPartVisibility(true)) return;
  const day_part = qs('pto_day_part')?.value || '';
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
          ...(day_part ? { day_part } : {}),
          leave_type,
          notes,
        }
      : {
          user_name,
          user_upn,
          entry_date: entry_date_from,
          hours,
          ...(day_part ? { day_part } : {}),
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
    syncPtoRangeValidation();
    syncPtoDayPartVisibility();
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

syncPtoDayPartVisibility();

qs('btnPtoView')?.addEventListener('click', () => {
  const user = qs('pto_list_user')?.value.trim() || '';
  loadPtoEntries(user);
});

document.addEventListener('click', (e) => {
  const nav = e.target.closest('[data-pto-calendar-nav]');
  if (!nav) return;
  const action = nav.dataset.ptoCalendarNav;
  if (action === 'prev') ptoCalendarShiftMonth(-1);
  else if (action === 'next') ptoCalendarShiftMonth(1);
  else if (action === 'today') PTO_CALENDAR_MONTH = ymdTodayInReportTz().slice(0, 7);
  else return;
  loadPtoEntries();
});

document.addEventListener('click', (e) => {
  const chip = e.target.closest('[data-pto-calendar-item]');
  if (!chip) return;
  openPtoDetailModal(chip.dataset.ptoCalendarItem);
});

qs('btnPtoDetailClose')?.addEventListener('click', closePtoDetailModal);

qs('ptoDetailModal')?.addEventListener('click', (e) => {
  if (e.target === e.currentTarget) closePtoDetailModal();
});

// -------- Delete delegation (all three tables) --------
document.addEventListener('click', async (e) => {
  const btn = e.target.closest('.btn-del');
  if (!btn) return;
  const id = btn.dataset.id;
  const batchId = btn.dataset.batchId;
  const type = btn.dataset.type;
  const fromPtoDetail = !!btn.closest('#ptoDetailModal');
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
    if (type === 'holiday') {
      await loadHolidays();
      await refreshPtoCalendarIfVisible();
    } else if (type === 'team-off') {
      await loadTeamOff();
      await refreshPtoCalendarIfVisible();
    } else {
      await loadPtoEntries();
      if (fromPtoDetail) closePtoDetailModal();
    }
  } catch (err) {
    alert(`Error: ${err.message}`);
  }
});

// -------- Offset / Make-up pilot --------
const OFFSET_STATUS_LABELS = {
  pending_review: 'Pending Review',
  returned: 'Returned',
  approved: 'Approved',
  cancelled: 'Cancelled',
};

const OFFSET_VALIDATION_LABELS = {
  pending: 'Pending',
  passed: 'Passed',
  warning: 'Warning',
  failed: 'Failed',
  stale: 'Stale Sync',
};

const OFFSET_ACTION_LABELS = {
  create: 'Created',
  edit: 'Edited',
  resubmit: 'Resubmitted',
  evidence_save: 'Evidence saved',
  recheck: 'Validation rechecked',
  approve: 'Approved',
  return: 'Returned',
  cancel: 'Cancelled',
  email_sent: 'Email sent',
  email_failed: 'Email failed',
  email_skipped: 'Email skipped',
};

const OFFSET_EVIDENCE_PREREQ_KEYS = new Set([
  'interruption_date_not_future',
  'interruption_minimum',
  'requested_hours_match',
  'makeup_window',
  'makeup_date_available',
  'sync_freshness',
]);

function isOffsetPilotUser() {
  return !!window.CURRENT_USER;
}

const OFFSET_REASON_OTHER_VALUE = 'Others';

function canUseOffsetRequestFilters() {
  return ['admin', 'pm', 'lead'].includes(window.CURRENT_USER?.role);
}

function canViewOffsetEmployeeField() {
  return window.CURRENT_USER?.role === 'admin';
}

function syncOffsetRoleUi() {
  const filterBar = qs('offsetRequestFilterBar');
  if (filterBar) filterBar.hidden = !canUseOffsetRequestFilters();

  const employeeField = qs('offsetEmployeeField');
  if (employeeField) employeeField.hidden = !canViewOffsetEmployeeField();

  const loadEvidenceField = qs('offsetLoadEvidenceField');
  if (loadEvidenceField) loadEvidenceField.hidden = !canManageOffsetRequest();
}

function clearOffsetCustomReasonOptions() {
  qs('offset_reason')
    ?.querySelectorAll('option[data-custom-reason="1"]')
    .forEach((option) => option.remove());
}

function ensureOffsetReasonOption(value) {
  const select = qs('offset_reason');
  const reason = String(value || '').trim();
  if (!select || !reason) return;
  if (Array.from(select.options).some((option) => option.value === reason))
    return;

  const option = document.createElement('option');
  option.value = reason;
  option.textContent = reason;
  option.dataset.customReason = '1';
  const other = Array.from(select.options).find(
    (item) => item.value === OFFSET_REASON_OTHER_VALUE,
  );
  select.insertBefore(option, other || null);
}

function syncOffsetReasonRequirement() {
  const reason = qs('offset_reason');
  const remarks = qs('offset_remarks');
  if (!reason || !remarks) return;

  const requiresRemarks =
    reason.value === OFFSET_REASON_OTHER_VALUE && !remarks.disabled;
  remarks.required = requiresRemarks;
  remarks.placeholder = requiresRemarks
    ? 'Required when Others is selected'
    : 'Optional remarks';
  if (!requiresRemarks) remarks.setCustomValidity('');
}

function offsetEvidenceBlockers(summary = OFFSET_CURRENT_VALIDATION) {
  const checks = Array.isArray(summary?.checks) ? summary.checks : [];
  return checks.filter(
    (check) => OFFSET_EVIDENCE_PREREQ_KEYS.has(check.key) && !check.passed,
  );
}

function offsetEvidenceBlockedMessage(blockers = offsetEvidenceBlockers()) {
  return blockers.length
    ? `Evidence linking disabled: ${blockers[0].message || blockers[0].label}`
    : '';
}

function offsetPrimaryFailedReasons(summary) {
  const blockers = offsetEvidenceBlockers(summary);
  if (blockers.length) return blockers;
  const checks = Array.isArray(summary?.checks) ? summary.checks : [];
  return Array.isArray(summary?.failedReasons) && summary.failedReasons.length
    ? summary.failedReasons
    : checks.filter((check) => !check.passed);
}

const OFFSET_FINAL_STATUSES = new Set(['approved', 'cancelled']);

function normOffsetIdentity(v) {
  return String(v || '')
    .trim()
    .toLowerCase();
}

function isOffsetFinalStatus(status) {
  return OFFSET_FINAL_STATUSES.has(String(status || ''));
}

function isOffsetOwnRequest(row) {
  const meEmail = normOffsetIdentity(window.CURRENT_USER?.email);
  const rowUpn = normOffsetIdentity(row?.user_upn);
  return !!meEmail && rowUpn === meEmail;
}

function canEditOffsetRequest(row = OFFSET_CURRENT_ROW) {
  if (!row) return true;
  if (isOffsetFinalStatus(row.status)) return false;
  return window.CURRENT_USER?.role === 'admin' || isOffsetOwnRequest(row);
}

function canManageOffsetRequest() {
  return window.CURRENT_USER?.role === 'admin';
}

function makeOffsetIdempotencyKey() {
  if (window.crypto?.randomUUID) return window.crypto.randomUUID();
  const bytes = new Uint8Array(16);
  if (window.crypto?.getRandomValues) {
    window.crypto.getRandomValues(bytes);
    return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function getOffsetCreateIdempotencyKey() {
  if (!OFFSET_CREATE_IDEMPOTENCY_KEY) {
    OFFSET_CREATE_IDEMPOTENCY_KEY = makeOffsetIdempotencyKey();
  }
  return OFFSET_CREATE_IDEMPOTENCY_KEY;
}

function resetOffsetCreateIdempotencyKey() {
  if (!OFFSET_CURRENT_ID && !OFFSET_SAVE_IN_FLIGHT) {
    OFFSET_CREATE_IDEMPOTENCY_KEY = null;
  }
}

function offsetCurrentUserLabel() {
  return window.CURRENT_USER?.name || window.CURRENT_USER?.email || '';
}

function isOffsetEvidenceLocked(row = OFFSET_CURRENT_ROW) {
  return !!row && !canEditOffsetRequest(row);
}

function syncOffsetFormAccess(row = OFFSET_CURRENT_ROW) {
  syncOffsetRoleUi();
  const canEdit = canEditOffsetRequest(row) && !OFFSET_SAVE_IN_FLIGHT;
  const isAdmin = window.CURRENT_USER?.role === 'admin';
  const employee = qs('offset_user');
  if (employee) {
    employee.readOnly = !isAdmin;
    employee.disabled = !canEdit;
    if (!isAdmin && (!row || isOffsetOwnRequest(row))) {
      employee.value = offsetCurrentUserLabel();
    }
  }
  [
    'offset_date',
    'offset_start',
    'offset_end',
    'offset_makeup_date',
    'offset_reason',
    'offset_remarks',
  ].forEach((id) => {
    const el = qs(id);
    if (el) el.disabled = !canEdit;
  });
  const save = qs('btnOffsetSave');
  if (save) save.disabled = !canEdit;
  syncOffsetReasonRequirement();
}

function updateOffsetEvidenceSection(row) {
  const section = qs('offsetEvidenceSection');
  const locked = isOffsetEvidenceLocked(row);
  const hidden = !row?.id || locked;
  if (section) section.hidden = hidden;
  if (hidden) {
    OFFSET_CANDIDATES = [];
    OFFSET_LAST_EVIDENCE_FILTER_SUMMARY = null;
    const status = qs('offsetEvidenceStatus');
    if (status) status.textContent = '';
    renderOffsetEvidenceContext(null);
    renderOffsetEvidence([]);
  }
  updateOffsetEvidenceControls();
}

function updateOffsetEvidenceControls(summary = OFFSET_CURRENT_VALIDATION) {
  syncOffsetRoleUi();
  const blockers = offsetEvidenceBlockers(summary);
  const hidden = qs('offsetEvidenceSection')?.hidden === true;
  const disabled = hidden || !OFFSET_CURRENT_ID || blockers.length > 0;
  const loadBtn = qs('btnOffsetLoadEvidence');
  if (loadBtn) loadBtn.disabled = disabled || !canManageOffsetRequest();
  [
    'btnOffsetAutoAllocate',
    'btnOffsetValidateEvidence',
    'btnOffsetSaveEvidence',
  ].forEach((id) => {
    const btn = qs(id);
    if (btn) btn.disabled = disabled;
  });
  const status = qs('offsetEvidenceStatus');
  if (status && blockers.length)
    status.textContent = offsetEvidenceBlockedMessage(blockers);
}

function offsetBadge(value, labels = OFFSET_STATUS_LABELS) {
  const raw = String(value || '');
  const label = labels[raw] || raw || '';
  const cls = `pto-status pto-status-${CSS.escape ? CSS.escape(raw) : raw.replace(/[^a-z_]/gi, '')}`;
  return `<span class="${cls}">${escapeHtml(label)}</span>`;
}

function resolveOffsetUser() {
  const typed = qs('offset_user')?.value.trim() || '';
  const match = PTO_USERS_CACHE.find(
    (u) =>
      u.name === typed ||
      u.upn === typed ||
      u.name?.toLowerCase() === typed.toLowerCase() ||
      u.upn?.toLowerCase() === typed.toLowerCase(),
  );
  return {
    user_name: match?.name || typed,
    user_upn: match?.upn || typed,
  };
}

function calculateOffsetRequestedHours() {
  const start = qs('offset_start')?.value || '';
  const end = qs('offset_end')?.value || '';
  if (!start || !end) return null;
  const [sh, sm] = start.split(':').map(Number);
  const [eh, em] = end.split(':').map(Number);
  if (![sh, sm, eh, em].every(Number.isFinite)) return null;
  const mins = eh * 60 + em - (sh * 60 + sm);
  return mins > 0 ? Math.round((mins / 60) * 100) / 100 : null;
}

function setOffsetComputedHours(value) {
  const hoursInput = qs('offset_hours');
  const hoursDisplay = qs('offset_hours_display');
  const hours = Number(value);
  if (Number.isFinite(hours) && hours > 0) {
    const normalized = (Math.round(hours * 100) / 100).toFixed(2);
    if (hoursInput) hoursInput.value = normalized;
    if (hoursDisplay) hoursDisplay.textContent = `${fmtHours(normalized)}h`;
  } else {
    if (hoursInput) hoursInput.value = '';
    if (hoursDisplay) hoursDisplay.textContent = '-';
  }
}

function syncOffsetRequestedHours() {
  setOffsetComputedHours(calculateOffsetRequestedHours());
}

function offsetRequestBodyFromForm() {
  const user = resolveOffsetUser();
  const requestedHours = calculateOffsetRequestedHours();
  setOffsetComputedHours(requestedHours);
  return {
    ...user,
    interruption_date: qs('offset_date')?.value || '',
    interruption_start_time: qs('offset_start')?.value || '',
    interruption_end_time: qs('offset_end')?.value || '',
    requested_makeup_hours: requestedHours ?? 0,
    planned_makeup_date: qs('offset_makeup_date')?.value || '',
    reason: qs('offset_reason')?.value.trim() || '',
    remarks: qs('offset_remarks')?.value.trim() || '',
  };
}

function syncOffsetDateLimits(todayYmd = ymdTodayInReportTz()) {
  const interruptionDate = qs('offset_date');
  if (interruptionDate) interruptionDate.max = todayYmd;
}

function renderOffsetValidation(summary) {
  const wrap = qs('offsetValidation');
  if (!wrap) return;
  if (!summary || !summary.checks) {
    OFFSET_CURRENT_VALIDATION = null;
    updateOffsetEvidenceControls(null);
    wrap.hidden = true;
    wrap.innerHTML = '';
    return;
  }
  OFFSET_CURRENT_VALIDATION = summary;
  const capacity = summary.capacity || {};
  const request = summary.request || {};
  const evidence = summary.evidence || {};
  const sync = summary.sync || {};
  const failedReasons = offsetPrimaryFailedReasons(summary);
  const item = (label, value) => `
    <div class="offset-summary-item">
      <span class="k">${escapeHtml(label)}</span>
      <span class="v">${escapeHtml(value)}</span>
    </div>`;
  const failureSummary = failedReasons.length
    ? `
    <div class="offset-failure-summary">
      <strong>${summary.validationStatus === 'stale' ? 'Needs attention' : 'Failed because'}</strong>
      <ul>
        ${failedReasons
          .map(
            (check) =>
              `<li><strong>${escapeHtml(check.label || 'Validation check')}</strong>: ${escapeHtml(check.message || '')}</li>`,
          )
          .join('')}
      </ul>
    </div>`
    : '';
  const validationNote = failedReasons.length
    ? failureSummary
    : summary.validationStatus === 'passed'
      ? '<div class="offset-pass-summary">All validation checks passed.</div>'
      : '';
  const capacityCards = canManageOffsetRequest()
    ? `
      ${item('Rendered', `${fmtHours(capacity.netRenderedHours)}h`)}
      ${item('Required', `${fmtHours(capacity.regularRequiredHours)}h`)}
      ${item('Available', `${fmtHours(capacity.availableMakeupHours)}h`)}`
    : '';
  wrap.innerHTML = `
    <div class="pto-list-bar">
      <strong>Validation ${offsetBadge(summary.validationStatus, OFFSET_VALIDATION_LABELS)}</strong>
    </div>
    <div class="offset-summary-grid">
      ${item('Requested', `${fmtHours(request.requestedMakeupHours)}h`)}
      ${item('Deadline', request.deadlineYmd || '-')}
      ${capacityCards}
      ${item('Evidence', `${fmtHours(evidence.allocatedHours)}h`)}
      ${item('Sync Age', sync.syncAgeHours == null ? '-' : `${fmtHours(sync.syncAgeHours)}h`)}
    </div>
    ${validationNote}`;
  wrap.hidden = false;
  updateOffsetEvidenceControls(summary);
}

function renderOffsetEvidenceContext(
  summary = OFFSET_LAST_EVIDENCE_FILTER_SUMMARY,
) {
  const wrap = qs('offsetEvidenceContext');
  if (!wrap) return;
  if (!canManageOffsetRequest() || !summary) {
    wrap.hidden = true;
    wrap.innerHTML = '';
    return;
  }
  const chips = [
    summary.plannedMakeupDate
      ? `Make-up date ${summary.plannedMakeupDate}`
      : '',
    summary.assignedTo ? `Assigned to ${summary.assignedTo}` : '',
    summary.positiveDeltaOnly ? 'Positive delta hours only' : '',
    summary.excludesUsedHours ? 'Reused task hours excluded' : '',
    summary.sameDay && summary.cutoffTime
      ? `Same-day cutoff ${summary.cutoffTime}`
      : 'Full make-up date',
  ].filter(Boolean);
  wrap.innerHTML = `
    <strong>TFS evidence filter</strong>
    <div class="offset-context-list">
      ${chips.map((chip) => `<span>${escapeHtml(chip)}</span>`).join('')}
    </div>`;
  wrap.hidden = false;
}

function offsetEvidenceClientKey(taskId, changedAt) {
  const iso = changedAt ? new Date(changedAt).toISOString() : '';
  return `${taskId}|${iso}`;
}

function offsetValidationEventNote(event) {
  const summary = event?.validation_summary || {};
  const reasons = Array.isArray(summary.failedReasons)
    ? summary.failedReasons
    : [];
  if (event?.validation_status === 'passed')
    return 'All validation checks passed.';
  if (!reasons.length) return '';
  return reasons
    .slice(0, 2)
    .map((reason) => `${reason.label || 'Validation'}: ${reason.message || ''}`)
    .join(' ');
}

function renderOffsetAuditTimeline(
  row,
  actionEvents = [],
  validationEvents = [],
) {
  const wrap = qs('offsetAuditTimeline');
  if (!wrap) return;
  if (!canManageOffsetRequest()) {
    wrap.hidden = true;
    wrap.innerHTML = '';
    return;
  }
  const actionItems = (Array.isArray(actionEvents) ? actionEvents : []).map(
    (event) => ({
      kind: 'action',
      at: event.created_at,
      title: OFFSET_ACTION_LABELS[event.action] || event.action || 'Action',
      actor: event.actor_email || '',
      note: event.note || '',
      meta:
        event.from_status &&
        event.to_status &&
        event.from_status !== event.to_status
          ? `${OFFSET_STATUS_LABELS[event.from_status] || event.from_status} -> ${OFFSET_STATUS_LABELS[event.to_status] || event.to_status}`
          : event.to_status
            ? OFFSET_STATUS_LABELS[event.to_status] || event.to_status
            : '',
    }),
  );
  const validationItems = (
    Array.isArray(validationEvents) ? validationEvents : []
  )
    .slice(0, 5)
    .map((event) => ({
      kind: 'validation',
      at: event.created_at,
      title:
        `Validation ${OFFSET_VALIDATION_LABELS[event.validation_status] || event.validation_status || ''}`.trim(),
      actor: '',
      note: offsetValidationEventNote(event),
      meta: '',
    }));
  const items = [...actionItems, ...validationItems]
    .filter((item) => item.at)
    .sort((a, b) => new Date(b.at).getTime() - new Date(a.at).getTime())
    .slice(0, 12);
  if (!row || !items.length) {
    wrap.hidden = true;
    wrap.innerHTML = '';
    return;
  }
  wrap.innerHTML = `
    <strong>Request timeline</strong>
    ${items
      .map((item) => {
        const meta = [fmtDateTime(item.at), item.actor, item.meta]
          .filter(Boolean)
          .join(' - ');
        return `
          <div class="offset-audit-item">
            <strong>${escapeHtml(item.title)}</strong>
            ${meta ? `<span class="offset-audit-meta">${escapeHtml(meta)}</span>` : ''}
            ${item.note ? `<span class="offset-audit-note">${escapeHtml(item.note)}</span>` : ''}
          </div>`;
      })
      .join('')}`;
  wrap.hidden = false;
}

function renderOffsetWorkflowAudit(row) {
  const wrap = qs('offsetWorkflowAudit');
  if (!wrap) return;
  let title = '';
  let actor = '';
  let when = '';
  let note = '';
  if (row?.status === 'returned' || row?.returned_at || row?.return_note) {
    title = 'Returned to employee';
    actor = row.returned_by || '';
    when = row.returned_at || '';
    note = row.return_note || '';
  } else if (
    row?.status === 'cancelled' ||
    row?.cancelled_at ||
    row?.cancel_note
  ) {
    title = 'Cancelled';
    actor = row.cancelled_by || '';
    when = row.cancelled_at || '';
    note = row.cancel_note || '';
  } else if (row?.status === 'approved' || row?.approved_at) {
    title = 'Approved';
    actor = row.approved_by || '';
    when = row.approved_at || '';
    note = row.approved_note || '';
  }
  if (!title) {
    wrap.hidden = true;
    wrap.innerHTML = '';
    return;
  }
  const details = [actor ? `By ${actor}` : '', when ? fmtDateTime(when) : '']
    .filter(Boolean)
    .join(' - ');
  wrap.innerHTML = `
    <strong>${escapeHtml(title)}</strong>
    ${details ? `<span class="muted">${escapeHtml(details)}</span>` : ''}
    ${note ? `<div>${escapeHtml(note)}</div>` : ''}
    ${row?.status === 'returned' ? '<div class="muted">Edit and save the request or evidence to resubmit it for review.</div>' : ''}`;
  wrap.hidden = false;
}

function fillOffsetForm(row, detail = {}) {
  OFFSET_CURRENT_ROW = row || null;
  OFFSET_CURRENT_ID = row?.id || null;
  OFFSET_CURRENT_ACTION_EVENTS = Array.isArray(detail.actionEvents)
    ? detail.actionEvents
    : OFFSET_CURRENT_ACTION_EVENTS;
  OFFSET_CURRENT_VALIDATION_EVENTS = Array.isArray(detail.validationEvents)
    ? detail.validationEvents
    : OFFSET_CURRENT_VALIDATION_EVENTS;
  qs('offset_id').value = OFFSET_CURRENT_ID || '';
  qs('offset_user').value = row?.user_name || row?.user_upn || '';
  qs('offset_date').value = row?.interruption_date || '';
  qs('offset_start').value = String(row?.interruption_start_time || '').slice(
    0,
    5,
  );
  qs('offset_end').value = String(row?.interruption_end_time || '').slice(0, 5);
  setOffsetComputedHours(row?.requested_makeup_hours);
  qs('offset_makeup_date').value = row?.planned_makeup_date || '';
  clearOffsetCustomReasonOptions();
  ensureOffsetReasonOption(row?.reason || '');
  qs('offset_reason').value = row?.reason || '';
  qs('offset_remarks').value = row?.remarks || '';
  syncOffsetReasonRequirement();
  renderOffsetWorkflowAudit(row);
  renderOffsetAuditTimeline(
    row,
    OFFSET_CURRENT_ACTION_EVENTS,
    OFFSET_CURRENT_VALIDATION_EVENTS,
  );
  renderOffsetValidation(row?.validation_summary);
  syncOffsetFormAccess(row);
  updateOffsetEvidenceSection(row);
}

function clearOffsetForm() {
  OFFSET_CURRENT_ID = null;
  OFFSET_CURRENT_ROW = null;
  OFFSET_CREATE_IDEMPOTENCY_KEY = null;
  OFFSET_CURRENT_ACTION_EVENTS = [];
  OFFSET_CURRENT_VALIDATION_EVENTS = [];
  OFFSET_LAST_EVIDENCE_FILTER_SUMMARY = null;
  OFFSET_CANDIDATES = [];
  qs('formOffset')?.reset();
  clearOffsetCustomReasonOptions();
  qs('offset_id').value = '';
  qs('offsetFormStatus').textContent = '';
  setOffsetComputedHours(null);
  renderOffsetWorkflowAudit(null);
  renderOffsetAuditTimeline(null);
  renderOffsetValidation(null);
  renderOffsetEvidenceContext(null);
  syncOffsetFormAccess(null);
  updateOffsetEvidenceSection(null);
  renderOffsetEvidence([]);
}

function offsetRequestFilterQuery() {
  if (!canUseOffsetRequestFilters()) return '';
  const p = new URLSearchParams();
  const status = qs('offsetFilterStatus')?.value || '';
  const validation = qs('offsetFilterValidation')?.value || '';
  const from = qs('offsetFilterFrom')?.value || '';
  const to = qs('offsetFilterTo')?.value || '';
  const q = qs('offsetFilterQ')?.value.trim() || '';
  if (status) p.set('status', status);
  if (validation) p.set('validation', validation);
  if (from) p.set('from', from);
  if (to) p.set('to', to);
  if (q) p.set('q', q);
  const s = p.toString();
  return s ? `?${s}` : '';
}

function clearOffsetFilters() {
  [
    'offsetFilterStatus',
    'offsetFilterValidation',
    'offsetFilterFrom',
    'offsetFilterTo',
    'offsetFilterQ',
  ].forEach((id) => {
    const el = qs(id);
    if (el) el.value = '';
  });
  loadOffsetRequests();
}

async function loadOffsetRequests() {
  const tbody = qs('tbodyOffsetRequests');
  if (!tbody || !isOffsetPilotUser()) return;
  syncOffsetRoleUi();
  try {
    const r = await apiFetch(
      `/api/offset-requests${offsetRequestFilterQuery()}`,
    );
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      tbody.innerHTML = `<tr><td colspan="7" class="muted">Failed to load offset requests.</td></tr>`;
      return;
    }
    OFFSET_LAST_ROWS = Array.isArray(j.rows) ? j.rows : [];
    renderOffsetRequests(OFFSET_LAST_ROWS);
  } catch {
    tbody.innerHTML = `<tr><td colspan="7" class="muted">Error loading offset requests.</td></tr>`;
  }
}

function renderOffsetRequests(rows) {
  const tbody = qs('tbodyOffsetRequests');
  if (!tbody) return;
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="7" class="muted">No offset requests yet.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows
    .map((row) => {
      const time = `${String(row.interruption_start_time || '').slice(0, 5)}-${String(row.interruption_end_time || '').slice(0, 5)}`;
      const canManage = canManageOffsetRequest();
      const isPending = row.status === 'pending_review';
      const isReturned = row.status === 'returned';
      const isApproved = row.status === 'approved';
      const canRecheck = canManage && (isPending || isReturned || isApproved);
      const canApprove =
        canManage && isPending && row.validation_status === 'passed';
      const statusNote =
        row.status === 'returned' && row.return_note
          ? `Returned: ${row.return_note}`
          : row.status === 'approved' && row.approved_note
            ? `Approved: ${row.approved_note}`
            : row.status === 'cancelled' && row.cancel_note
              ? `Cancelled: ${row.cancel_note}`
              : '';
      return `
      <tr>
        <td data-label="User">${escapeHtml(row.user_name || row.user_upn || '')}</td>
        <td data-label="Interruption">${escapeHtml(row.interruption_date || '')} ${escapeHtml(time)}</td>
        <td data-label="Requested">${fmtHours(row.requested_makeup_hours)}</td>
        <td data-label="Make-up Date">${escapeHtml(row.planned_makeup_date || '')}</td>
        <td data-label="Validation">${offsetBadge(row.validation_status, OFFSET_VALIDATION_LABELS)}</td>
        <td data-label="Status">${offsetBadge(row.status)}${statusNote ? `<span class="pto-status-note">${escapeHtml(statusNote)}</span>` : ''}</td>
        <td class="cell-actions" data-label="Actions">
          <div class="pto-table-actions">
            <button type="button" class="ghost" data-offset-action="open" data-id="${row.id}">Open</button>
            ${canRecheck ? `<button type="button" class="ghost" data-offset-action="validate" data-id="${row.id}">Recheck</button>` : ''}
            ${canApprove ? `<button type="button" class="btn-approve" data-offset-action="approve" data-id="${row.id}">Approve</button>` : ''}
            ${canManage && isPending ? `<button type="button" class="btn-deny" data-offset-action="return" data-id="${row.id}">Return</button>` : ''}
            ${canManage && (isPending || isReturned) ? `<button type="button" class="btn-cancel" data-offset-action="cancel" data-id="${row.id}">Cancel</button>` : ''}
          </div>
        </td>
      </tr>`;
    })
    .join('');
}

async function openOffsetRequest(id) {
  const status = qs('offsetFormStatus');
  if (status) status.textContent = 'Loading request.';
  try {
    const r = await apiFetch(`/api/offset-requests/${id}`);
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      if (status) status.textContent = `Error: ${j.error || r.status}`;
      return;
    }
    fillOffsetForm(j.row, j);
    if (!isOffsetEvidenceLocked(j.row)) {
      renderOffsetEvidenceFromSaved(j.evidence || []);
      await loadOffsetEvidenceCandidates();
    }
    if (status) status.textContent = `Loaded request #${id}.`;
  } catch (err) {
    if (status) status.textContent = `Error: ${err.message}`;
  }
}

async function saveOffsetRequest() {
  const status = qs('offsetFormStatus');
  if (OFFSET_SAVE_IN_FLIGHT) return;
  if (!canEditOffsetRequest(OFFSET_CURRENT_ROW)) {
    if (status) status.textContent = 'This request is read-only.';
    return;
  }
  syncOffsetRequestedHours();
  syncOffsetReasonRequirement();
  const form = qs('formOffset');
  if (form && !form.reportValidity()) return;
  OFFSET_SAVE_IN_FLIGHT = true;
  syncOffsetFormAccess(OFFSET_CURRENT_ROW);
  if (status) status.textContent = 'Saving request.';
  try {
    await populatePtoUserList();
    const body = offsetRequestBodyFromForm();
    const id = OFFSET_CURRENT_ID;
    if (!id) body.idempotency_key = getOffsetCreateIdempotencyKey();
    const r = await apiFetch(
      id ? `/api/offset-requests/${id}` : '/api/offset-requests',
      {
        method: id ? 'PATCH' : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      },
    );
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      if (status) status.textContent = `Error: ${j.error || r.status}`;
      return;
    }
    OFFSET_CURRENT_ID = j.row.id;
    OFFSET_CREATE_IDEMPOTENCY_KEY = null;
    fillOffsetForm(j.row, j);
    renderOffsetEvidenceFromSaved(j.evidence || []);
    await loadOffsetRequests();
    await loadOffsetEvidenceCandidates();
    if (status) {
      status.textContent = j.idempotent
        ? `Loaded existing request #${j.row.id}.`
        : `Saved request #${j.row.id}.`;
    }
  } catch (err) {
    if (status) status.textContent = `Error: ${err.message}`;
  } finally {
    OFFSET_SAVE_IN_FLIGHT = false;
    syncOffsetFormAccess(OFFSET_CURRENT_ROW);
  }
}

function renderOffsetEvidenceFromSaved(rows) {
  if (!rows.length) {
    renderOffsetEvidence([]);
    return;
  }
  OFFSET_CANDIDATES = rows.map((row) => ({
    ...row,
    evidence_key:
      row.evidence_key || offsetEvidenceClientKey(row.task_id, row.changed_at),
    selected_allocated_hours: Number(row.allocated_hours || 0),
    remaining_hours: Number(row.eligible_hours || 0),
  }));
  renderOffsetEvidence(OFFSET_CANDIDATES);
}

async function loadOffsetEvidenceCandidates() {
  const status = qs('offsetEvidenceStatus');
  if (!OFFSET_CURRENT_ID) {
    if (status) status.textContent = 'Save a request first.';
    return;
  }
  if (isOffsetEvidenceLocked()) {
    if (status)
      status.textContent = 'Evidence editing is locked for this request.';
    return;
  }
  const blockers = offsetEvidenceBlockers();
  if (blockers.length) {
    if (status) status.textContent = offsetEvidenceBlockedMessage(blockers);
    return;
  }
  if (status) status.textContent = 'Loading TFS evidence.';
  try {
    const r = await apiFetch(
      `/api/offset-requests/${OFFSET_CURRENT_ID}/evidence-candidates`,
    );
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      if (j.validation) renderOffsetValidation(j.validation);
      OFFSET_LAST_EVIDENCE_FILTER_SUMMARY = j.filterSummary || null;
      renderOffsetEvidenceContext(OFFSET_LAST_EVIDENCE_FILTER_SUMMARY);
      if (status) status.textContent = `Error: ${j.error || r.status}`;
      return;
    }
    OFFSET_LAST_EVIDENCE_FILTER_SUMMARY = j.filterSummary || null;
    renderOffsetEvidenceContext(OFFSET_LAST_EVIDENCE_FILTER_SUMMARY);
    OFFSET_CANDIDATES = Array.isArray(j.candidates) ? j.candidates : [];
    renderOffsetEvidence(OFFSET_CANDIDATES);
    if (status) {
      const cutoffTime = String(
        j.evidenceWindow?.interruptionEndTime || '',
      ).slice(0, 5);
      const diagnostics = j.diagnostics || {};
      const positiveDeltaRows = Number(diagnostics.positiveDeltaRows || 0);
      const fullyUsedRows = Number(diagnostics.fullyUsedRows || 0);
      const cutoffExcludedRows = Number(diagnostics.cutoffExcludedRows || 0);
      status.textContent = OFFSET_CANDIDATES.length
        ? `${OFFSET_CANDIDATES.length} candidate row(s) loaded.`
        : positiveDeltaRows <= 0
          ? 'No positive TFS delta rows found for this make-up date.'
          : fullyUsedRows > 0
            ? `${fullyUsedRows} positive TFS delta row(s) found, but their available hours are already allocated to other active offset requests.`
            : j.evidenceWindow?.sameDay && cutoffTime && cutoffExcludedRows > 0
              ? `Positive TFS delta rows exist for this date, but none are on or after interruption end time ${cutoffTime}.`
              : 'No available TFS evidence rows found for this make-up date.';
    }
  } catch (err) {
    if (status) status.textContent = `Error: ${err.message}`;
  }
}

function renderOffsetEvidence(rows) {
  const tbody = qs('tbodyOffsetEvidence');
  if (!tbody) return;
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="6" class="muted">No evidence rows loaded.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows
    .map((row) => {
      const selected = Number(
        row.selected_allocated_hours || row.allocated_hours || 0,
      );
      const max = Math.max(
        selected,
        Number(row.remaining_hours ?? row.eligible_hours ?? 0),
      );
      const disabled = max <= 0 || offsetEvidenceBlockers().length > 0;
      return `
      <tr>
        <td data-label="Use"><input type="checkbox" class="offset-use" data-key="${escapeHtml(row.evidence_key || '')}" ${selected > 0 ? 'checked' : ''} ${disabled ? 'disabled' : ''} /></td>
        <td data-label="Changed">${escapeHtml(fmtDateTime(row.changed_at))}</td>
        <td data-label="Task">${renderIdTag(row.task_id)} ${escapeHtml(row.task_title || '')}</td>
        <td data-label="Ticket">${renderIdTag(row.ticket_id)} ${escapeHtml(row.ticket_title || '')}</td>
        <td data-label="Available Hours">${fmtHours(max)}</td>
        <td data-label="Allocate"><input type="number" class="offset-allocated" data-key="${escapeHtml(row.evidence_key || '')}" min="0" step="0.01" max="${escapeHtml(max)}" value="${selected > 0 ? selected : ''}" ${disabled ? 'disabled' : ''} /></td>
      </tr>`;
    })
    .join('');
}

function autoAllocateOffsetEvidence() {
  if (isOffsetEvidenceLocked()) {
    const status = qs('offsetEvidenceStatus');
    if (status)
      status.textContent = 'Evidence editing is locked for this request.';
    return;
  }
  const blockers = offsetEvidenceBlockers();
  if (blockers.length) {
    const status = qs('offsetEvidenceStatus');
    if (status) status.textContent = offsetEvidenceBlockedMessage(blockers);
    return;
  }
  const requested = Number(qs('offset_hours')?.value || 0);
  if (!Number.isFinite(requested) || requested <= 0) return;
  let remaining = requested;
  document.querySelectorAll('.offset-use').forEach((box) => {
    const key = box.dataset.key;
    const input = document.querySelector(
      `.offset-allocated[data-key="${CSS.escape(key)}"]`,
    );
    const row = OFFSET_CANDIDATES.find((x) => x.evidence_key === key);
    const max = Number(
      row?.remaining_hours ?? row?.eligible_hours ?? input?.max ?? 0,
    );
    const allocation = Math.min(max, remaining);
    box.checked = allocation > 0;
    if (input)
      input.value =
        allocation > 0 ? (Math.round(allocation * 100) / 100).toFixed(2) : '';
    remaining = Math.max(0, remaining - allocation);
  });
}

function collectOffsetEvidenceInput() {
  const evidence = [];
  document.querySelectorAll('.offset-use:checked').forEach((box) => {
    const key = box.dataset.key;
    const row = OFFSET_CANDIDATES.find((x) => x.evidence_key === key);
    const input = document.querySelector(
      `.offset-allocated[data-key="${CSS.escape(key)}"]`,
    );
    const allocatedHours = Number(input?.value || 0);
    if (!row || !Number.isFinite(allocatedHours) || allocatedHours <= 0) return;
    evidence.push({
      taskId: row.task_id,
      changedAt: row.changed_at,
      allocatedHours,
    });
  });
  return evidence;
}

async function validateOffsetEvidence() {
  const status = qs('offsetEvidenceStatus');
  if (!OFFSET_CURRENT_ID) {
    if (status) status.textContent = 'Save a request first.';
    return;
  }
  if (isOffsetEvidenceLocked()) {
    if (status)
      status.textContent = 'Evidence editing is locked for this request.';
    return;
  }
  const blockers = offsetEvidenceBlockers();
  if (blockers.length) {
    if (status) status.textContent = offsetEvidenceBlockedMessage(blockers);
    return;
  }
  const evidence = collectOffsetEvidenceInput();
  if (status) status.textContent = 'Validating current allocation.';
  try {
    const r = await apiFetch(
      `/api/offset-requests/${OFFSET_CURRENT_ID}/validate-evidence`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ evidence }),
      },
    );
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      if (j.validation) renderOffsetValidation(j.validation);
      if (status) status.textContent = `Error: ${j.error || r.status}`;
      return;
    }
    renderOffsetValidation(j.validation);
    const result = j.validation?.validationStatus || 'checked';
    if (status)
      status.textContent = `Current allocation validation: ${result}. Save evidence to persist it.`;
  } catch (err) {
    if (status) status.textContent = `Error: ${err.message}`;
  }
}

async function saveOffsetEvidence() {
  const status = qs('offsetEvidenceStatus');
  if (!OFFSET_CURRENT_ID) {
    if (status) status.textContent = 'Save a request first.';
    return;
  }
  if (isOffsetEvidenceLocked()) {
    if (status)
      status.textContent = 'Evidence editing is locked for this request.';
    return;
  }
  const blockers = offsetEvidenceBlockers();
  if (blockers.length) {
    if (status) status.textContent = offsetEvidenceBlockedMessage(blockers);
    return;
  }
  const evidence = collectOffsetEvidenceInput();
  if (status) status.textContent = 'Saving evidence.';
  try {
    const r = await apiFetch(
      `/api/offset-requests/${OFFSET_CURRENT_ID}/evidence`,
      {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ evidence }),
      },
    );
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      if (j.validation) renderOffsetValidation(j.validation);
      if (status) status.textContent = `Error: ${j.error || r.status}`;
      return;
    }
    fillOffsetForm(j.row, j);
    renderOffsetEvidenceFromSaved(j.evidence || []);
    await loadOffsetRequests();
    await loadOffsetEvidenceCandidates();
    if (status) status.textContent = 'Evidence saved and validation refreshed.';
  } catch (err) {
    if (status) status.textContent = `Error: ${err.message}`;
  }
}

function showOffsetReviewNotePanel(id, action) {
  const status = qs('offsetRequestActionStatus');
  const panel = qs('offsetReviewNotePanel');
  const title = qs('offsetReviewNoteTitle');
  const idInput = qs('offsetReviewNoteId');
  const actionInput = qs('offsetReviewNoteAction');
  const text = qs('offsetReviewNoteText');
  if (!panel || !idInput || !actionInput || !text) return;
  idInput.value = id;
  actionInput.value = action;
  text.value = '';
  panel.hidden = false;
  const copy =
    action === 'return'
      ? {
          title: 'Return',
          placeholder: 'Required: explain what the employee must correct',
          confirm: 'Return Request',
          status: `Add a return note for request #${id}.`,
        }
      : action === 'approve'
        ? {
            title: 'Approve',
            placeholder: 'Optional: add approval note',
            confirm: 'Approve Request',
            status: `Add an optional approval note for request #${id}.`,
          }
        : {
            title: 'Cancel',
            placeholder:
              'Optional: explain why this request is being cancelled',
            confirm: 'Cancel Request',
            status: `Add an optional cancel note for request #${id}.`,
          };
  if (title) title.textContent = `${copy.title} request #${id}`;
  text.placeholder = copy.placeholder;
  const confirm = qs('btnOffsetConfirmReviewNote');
  if (confirm) confirm.textContent = copy.confirm;
  if (status) status.textContent = copy.status;
  text.focus();
}

function hideOffsetReviewNotePanel() {
  const panel = qs('offsetReviewNotePanel');
  if (panel) panel.hidden = true;
  qs('offsetReviewNoteId').value = '';
  qs('offsetReviewNoteAction').value = '';
  qs('offsetReviewNoteText').value = '';
}

async function submitOffsetReviewNote() {
  const id = qs('offsetReviewNoteId')?.value;
  const action = qs('offsetReviewNoteAction')?.value;
  const note = qs('offsetReviewNoteText')?.value.trim() || '';
  const status = qs('offsetRequestActionStatus');
  if (!id || !action) return;
  if (action === 'return' && !note) {
    if (status) status.textContent = `Return request #${id}: note is required.`;
    qs('offsetReviewNoteText')?.focus();
    return;
  }
  await performOffsetAction(id, action, note);
  hideOffsetReviewNotePanel();
}

async function offsetAction(id, action, note = null) {
  if (action === 'approve' || action === 'return' || action === 'cancel') {
    showOffsetReviewNotePanel(id, action);
    return;
  }
  await performOffsetAction(id, action, note);
}

async function performOffsetAction(id, action, note = null) {
  const status = qs('offsetRequestActionStatus');
  const actionLabel =
    action === 'validate'
      ? 'Rechecking'
      : action === 'approve'
        ? 'Approving'
        : action === 'return'
          ? 'Returning'
          : 'Cancelling';
  if (status) status.textContent = `${actionLabel} request #${id}.`;
  try {
    const r = await apiFetch(`/api/offset-requests/${id}/${action}`, {
      method: action === 'validate' ? 'POST' : 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ note }),
    });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || !j.ok) {
      if (status)
        status.textContent = `Error on request #${id}: ${j.error || r.status}`;
      alert(`Error: ${j.error || r.status}`);
      return;
    }
    await loadOffsetRequests();
    if (OFFSET_CURRENT_ID === Number(id)) await openOffsetRequest(id);
    const resultStatus =
      j.row?.validation_status ||
      j.validation?.validationStatus ||
      j.row?.status ||
      '';
    const doneLabel =
      action === 'validate'
        ? `Rechecked request #${id}${resultStatus ? `: ${resultStatus}` : ''}.`
        : action === 'approve'
          ? `Approved request #${id}.`
          : action === 'return'
            ? `Returned request #${id}.`
            : `Cancelled request #${id}.`;
    if (status) status.textContent = doneLabel;
  } catch (err) {
    if (status) status.textContent = `Error on request #${id}: ${err.message}`;
    alert(`Error: ${err.message}`);
  }
}

qs('formOffset')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  await saveOffsetRequest();
});
qs('btnOffsetNew')?.addEventListener('click', clearOffsetForm);
qs('btnOffsetRefresh')?.addEventListener('click', loadOffsetRequests);
qs('btnOffsetClearFilters')?.addEventListener('click', clearOffsetFilters);
qs('btnOffsetLoadEvidence')?.addEventListener(
  'click',
  loadOffsetEvidenceCandidates,
);
qs('btnOffsetAutoAllocate')?.addEventListener(
  'click',
  autoAllocateOffsetEvidence,
);
qs('btnOffsetValidateEvidence')?.addEventListener('click', validateOffsetEvidence);
qs('btnOffsetSaveEvidence')?.addEventListener('click', saveOffsetEvidence);
qs('tbodyOffsetEvidence')?.addEventListener('input', (e) => {
  const input = e.target?.closest?.('.offset-allocated');
  if (!input) return;
  const key = input.dataset.key;
  const box = document.querySelector(
    `.offset-use[data-key="${CSS.escape(key)}"]`,
  );
  if (box && !box.disabled) box.checked = Number(input.value || 0) > 0;
});
qs('btnOffsetConfirmReviewNote')?.addEventListener(
  'click',
  submitOffsetReviewNote,
);
qs('btnOffsetCancelReviewNote')?.addEventListener('click', () => {
  const id = qs('offsetReviewNoteId')?.value;
  const action = qs('offsetReviewNoteAction')?.value;
  hideOffsetReviewNotePanel();
  const status = qs('offsetRequestActionStatus');
  const label =
    action === 'return'
      ? 'Return'
      : action === 'approve'
        ? 'Approve'
        : 'Cancel';
  if (status && id && action)
    status.textContent = `${label} cancelled for request #${id}.`;
});
qs('offset_start')?.addEventListener('input', syncOffsetRequestedHours);
qs('offset_end')?.addEventListener('input', syncOffsetRequestedHours);
qs('offset_reason')?.addEventListener('change', syncOffsetReasonRequirement);
[
  'offset_user',
  'offset_date',
  'offset_start',
  'offset_end',
  'offset_makeup_date',
  'offset_reason',
  'offset_remarks',
].forEach((id) => {
  qs(id)?.addEventListener('input', resetOffsetCreateIdempotencyKey);
  qs(id)?.addEventListener('change', resetOffsetCreateIdempotencyKey);
});
[
  'offsetFilterStatus',
  'offsetFilterValidation',
  'offsetFilterFrom',
  'offsetFilterTo',
].forEach((id) => qs(id)?.addEventListener('change', loadOffsetRequests));
qs('offsetFilterQ')?.addEventListener('change', loadOffsetRequests);
qs('offsetFilterQ')?.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') loadOffsetRequests();
});

document.addEventListener('click', async (e) => {
  const btn = e.target.closest('[data-offset-action]');
  if (!btn) return;
  const id = btn.dataset.id;
  const action = btn.dataset.offsetAction;
  if (!id || !action) return;
  if (action === 'open') await openOffsetRequest(id);
  else await offsetAction(id, action);
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

    const totalLogged = rows.reduce((acc, row) => {
      return acc + rowLoggedHours(row);
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
      workingHours - teamOffHours - individualPtoHours - totalLogged;

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

  const offsetTabButton = qs('tabBtnOffset');
  if (offsetTabButton) offsetTabButton.hidden = !OFFSET_UI_ENABLED;
  syncOffsetRoleUi();

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

  // PTO submission/list controls remain role-aware.
  updatePtoListControlVisibility();
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
  syncPtoViewButtons();
  updatePtoListControlVisibility();
  syncPtoRangeValidation();

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
  syncOffsetDateLimits(toStr);
  syncOffsetFormAccess(null);
  updateOffsetEvidenceSection(null);

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
