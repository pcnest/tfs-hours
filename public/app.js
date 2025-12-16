let APP_CFG = null;

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

function renderIdPill(id) {
  if (id === null || id === undefined || id === '') return '—';
  const href = workItemHref(id);
  const label = escapeHtml(id);
  if (href) {
    return `<a class="pill" href="${escapeHtml(
      href
    )}" target="_blank" rel="noopener noreferrer">${label}</a>`;
  }
  return `<span class="pill">${label}</span>`;
}

function buildCommonParams() {
  const p = new URLSearchParams();
  const add = (k, v) => {
    if (v !== null && v !== undefined && String(v).trim() !== '') p.set(k, v);
  };

  add('from', qs('from').value);
  add('to', qs('to').value);
  add('bucket', qs('bucket').value);
  add('assignedToUPN', qs('assignedToUPN').value);
  add('accountCode', qs('accountCode').value);

  return p;
}

function sumHours(rows) {
  return rows.reduce((acc, r) => acc + Number(r.hours || 0), 0);
}

function uniquePeople(rows) {
  const s = new Set(
    rows.map((r) => (r.assignedTo || '').trim()).filter(Boolean)
  );
  return s.size;
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
  const a = qs('tzLabelFrom');
  const b = qs('tzLabelTo');
  if (a) a.textContent = tz;
  if (b) b.textContent = tz;
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

function fmtDate(v) {
  if (!v) return '—';
  const d = new Date(v);
  if (isNaN(d.getTime())) return '—';

  const shifted = shiftDateByOffset(d, tzOffsetMinutes());
  return shifted.toISOString().slice(0, 10);
}

function fmtDateTime(v) {
  if (!v) return '—';
  const d = new Date(v);
  if (isNaN(d.getTime())) return '—';

  const off = tzOffsetMinutes(); // e.g. -480
  const shifted = shiftDateByOffset(d, off);

  // Use ISO formatting on the shifted date (gives us YYYY-MM-DD HH:mm)
  const s = shifted.toISOString().replace('T', ' ').slice(0, 16);

  return `${s} ${tzLabel()}`;
}

async function loadSummary() {
  qs(
    'tbodySummary'
  ).innerHTML = `<tr><td colspan="4" class="muted">Loading…</td></tr>`;

  const params = buildCommonParams();
  const r = await fetch(`/api/hours/summary?${params.toString()}`);
  const data = await r.json().catch(() => ({}));

  if (!r.ok || !data.ok) {
    qs(
      'tbodySummary'
    ).innerHTML = `<tr><td colspan="4" class="muted">Error: ${escapeHtml(
      data.error || `HTTP ${r.status}`
    )}</td></tr>`;
    return { ok: false };
  }

  const rows = data.rows || [];
  const total = sumHours(rows);
  const people = uniquePeople(rows);

  qs('m_totalHours').textContent = Number.isFinite(total)
    ? total.toFixed(2)
    : '—';
  qs('m_people').textContent = String(people);
  qs('m_rows').textContent = String(rows.length);

  if (!rows.length) {
    qs(
      'tbodySummary'
    ).innerHTML = `<tr><td colspan="4" class="muted">No rows.</td></tr>`;
    return { ok: true, bucket: data.bucket, from: data.from, to: data.to };
  }

  qs('tbodySummary').innerHTML = rows
    .map((x) => {
      const bucket = x.bucket ? fmtDate(x.bucket) : '—';
      return `
        <tr>
          <td>${escapeHtml(bucket)}</td>
          <td>${escapeHtml(x.assignedTo || '')}</td>
          <td>${x.accountCode ?? ''}</td>
          <td>${Number(x.hours || 0).toFixed(2)}</td>
        </tr>
      `;
    })
    .join('');

  return { ok: true, bucket: data.bucket, from: data.from, to: data.to };
}

async function loadEntries() {
  qs(
    'tbodyLatest'
  ).innerHTML = `<tr><td colspan="10" class="muted">Loading…</td></tr>`;

  const params = buildCommonParams();
  // entries endpoint doesn't need bucket, but harmless if present
  params.set('limit', qs('latestLimit')?.value || '200');

  const r = await fetch(`/api/hours/entries?${params.toString()}`);
  const data = await r.json().catch(() => ({}));

  if (!r.ok || !data.ok) {
    qs(
      'tbodyLatest'
    ).innerHTML = `<tr><td colspan="10" class="muted">Error: ${escapeHtml(
      data.error || `HTTP ${r.status}`
    )}</td></tr>`;
    qs('m_latestRows').textContent = '—';
    return { ok: false };
  }

  const rows = data.rows || [];
  qs('m_latestRows').textContent = String(rows.length);

  if (!rows.length) {
    qs(
      'tbodyLatest'
    ).innerHTML = `<tr><td colspan="10" class="muted">No rows.</td></tr>`;
    return { ok: true };
  }

  qs('tbodyLatest').innerHTML = rows
    .map(
      (x) => `
      <tr>
        <td>${renderIdPill(x.task_id)}</td>
        <td>${escapeHtml(x.task_title || '')}</td>
        <td>${escapeHtml(x.task_activity || '')}</td>
        <td>${escapeHtml(fmtDateTime(x.changed_at))}</td>
        <td>${Number(x.delta_hours || 0).toFixed(2)}</td>
        <td>${x.actual_hours ?? ''}</td>
        <td>${escapeHtml(x.task_assigned_to || '')}</td>
        <td>${renderIdPill(x.parent_id)} <span class="muted">${escapeHtml(
        x.parent_type || ''
      )}</span></td>
        <td>${escapeHtml(x.parent_title || '')}</td>
        <td>${x.account_code ?? ''}</td>
      </tr>
    `
    )
    .join('');

  return { ok: true, total: data.total };
}

async function loadAll() {
  qs('status').textContent = 'Loading…';

  const s = await loadSummary();
  await loadEntries();

  if (s?.ok) {
    qs('status').innerHTML = `Bucket <b>${escapeHtml(
      s.bucket
    )}</b> · Range <b>${escapeHtml(s.from)}</b> → <b>${escapeHtml(
      s.to
    )}</b> <span class="muted">(${escapeHtml(tzLabel())})</span>`;
  }
}

qs('btnLoad').addEventListener('click', async () => {
  await loadConfig();
  setTzLabels(); // optional but recommended (keeps UI labels correct if config changes)
  await loadAll(); // IMPORTANT: await so UI status + tables update in order
});

qs('btnExport').addEventListener('click', async () => {
  await loadConfig();
  setTzLabels();
  const params = buildCommonParams();
  window.location.href = `/api/hours/export.csv?${params.toString()}`;
});

// boot defaults: last 30 days (report TZ)
(async function boot() {
  await loadConfig();
  setTzLabels();

  const toStr = ymdTodayInReportTz();
  const fromStr = ymdAddDays(toStr, -29);

  qs('from').value = fromStr;
  qs('to').value = toStr;

  await loadAll();
})();
