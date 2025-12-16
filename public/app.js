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

function buildParams() {
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
    rows.map((r) => r.assignedToUPN || r.assignedTo || '').filter(Boolean)
  );
  return s.size;
}

async function load() {
  qs('status').textContent = 'Loading…';
  qs(
    'tbody'
  ).innerHTML = `<tr><td colspan="5" class="muted">Loading…</td></tr>`;

  const params = buildParams();
  const r = await fetch(`/api/hours/summary?${params.toString()}`);
  const data = await r.json().catch(() => ({}));

  if (!r.ok || !data.ok) {
    qs('status').textContent = `Error: ${data.error || `HTTP ${r.status}`}`;
    return;
  }

  const rows = data.rows || [];
  const total = sumHours(rows);
  const people = uniquePeople(rows);

  qs('m_totalHours').textContent = Number.isFinite(total)
    ? total.toFixed(2)
    : '—';
  qs('m_people').textContent = String(people);
  qs('m_rows').textContent = String(rows.length);

  qs('status').innerHTML = `Bucket <b>${escapeHtml(
    data.bucket
  )}</b> · Range <b>${escapeHtml(data.from)}</b> → <b>${escapeHtml(
    data.to
  )}</b>`;

  if (!rows.length) {
    qs(
      'tbody'
    ).innerHTML = `<tr><td colspan="5" class="muted">No rows.</td></tr>`;
    return;
  }

  qs('tbody').innerHTML = rows
    .map((x) => {
      const bucket = x.bucket
        ? new Date(x.bucket).toISOString().slice(0, 10)
        : '—';
      return `
      <tr>
        <td>${escapeHtml(bucket)}</td>
        <td>${escapeHtml(x.assignedTo || '')}</td>
        <td>${escapeHtml(x.assignedToUPN || '')}</td>
        <td>${x.accountCode ?? ''}</td>
        <td>${Number(x.hours || 0).toFixed(2)}</td>
      </tr>
    `;
    })
    .join('');
}

qs('btnLoad').addEventListener('click', async () => {
  await loadConfig();
  load();
});

qs('btnExport').addEventListener('click', async () => {
  await loadConfig();
  const params = buildParams();
  window.location.href = `/api/hours/export.csv?${params.toString()}`;
});

// boot defaults: last 30 days (UTC)
(function boot() {
  const today = new Date();
  const to = new Date(
    Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate())
  );
  const from = new Date(to.getTime() - 29 * 86400 * 1000);

  qs('from').value = from.toISOString().slice(0, 10);
  qs('to').value = to.toISOString().slice(0, 10);

  load();
})();
