// Promogen Intelligence — upload-based viewer
console.log('[app.js] script starting...');
window.__appjs_running__ = true;
window.App = {
  state: {
    accounts: [],
    segments: [],
    selectedAccountIds: new Set(),
    activeSegment: '',
    period: 'all',
    activeMetric: 'sessions',
    activeView: 'overview',
    yoyEnabled: false,
    accountsSegmentFilter: '',
    accountsNameFilter: '',
    dataRange: { min_week: null, max_week: null },
  },
  charts: {},
};

const byId = id => document.getElementById(id);
const qsa = sel => document.querySelectorAll(sel);

const fmt = n => {
  if (n == null || isNaN(n)) return '—';
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + 'k';
  return Number(n).toLocaleString('cs-CZ', { maximumFractionDigits: 1 });
};
const fmtPct = (n, plus = true) => {
  if (n == null || isNaN(n)) return '—';
  return `${plus && n > 0 ? '+' : ''}${n.toFixed(1)}%`;
};

async function api(path, opts = {}) {
  const r = await fetch(path, { headers: { 'Content-Type': 'application/json' }, ...opts });
  if (!r.ok) {
    const text = await r.text();
    throw new Error(`API ${path} ${r.status}: ${text.slice(0, 200)}`);
  }
  return r.json();
}

// ─────────────── Period helpers ───────────────

function periodRange() {
  const range = App.state.dataRange;
  if (!range.max_week) return { start: null, end: null };
  const end = range.max_week;
  const p = App.state.period;
  if (p === 'all') return { start: range.min_week, end };

  const endD = new Date(end);
  if (p.startsWith('last_')) {
    const weeks = parseInt(p.split('_')[1]);
    const startD = new Date(endD);
    startD.setDate(startD.getDate() - weeks * 7);
    return { start: startD.toISOString().slice(0,10), end };
  }
  if (p === 'ytd') {
    return { start: `${endD.getFullYear()}-01-01`, end };
  }
  return { start: range.min_week, end };
}

// ─────────────── Init ───────────────

async function init() {
  bindNav();
  bindFilters();
  bindUpload();

  await loadStatus();
  await loadAccounts();
  await loadSegments();
  await loadView('overview');
}

function bindNav() {
  qsa('.nav-item').forEach(btn => {
    btn.addEventListener('click', () => {
      qsa('.nav-item').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      qsa('.view').forEach(v => v.classList.remove('active'));
      const view = btn.dataset.view;
      byId('view-' + view).classList.add('active');
      App.state.activeView = view;
      loadView(view);
    });
  });
}

function bindFilters() {
  byId('filter-segment').addEventListener('change', e => {
    App.state.activeSegment = e.target.value;
    loadView(App.state.activeView);
  });
  byId('filter-period').addEventListener('change', e => {
    App.state.period = e.target.value;
    loadView(App.state.activeView);
  });
  byId('filter-yoy').addEventListener('change', e => {
    App.state.yoyEnabled = e.target.checked;
    loadView(App.state.activeView);
  });
  byId('btn-toggle-accounts').addEventListener('click', () => {
    byId('accounts-dropdown').classList.toggle('hidden');
  });
  document.addEventListener('click', e => {
    if (!e.target.closest('.filter-accounts')) {
      byId('accounts-dropdown').classList.add('hidden');
    }
  });
  byId('accounts-search').addEventListener('input', renderAccountsDropdown);
  byId('btn-select-all').addEventListener('click', () => {
    App.state.accounts.forEach(a => App.state.selectedAccountIds.add(a.property_id));
    renderAccountsDropdown(); updateSelectedCount(); loadView(App.state.activeView);
  });
  byId('btn-clear-all').addEventListener('click', () => {
    App.state.selectedAccountIds.clear();
    renderAccountsDropdown(); updateSelectedCount(); loadView(App.state.activeView);
  });
  byId('btn-select-segment').addEventListener('click', () => {
    if (!App.state.activeSegment) { alert('Vyber segment vlevo'); return; }
    App.state.accounts.forEach(a => {
      if (a.segments.includes(App.state.activeSegment)) {
        App.state.selectedAccountIds.add(a.property_id);
      }
    });
    renderAccountsDropdown(); updateSelectedCount(); loadView(App.state.activeView);
  });

  // Metric tabs
  qsa('.metric-tab').forEach(t => {
    t.addEventListener('click', () => {
      qsa('.metric-tab').forEach(x => x.classList.remove('active'));
      t.classList.add('active');
      App.state.activeMetric = t.dataset.metric;
      renderMainChart();
    });
  });
}

// ─────────────── Upload ───────────────

function bindUpload() {
  const zone = byId('upload-zone');
  const input = byId('csv-input');
  zone.addEventListener('click', () => input.click());
  zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('dragging'); });
  zone.addEventListener('dragleave', () => zone.classList.remove('dragging'));
  zone.addEventListener('drop', e => {
    e.preventDefault(); zone.classList.remove('dragging');
    if (e.dataTransfer.files[0]) handleUpload(e.dataTransfer.files[0]);
  });
  input.addEventListener('change', e => { if (e.target.files[0]) handleUpload(e.target.files[0]); });

  byId('btn-reset-data').addEventListener('click', async () => {
    if (!confirm('Smazat VŠECHNA data (importy + týdenní záznamy)? Segmenty a účty zůstanou.')) return;
    await api('/api/imports/reset', { method: 'POST' });
    await loadStatus();
    await loadView(App.state.activeView);
    loadImports();
  });
}

async function handleUpload(file) {
  const result = byId('upload-result');
  result.innerHTML = '<div class="loading">⏳ Nahrávám a parsuji…</div>';
  try {
    const form = new FormData();
    form.append('file', file);
    const r = await fetch('/api/upload', { method: 'POST', body: form });
    if (!r.ok) {
      const err = await r.text();
      result.innerHTML = `<div class="upload-error">❌ ${err}</div>`;
      return;
    }
    const data = await r.json();
    result.innerHTML = `
      <div class="upload-success">
        <strong>✅ Nahráno!</strong><br>
        <code>${data.rows.toLocaleString('cs-CZ')}</code> řádků,
        <code>${data.properties}</code> účtů,
        <code>${data.weeks}</code> týdnů (${data.min_week} → ${data.max_week})
        ${data.parse_errors > 0 ? `<br>⚠️ ${data.parse_errors} řádků s chybou (přeskočeno)` : ''}
      </div>
    `;
    await loadStatus();
    await loadAccounts();
    await loadSegments();
    loadImports();
    await loadView(App.state.activeView);
  } catch (e) {
    result.innerHTML = `<div class="upload-error">❌ ${e.message}</div>`;
  }
}

async function loadImports() {
  const data = await api('/api/imports');
  const wrap = byId('imports-list');
  if (!data.imports.length) {
    wrap.innerHTML = '<div class="empty-state">Zatím žádné importy.</div>';
    return;
  }
  wrap.innerHTML = `<table class="accounts-table">
    <thead><tr><th>Datum</th><th>Soubor</th><th>Řádky</th><th>Účtů</th><th>Týdnů</th><th>Rozsah</th><th></th></tr></thead>
    <tbody>${data.imports.map(i => `
      <tr>
        <td>${new Date(i.uploaded_at).toLocaleString('cs-CZ')}</td>
        <td><code>${i.filename}</code></td>
        <td>${(i.rows_imported || 0).toLocaleString('cs-CZ')}</td>
        <td>${i.properties_count || 0}</td>
        <td>${i.weeks_count || 0}</td>
        <td>${i.min_week || '—'} → ${i.max_week || '—'}</td>
        <td><button class="btn-mini" data-del="${i.id}" style="color:var(--danger)">×</button></td>
      </tr>`).join('')}</tbody></table>`;
  wrap.querySelectorAll('[data-del]').forEach(b => {
    b.addEventListener('click', async () => {
      if (!confirm('Smazat tento import?')) return;
      await api(`/api/imports/${b.dataset.del}`, { method: 'DELETE' });
      loadImports();
      loadStatus();
    });
  });
}

// ─────────────── Status & data load ───────────────

async function loadStatus() {
  const s = await api('/api/status');
  App.state.dataRange = s.data_range || {};
  byId('kpi-accounts').textContent = s.accounts;
  byId('kpi-segments').textContent = s.segments;
  byId('kpi-weeks').textContent = (s.data_range && s.data_range.rows) ? Math.round(s.data_range.rows / Math.max(1, s.data_range.properties)) : 0;
  byId('kpi-range').textContent = (s.data_range && s.data_range.min_week)
    ? `${s.data_range.min_week} → ${s.data_range.max_week}` : '—';
  byId('data-status').innerHTML = (s.data_range && s.data_range.rows)
    ? `📊 ${s.data_range.properties} účtů, ${s.data_range.rows.toLocaleString('cs-CZ')} řádků<br>${s.data_range.min_week} → ${s.data_range.max_week}`
    : '⚠️ Žádná data';
  byId('no-data-banner').classList.toggle('hidden', !!(s.data_range && s.data_range.rows));
  byId('data-summary').classList.toggle('hidden', !(s.data_range && s.data_range.rows));
}

async function loadAccounts() {
  App.state.accounts = await api('/api/accounts');
  renderAccountsDropdown();
  updateSelectedCount();
}

async function loadSegments() {
  App.state.segments = await api('/api/segments');
  const sel = byId('filter-segment');
  const populated = App.state.segments.filter(s => s.account_count > 0).sort((a,b) => b.account_count - a.account_count);
  sel.innerHTML = '<option value="">Všechny segmenty</option>' +
    populated.map(s => `<option value="${s.slug}">${s.icon} ${s.name} (${s.account_count})</option>`).join('');
}

function renderAccountsDropdown() {
  const search = (byId('accounts-search')?.value || '').toLowerCase();
  const wrap = byId('accounts-list');
  if (!wrap) return;
  const segMap = Object.fromEntries(App.state.segments.map(s => [s.slug, s]));
  const filtered = App.state.accounts.filter(a =>
    !search || a.display_name.toLowerCase().includes(search) || a.property_id.includes(search)
  );
  wrap.innerHTML = filtered.length ? filtered.map(a => {
    const checked = App.state.selectedAccountIds.has(a.property_id) ? 'checked' : '';
    const segs = (a.segments || []).map(s => `<span class="seg-tag">${segMap[s]?.icon || ''}</span>`).join('');
    return `<label class="account-row">
      <input type="checkbox" data-pid="${a.property_id}" ${checked}>
      <span class="acc-name">${a.display_name}</span>
      <span class="acc-segments">${segs}</span>
    </label>`;
  }).join('') : '<div class="empty-state" style="padding:10px">Žádné účty.</div>';
  wrap.querySelectorAll('input[type="checkbox"]').forEach(cb => {
    cb.addEventListener('change', e => {
      const pid = e.target.dataset.pid;
      if (e.target.checked) App.state.selectedAccountIds.add(pid);
      else App.state.selectedAccountIds.delete(pid);
      updateSelectedCount();
      loadView(App.state.activeView);
    });
  });
}

function updateSelectedCount() {
  byId('accounts-selected-count').textContent = App.state.selectedAccountIds.size;
}

// ─────────────── Views ───────────────

async function loadView(view) {
  if (view === 'overview') return loadOverview();
  if (view === 'segments') return loadSegmentsView();
  if (view === 'accounts') return renderAccountsTable();
  if (view === 'upload') return loadImports();
}

async function loadOverview() {
  await renderAccountStrip();
  await renderMainChart();
}

function effectivePropertyIds() {
  if (App.state.activeSegment) {
    return App.state.accounts
      .filter(a => (a.segments || []).includes(App.state.activeSegment))
      .map(a => a.property_id);
  }
  if (App.state.selectedAccountIds.size > 0) {
    return [...App.state.selectedAccountIds];
  }
  // Default: show TOP 20 accounts (or all if fewer) when nothing is selected
  return App.state.accounts.slice(0, 20).map(a => a.property_id);
}

async function renderAccountStrip() {
  const ids = effectivePropertyIds();
  const wrap = byId('account-strip');
  if (!ids.length) {
    wrap.innerHTML = '<div class="empty-state">Žádné účty. Nahraj CSV.</div>';
    return;
  }
  // Show notice if showing default top-20 (no manual selection)
  const isDefault = !App.state.activeSegment && App.state.selectedAccountIds.size === 0;
  if (isDefault && App.state.accounts.length > 20) {
    wrap.innerHTML = `<div style="background:rgba(255,107,53,0.1); border:1px solid var(--accent); border-radius:6px; padding:10px; margin-bottom:10px; font-size:12px">
      📌 Zobrazujem top 20 účtů z ${App.state.accounts.length}. Pro výběr klikni „Vybrat účty ▾" nebo vyber segment.
    </div>`;
  } else {
    wrap.innerHTML = '';
  }
  const { start, end } = periodRange();
  const data = await api(`/api/data/account_strip?property_ids=${ids.join(',')}${start ? '&start='+start : ''}${end ? '&end='+end : ''}`);
  wrap.innerHTML = '';
  // Don't reset wrap.innerHTML — append to (possible) notice
  data.accounts.forEach((a, i) => {
    if (a.no_data) {
      wrap.innerHTML += `<div class="account-row-strip"><div class="acc-strip-name">${a.display_name}<span class="pid">${a.property_id}</span></div><div style="grid-column:2/-1; color:var(--text-faint)">žádná data v období</div></div>`;
      return;
    }
    const k = a.kpis;
    const trendCol = a.trend_pct == null ? '#94a3b8' : a.trend_pct >= 0 ? '#22c55e' : '#ef4444';
    wrap.innerHTML += `
      <div class="account-row-strip">
        <div class="acc-strip-name">${a.display_name}<span class="pid">${a.property_id} · ${a.parent_account || ''}</span></div>
        <div class="acc-strip-kpi"><div class="kpi-label">Návštěvnost</div><div class="kpi-val">${fmt(k.sessions)}</div></div>
        <div class="acc-strip-kpi"><div class="kpi-label">Konverze</div><div class="kpi-val">${fmt(k.conversions)}</div></div>
        <div class="acc-strip-kpi"><div class="kpi-label">Konv. míra</div><div class="kpi-val">${k.conv_rate}%</div></div>
        <div class="acc-strip-kpi"><div class="kpi-label">Týdnů</div><div class="kpi-val">${k.weeks}</div></div>
        <div class="acc-strip-spark"><canvas id="spark-${i}" height="36"></canvas></div>
        <div class="acc-strip-trend" style="color:${trendCol}; font-weight:600">${a.trend_pct == null ? '—' : fmtPct(a.trend_pct) + ' (4t)'}</div>
      </div>`;
  });
  data.accounts.forEach((a, i) => {
    if (a.no_data || !a.sparkline?.length) return;
    const c = byId(`spark-${i}`)?.getContext('2d');
    if (!c) return;
    new Chart(c, {
      type: 'line',
      data: { labels: a.sparkline.map((_,j)=>j), datasets: [{ data: a.sparkline, borderColor: '#FF6B35', borderWidth: 1.5, fill: false, pointRadius: 0, tension: 0.3 }] },
      options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: { enabled: false } }, scales: { x: { display: false }, y: { display: false } } },
    });
  });
}

async function renderMainChart() {
  const ids = effectivePropertyIds();
  const canvas = byId('main-chart');
  if (!canvas) return;
  if (App.charts.main) { App.charts.main.destroy(); delete App.charts.main; }

  if (!ids.length) {
    canvas.getContext('2d').clearRect(0, 0, canvas.width, canvas.height);
    return;
  }
  const { start, end } = periodRange();
  const yoy = App.state.yoyEnabled ? '&yoy=true' : '';
  const params = `property_ids=${ids.join(',')}&metric=${App.state.activeMetric}${start ? '&start='+start : ''}${end ? '&end='+end : ''}${yoy}`;
  const data = await api(`/api/data/timeseries?${params}`);

  const colors = ['#FF6B35', '#22c55e', '#f59e0b', '#ef4444', '#06b6d4', '#a855f7', '#ec4899', '#84cc16', '#3b82f6', '#f43f5e', '#fbbf24', '#10b981'];

  // Compute SUM line + trendline
  const dateMap = {};
  data.series.forEach(s => s.data.forEach(d => { dateMap[d.week_start] = (dateMap[d.week_start] || 0) + d.value; }));
  const sumPoints = Object.entries(dateMap).sort((a,b) => a[0].localeCompare(b[0])).map(([w,v]) => ({ x: w, y: v }));

  const datasets = [];
  if (sumPoints.length && ids.length > 1) {
    datasets.push({
      label: 'Součet',
      data: sumPoints,
      borderColor: '#FF6B35',
      backgroundColor: 'rgba(255,107,53,0.15)',
      borderWidth: 3, fill: true, pointRadius: 0, tension: 0.3, order: 0,
    });

    if (sumPoints.length >= 2) {
      const ys = sumPoints.map(p => p.y);
      const xs = sumPoints.map((_, i) => i);
      const n = ys.length;
      const sumX = xs.reduce((a,b) => a+b, 0);
      const sumY = ys.reduce((a,b) => a+b, 0);
      const sumXY = xs.reduce((s, x, i) => s + x*ys[i], 0);
      const sumX2 = xs.reduce((s, x) => s + x*x, 0);
      const slope = (n*sumXY - sumX*sumY) / (n*sumX2 - sumX*sumX || 1);
      const intercept = (sumY - slope*sumX) / n;
      const avg = sumY / n;
      const totalPct = avg > 0 ? slope/avg*100 * n : 0;
      const tcolor = totalPct > 5 ? '#22c55e' : totalPct < -5 ? '#ef4444' : '#94a3b8';
      datasets.push({
        label: `Trend (${fmtPct(totalPct)} za období)`,
        data: xs.map(x => ({ x: sumPoints[x].x, y: slope*x + intercept })),
        borderColor: tcolor, borderDash: [6,4], borderWidth: 2, fill: false, pointRadius: 0, order: 1,
      });
    }
  }

  data.series.forEach((s, i) => {
    if (!s.data.length) return;
    datasets.push({
      label: s.display_name,
      data: s.data.map(d => ({ x: d.week_start, y: d.value })),
      borderColor: ids.length > 1 ? colors[i % colors.length] + 'cc' : '#FF6B35',
      borderWidth: ids.length > 1 ? 1.5 : 2,
      fill: false, pointRadius: ids.length > 1 ? 0 : 2, tension: 0.2,
      order: 2,
    });
  });

  if (data.yoy) {
    data.yoy.forEach((s, i) => {
      if (!s.data.length) return;
      datasets.push({
        label: `${s.display_name} (loňský rok)`,
        data: s.data.map(d => ({ x: d.week_start, y: d.value })),
        borderColor: colors[i % colors.length] + '55',
        borderDash: [4,4], borderWidth: 1.5, fill: false, pointRadius: 0, tension: 0.2,
        order: 3,
      });
    });
  }

  App.charts.main = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: { datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { position: 'bottom', labels: { color: '#e2e8f0', boxWidth: 10, font: { size: 11 } } },
        tooltip: { backgroundColor: '#1a2230', borderColor: '#FF6B35', borderWidth: 1, padding: 10 },
      },
      scales: {
        x: { type: 'time', time: { unit: 'week' }, ticks: { color: '#94a3b8' }, grid: { color: '#1f2a3a' } },
        y: { ticks: { color: '#94a3b8' }, grid: { color: '#1f2a3a' } },
      },
    },
  });
}

// ─────────────── Segments view ───────────────

async function loadSegmentsView() {
  const wrap = byId('segments-detail');
  const populated = App.state.segments.filter(s => s.account_count > 0).sort((a,b) => b.account_count - a.account_count);
  if (!populated.length) {
    wrap.innerHTML = '<div class="empty-state">Žádné segmenty s účty. Otevři kartu Účty a přiřaď.</div>';
    return;
  }
  wrap.innerHTML = populated.map(s => `
    <div class="segment-row" data-slug="${s.slug}">
      <div class="seg-row-header">
        <span class="sb-icon">${s.icon}</span>
        <div class="seg-row-title">
          <div class="sd-name">${s.name}</div>
          <div style="font-size:11px; color:var(--text-faint)">${s.account_count} účtů</div>
        </div>
        <button class="btn btn-light btn-mini seg-toggle">Otevřít detail ▾</button>
      </div>
      <div class="seg-row-body hidden">
        <div class="seg-row-summary" id="sum-${s.slug}">Načítám…</div>
        <div class="seg-row-chart-wrap"><canvas id="seg-chart-${s.slug}"></canvas></div>
        <div class="seg-row-accounts" id="seg-accs-${s.slug}"></div>
      </div>
    </div>
  `).join('');

  wrap.querySelectorAll('.segment-row').forEach(row => {
    row.querySelector('.seg-toggle').addEventListener('click', async () => {
      const body = row.querySelector('.seg-row-body');
      const wasHidden = body.classList.contains('hidden');
      body.classList.toggle('hidden');
      row.querySelector('.seg-toggle').textContent = wasHidden ? 'Sbalit ▴' : 'Otevřít detail ▾';
      if (wasHidden) await renderSegmentDetail(row.dataset.slug);
    });
  });
}

async function renderSegmentDetail(slug) {
  const { start, end } = periodRange();
  const yoy = App.state.yoyEnabled ? '&yoy=true' : '';
  const params = `segment=${slug}${start ? '&start='+start : ''}${end ? '&end='+end : ''}${yoy}`;
  const data = await api(`/api/data/segment_rollup?${params}`);
  if (!data.available) {
    byId(`sum-${slug}`).innerHTML = '<em>Žádné účty v segmentu.</em>';
    return;
  }
  const o = data.overall;
  const yoyOverall = data.yoy_overall;
  const sessYoY = yoyOverall ? ((o.sessions - yoyOverall.sessions) / yoyOverall.sessions * 100) : null;
  const convYoY = yoyOverall ? ((o.conversions - yoyOverall.conversions) / yoyOverall.conversions * 100) : null;
  const cYoY = yoyOverall ? (o.conv_rate - yoyOverall.conv_rate) : null;

  byId(`sum-${slug}`).innerHTML = `
    <div style="display:flex; gap:24px; flex-wrap:wrap; padding-top:8px">
      <div><strong>${data.n_accounts}</strong> účtů</div>
      <div>Návštěvnost: <strong>${fmt(o.sessions)}</strong>${sessYoY != null ? ` <span style="color:${sessYoY>=0?'#22c55e':'#ef4444'}">${fmtPct(sessYoY)} YoY</span>` : ''}</div>
      <div>Konverze: <strong>${fmt(o.conversions)}</strong>${convYoY != null ? ` <span style="color:${convYoY>=0?'#22c55e':'#ef4444'}">${fmtPct(convYoY)} YoY</span>` : ''}</div>
      <div>Konv. míra: <strong>${o.conv_rate}%</strong>${cYoY != null ? ` <span style="color:${cYoY>=0?'#22c55e':'#ef4444'}">${cYoY>=0?'+':''}${cYoY.toFixed(2)} pp</span>` : ''}</div>
    </div>
  `;

  // Chart: segment sum + per-account lines
  const canvas = byId(`seg-chart-${slug}`);
  if (App.charts['seg-' + slug]) App.charts['seg-' + slug].destroy();
  const colors = ['#22c55e', '#f59e0b', '#ef4444', '#06b6d4', '#a855f7', '#ec4899', '#84cc16', '#3b82f6', '#f43f5e', '#fbbf24'];
  const datasets = [
    { label: 'Součet segmentu', data: data.series.map(s => ({ x: s.week_start, y: s.sessions })),
      borderColor: '#FF6B35', backgroundColor: 'rgba(255,107,53,0.15)', fill: true, borderWidth: 3, pointRadius: 0, tension: 0.3, order: 0 },
    ...data.per_account.slice(0, 20).map((acc, i) => ({
      label: acc.display_name,
      data: acc.weekly.map(w => ({ x: w.week_start, y: w.sessions })),
      borderColor: colors[i % colors.length] + 'aa', borderWidth: 1, fill: false, pointRadius: 0, tension: 0.2, order: 2,
    })),
  ];
  if (data.yoy_series) {
    datasets.push({
      label: 'Loňský rok (součet)',
      data: data.yoy_series.map(s => ({ x: s.week_start, y: s.sessions })),
      borderColor: '#a855f7', borderDash: [4,4], borderWidth: 2, fill: false, pointRadius: 0, tension: 0.3, order: 1,
    });
  }

  App.charts['seg-' + slug] = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: { datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { position: 'bottom', labels: { color: '#e2e8f0', boxWidth: 10, font: { size: 10 } } },
        tooltip: { backgroundColor: '#1a2230', borderColor: '#FF6B35', borderWidth: 1, padding: 10 },
      },
      scales: {
        x: { type: 'time', time: { unit: 'week' }, ticks: { color: '#94a3b8' }, grid: { color: '#1f2a3a' } },
        y: { ticks: { color: '#94a3b8' }, grid: { color: '#1f2a3a' } },
      },
    },
  });

  // Per-account table
  byId(`seg-accs-${slug}`).innerHTML = `
    <table class="accounts-table" style="margin-top:8px">
      <thead><tr><th>Účet</th><th style="text-align:right">Návštěvnost</th><th style="text-align:right">Konverze</th><th style="text-align:right">Konv. míra</th></tr></thead>
      <tbody>${data.per_account.map(a => `
        <tr><td>${a.display_name}<br><code style="font-size:10px">${a.property_id}</code></td>
        <td style="text-align:right">${fmt(a.sessions)}</td>
        <td style="text-align:right">${fmt(a.conversions)}</td>
        <td style="text-align:right">${a.conv_rate}%</td></tr>`).join('')}
      </tbody>
    </table>
  `;
}

// ─────────────── Accounts view ───────────────

async function renderAccountsTable() {
  await loadAccounts();
  const wrap = byId('accounts-table-wrap');
  if (!App.state.accounts.length) {
    wrap.innerHTML = '<div class="empty-state">Zatím žádné účty. Nahraj CSV.</div>';
    return;
  }
  const segMap = Object.fromEntries(App.state.segments.map(s => [s.slug, s]));

  // Segment filter dropdown
  const segFilter = byId('accounts-segment-filter');
  if (segFilter) {
    const populated = App.state.segments.filter(s => s.account_count > 0).sort((a,b) => b.account_count - a.account_count);
    const cur = App.state.accountsSegmentFilter;
    segFilter.innerHTML = `<option value="">Všechny segmenty (${App.state.accounts.length})</option>` +
      populated.map(s => `<option value="${s.slug}" ${cur === s.slug ? 'selected' : ''}>${s.icon} ${s.name} (${s.account_count})</option>`).join('') +
      '<option value="__none__">Bez segmentu</option>';
    segFilter.onchange = () => { App.state.accountsSegmentFilter = segFilter.value; renderAccountsTable(); };
  }
  const nameFilter = byId('accounts-name-filter');
  if (nameFilter) {
    nameFilter.value = App.state.accountsNameFilter;
    nameFilter.oninput = () => { App.state.accountsNameFilter = nameFilter.value; renderAccountsTable(); };
  }

  let filtered = App.state.accounts;
  if (App.state.accountsSegmentFilter === '__none__') filtered = filtered.filter(a => !a.segments?.length);
  else if (App.state.accountsSegmentFilter) filtered = filtered.filter(a => (a.segments || []).includes(App.state.accountsSegmentFilter));
  if (App.state.accountsNameFilter) {
    const q = App.state.accountsNameFilter.toLowerCase();
    filtered = filtered.filter(a => a.display_name.toLowerCase().includes(q) || a.property_id.includes(q));
  }
  byId('accounts-filter-count').textContent = `${filtered.length} / ${App.state.accounts.length}`;

  const bulkBar = `<div id="bulk-action-bar" style="background:var(--bg-elevated); padding:10px 14px; border-radius:8px; margin-bottom:10px; display:none; align-items:center; gap:10px; flex-wrap:wrap">
    <span><strong id="bulk-count">0</strong> vybraných</span>
    <select id="bulk-segment-select" style="min-width:240px">
      <option value="">— vyber segment —</option>
      ${App.state.segments.map(s => `<option value="${s.slug}">${s.icon} ${s.name}</option>`).join('')}
    </select>
    <button class="btn btn-primary" id="bulk-replace-btn">Přesunout (nahradit)</button>
    <button class="btn btn-light" id="bulk-add-btn">Přidat (vedle)</button>
    <button class="btn-mini" id="bulk-cancel-btn">Zrušit</button>
  </div>`;

  let html = bulkBar + `<table class="accounts-table">
    <thead><tr><th><input type="checkbox" id="bulk-check-all"></th><th>Property ID</th><th>Název</th><th>Účet</th><th>Segmenty</th></tr></thead>
    <tbody>`;
  filtered.forEach(a => {
    const segs = (a.segments || []).map(slug => {
      const s = segMap[slug];
      return s ? `<span class="acc-seg-pill">${s.icon} ${s.name}<span class="x" data-rm-pid="${a.property_id}" data-rm-slug="${slug}">×</span></span>` : '';
    }).join('');
    html += `<tr>
      <td><input type="checkbox" class="bulk-check" data-pid="${a.property_id}"></td>
      <td><code>${a.property_id}</code></td>
      <td>${a.display_name}</td>
      <td>${a.parent_account || ''}</td>
      <td><div class="acc-segs-cell">${segs}<span class="acc-seg-pill adder" data-add-pid="${a.property_id}">+ přidat</span></div></td>
    </tr>`;
  });
  html += '</tbody></table>';
  wrap.innerHTML = html;

  // Bulk actions
  const bar = byId('bulk-action-bar');
  const updateBar = () => {
    const sel = wrap.querySelectorAll('.bulk-check:checked');
    bar.style.display = sel.length ? 'flex' : 'none';
    byId('bulk-count').textContent = sel.length;
  };
  wrap.querySelectorAll('.bulk-check').forEach(cb => cb.addEventListener('change', updateBar));
  byId('bulk-check-all').addEventListener('change', e => {
    wrap.querySelectorAll('.bulk-check').forEach(c => c.checked = e.target.checked);
    updateBar();
  });
  const doBulk = async (replace) => {
    const slug = byId('bulk-segment-select').value;
    if (!slug) { alert('Vyber segment'); return; }
    const ids = [...wrap.querySelectorAll('.bulk-check:checked')].map(c => c.dataset.pid);
    if (!ids.length) return;
    await api('/api/accounts/bulk_assign', {
      method: 'POST',
      body: JSON.stringify({ property_ids: ids, segment_slug: slug, replace }),
    });
    await loadSegments();
    renderAccountsTable();
  };
  byId('bulk-replace-btn').addEventListener('click', () => doBulk(true));
  byId('bulk-add-btn').addEventListener('click', () => doBulk(false));
  byId('bulk-cancel-btn').addEventListener('click', () => {
    wrap.querySelectorAll('.bulk-check').forEach(c => c.checked = false);
    byId('bulk-check-all').checked = false;
    updateBar();
  });

  // Single segment removal
  wrap.querySelectorAll('[data-rm-pid]').forEach(x => {
    x.addEventListener('click', async () => {
      await api(`/api/accounts/${x.dataset.rmPid}/segments/${x.dataset.rmSlug}`, { method: 'DELETE' });
      await loadSegments();
      renderAccountsTable();
    });
  });

  // Add segment popover
  wrap.querySelectorAll('[data-add-pid]').forEach(b => {
    b.addEventListener('click', e => {
      e.stopPropagation();
      document.querySelectorAll('.seg-add-popover').forEach(p => p.remove());
      const pid = b.dataset.addPid;
      const account = App.state.accounts.find(a => a.property_id === pid);
      const current = new Set(account?.segments || []);
      const pop = document.createElement('div');
      pop.className = 'seg-add-popover';
      pop.innerHTML = `<div style="font-weight:600; margin-bottom:8px">${account?.display_name}</div>
        <div style="max-height:300px; overflow-y:auto">
          ${App.state.segments.map(s => `<label class="seg-pop-row">
            <input type="checkbox" data-slug="${s.slug}" ${current.has(s.slug) ? 'checked' : ''}>
            <span>${s.icon} ${s.name}</span>
            <span style="margin-left:auto; font-size:10px; color:var(--text-faint)">${s.account_count}</span>
          </label>`).join('')}
        </div>
        <div style="display:flex; gap:6px; margin-top:8px">
          <button class="btn-mini seg-pop-save">Uložit</button>
          <button class="btn-mini seg-pop-close">Zrušit</button>
        </div>`;
      const r = b.getBoundingClientRect();
      pop.style.cssText = `position:fixed; top:${r.bottom + 6}px; left:${Math.min(r.left, window.innerWidth - 320)}px; z-index:1000`;
      document.body.appendChild(pop);
      pop.querySelector('.seg-pop-close').addEventListener('click', () => pop.remove());
      pop.querySelector('.seg-pop-save').addEventListener('click', async () => {
        const checked = new Set([...pop.querySelectorAll('input:checked')].map(i => i.dataset.slug));
        for (const slug of checked) if (!current.has(slug)) await api(`/api/accounts/${pid}/segments`, { method: 'POST', body: JSON.stringify({ segment_slug: slug }) });
        for (const slug of current) if (!checked.has(slug)) await api(`/api/accounts/${pid}/segments/${slug}`, { method: 'DELETE' });
        pop.remove();
        await loadSegments();
        renderAccountsTable();
      });
      setTimeout(() => {
        document.addEventListener('click', function close(e) {
          if (!pop.contains(e.target)) { pop.remove(); document.removeEventListener('click', close); }
        });
      }, 0);
    });
  });
}

window.init = init;
window.loadView = loadView;
window.updateSelectedCount = updateSelectedCount;
document.addEventListener('DOMContentLoaded', init);
