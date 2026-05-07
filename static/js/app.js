// GA4 Market Intelligence - frontend
const App = {
  state: {
    accounts: [],
    segments: [],
    selectedAccountIds: new Set(),
    activeSegment: '',
    period: 30,
    activeMetric: 'sessions',
    activeView: 'overview',
    activeInsightType: '',
  },
  charts: {},
};

// ─────────────── Helpers ───────────────

function $(id) { return document.getElementById(id); }
function $$(sel) { return document.querySelectorAll(sel); }
function fmt(n) {
  if (n == null || isNaN(n)) return '—';
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + 'k';
  return n.toLocaleString('cs-CZ', { maximumFractionDigits: 1 });
}
function fmtPct(n, plus = true) {
  if (n == null || isNaN(n)) return '—';
  const sign = plus && n > 0 ? '+' : '';
  return `${sign}${n.toFixed(1)}%`;
}
function daysAgoISO(n) {
  const d = new Date(); d.setDate(d.getDate() - n);
  return d.toISOString().slice(0, 10);
}
function todayISO() { return new Date().toISOString().slice(0, 10); }

async function api(path, opts = {}) {
  const r = await fetch(path, { headers: { 'Content-Type': 'application/json' }, ...opts });
  if (!r.ok) throw new Error(`API ${path} ${r.status}`);
  return r.json();
}

// ─────────────── Init ───────────────

async function init() {
  bindNav();
  bindFilters();
  bindControls();
  await loadStatus();
  await loadAccounts();
  await loadSegments();
  await loadView('overview');
  // Poll status every 30s for agent activity
  setInterval(loadStatus, 30000);
  setInterval(() => { if (App.state.activeView === 'agents') loadAgentActivity(); }, 10000);
}

function bindNav() {
  $$('.nav-item').forEach(btn => {
    btn.addEventListener('click', () => {
      $$('.nav-item').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      $$('.view').forEach(v => v.classList.remove('active'));
      const view = btn.dataset.view;
      $('view-' + view).classList.add('active');
      App.state.activeView = view;
      loadView(view);
    });
  });
}

function bindFilters() {
  $('filter-segment').addEventListener('change', e => {
    App.state.activeSegment = e.target.value;
    loadView(App.state.activeView);
  });
  $('filter-period').addEventListener('change', e => {
    App.state.period = parseInt(e.target.value);
    loadView(App.state.activeView);
  });
  $('btn-toggle-accounts').addEventListener('click', () => {
    $('accounts-dropdown').classList.toggle('hidden');
  });
  document.addEventListener('click', e => {
    if (!e.target.closest('.filter-accounts')) {
      $('accounts-dropdown').classList.add('hidden');
    }
  });
  $('accounts-search').addEventListener('input', renderAccountsDropdown);
  $('btn-select-all').addEventListener('click', () => {
    App.state.accounts.forEach(a => App.state.selectedAccountIds.add(a.property_id));
    renderAccountsDropdown(); updateSelectedCount(); loadView(App.state.activeView);
  });
  $('btn-clear-all').addEventListener('click', () => {
    App.state.selectedAccountIds.clear();
    renderAccountsDropdown(); updateSelectedCount(); loadView(App.state.activeView);
  });
  $('btn-select-segment').addEventListener('click', () => {
    if (!App.state.activeSegment) { alert('Vyber segment vo filtri vľavo'); return; }
    App.state.accounts.forEach(a => {
      if (a.segments.includes(App.state.activeSegment)) {
        App.state.selectedAccountIds.add(a.property_id);
      }
    });
    renderAccountsDropdown(); updateSelectedCount(); loadView(App.state.activeView);
  });
}

function bindControls() {
  $('btn-refresh').addEventListener('click', async () => {
    $('btn-refresh').textContent = '⏳';
    try { await api('/api/sync/run?deep=false', { method: 'POST' }); } catch(e) {}
    await loadStatus();
    await loadView(App.state.activeView);
    $('btn-refresh').textContent = '↻ Refresh';
  });
  $('btn-run-agents').addEventListener('click', async () => {
    $('btn-run-agents').textContent = '⏳';
    try { await api('/api/agents/run/all', { method: 'POST' }); } catch(e) {}
    await loadStatus();
    await loadView(App.state.activeView);
    $('btn-run-agents').textContent = '⚡ Run Agents';
  });
  $('btn-discover')?.addEventListener('click', async () => {
    $('btn-discover').textContent = '⏳ Hľadám...';
    try { await api('/api/accounts/discover', { method: 'POST' }); } catch(e) { alert('Discover zlyhal: ' + e.message); }
    await loadAccounts();
    await renderAccountsTable();
    $('btn-discover').textContent = 'Discover GA4 účty';
  });

  // Metric tabs (dashboard)
  $$('.metric-tab').forEach(t => {
    t.addEventListener('click', () => {
      $$('.metric-tab').forEach(x => x.classList.remove('active'));
      t.classList.add('active');
      App.state.activeMetric = t.dataset.metric;
      if (App.state.activeView === 'dashboard') renderMultiChart();
    });
  });

  // Insights filter tabs
  $$('#insights-filters .ft').forEach(t => {
    t.addEventListener('click', () => {
      $$('#insights-filters .ft').forEach(x => x.classList.remove('active'));
      t.classList.add('active');
      App.state.activeInsightType = t.dataset.type;
      loadFullInsights();
    });
  });

  // Agent buttons
  $$('[data-agent]').forEach(b => {
    b.addEventListener('click', async () => {
      const original = b.textContent;
      b.textContent = '⏳';
      try { await api(`/api/agents/run/${b.dataset.agent}`, { method: 'POST' }); }
      catch(e) { alert('Agent failed: ' + e.message); }
      b.textContent = original;
      loadAgentActivity();
      loadStatus();
    });
  });

  // Hypothesis
  $('hypothesis-scope')?.addEventListener('change', e => {
    const v = e.target.value;
    const sel = $('hypothesis-scope-id');
    sel.innerHTML = '';
    if (v === 'segment') {
      App.state.segments.forEach(s => sel.innerHTML += `<option value="${s.slug}">${s.icon} ${s.name}</option>`);
      sel.classList.remove('hidden');
    } else if (v === 'account') {
      App.state.accounts.forEach(a => sel.innerHTML += `<option value="${a.property_id}">${a.display_name}</option>`);
      sel.classList.remove('hidden');
    } else {
      sel.classList.add('hidden');
    }
  });
  $('btn-test-hypothesis')?.addEventListener('click', async () => {
    const q = $('hypothesis-question').value.trim();
    if (!q) { alert('Napíš otázku'); return; }
    const scope = $('hypothesis-scope').value;
    const scope_id = $('hypothesis-scope-id').value || '';
    const period_start = daysAgoISO(App.state.period);
    const period_end = todayISO();
    $('btn-test-hypothesis').textContent = '⏳';
    try {
      await api('/api/hypothesis', {
        method: 'POST',
        body: JSON.stringify({ question: q, scope, scope_id, period_start, period_end }),
      });
      $('hypothesis-question').value = '';
      loadHypotheses();
    } catch(e) { alert('Hypothesis failed: ' + e.message); }
    $('btn-test-hypothesis').textContent = 'Otestovať';
  });
}

// ─────────────── Status ───────────────

async function loadStatus() {
  try {
    const s = await api('/api/status');
    $('status-text').textContent = s.has_credentials ? 'OAuth pripojený' : 'OAuth chýba';
    $('status-dot').className = 'status-dot ' + (s.has_credentials ? 'ok' : 'error');
    $('kpi-accounts').textContent = s.accounts_monitored;
    $('kpi-segments').textContent = s.segments;
    $('kpi-insights').textContent = s.recent_insights;
    $('insights-badge').textContent = s.recent_insights;
  } catch(e) {
    $('status-text').textContent = 'Chyba: ' + e.message;
    $('status-dot').className = 'status-dot error';
  }
}

// ─────────────── Data loaders ───────────────

async function loadAccounts() {
  const list = await api('/api/accounts');
  App.state.accounts = list;
  renderAccountsDropdown();
  updateSelectedCount();
}

async function loadSegments() {
  const list = await api('/api/segments');
  App.state.segments = list;
  const sel = $('filter-segment');
  sel.innerHTML = '<option value="">Všetky segmenty</option>' +
    list.map(s => `<option value="${s.slug}">${s.icon} ${s.name} (${s.account_count})</option>`).join('');
}

function renderAccountsDropdown() {
  const search = $('accounts-search').value.toLowerCase();
  const wrap = $('accounts-list');
  const segMap = Object.fromEntries(App.state.segments.map(s => [s.slug, s]));
  const filtered = App.state.accounts.filter(a =>
    !search || a.display_name.toLowerCase().includes(search) || a.property_id.includes(search)
  );
  wrap.innerHTML = filtered.length ? filtered.map(a => {
    const checked = App.state.selectedAccountIds.has(a.property_id) ? 'checked' : '';
    const segs = (a.segments || []).map(s => `<span class="seg-tag">${segMap[s]?.icon || ''}${s}</span>`).join('');
    return `<label class="account-row">
      <input type="checkbox" data-pid="${a.property_id}" ${checked}>
      <span class="acc-name">${a.display_name}</span>
      <span class="acc-segments">${segs}</span>
    </label>`;
  }).join('') : '<div class="empty-state">Žiadne účty. Klikni Discover na karte „Účty".</div>';
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
  $('accounts-selected-count').textContent = App.state.selectedAccountIds.size;
}

// ─────────────── Views ───────────────

async function loadView(view) {
  if (view === 'overview') return loadOverview();
  if (view === 'dashboard') return loadDashboard();
  if (view === 'segments') return loadSegmentsView();
  if (view === 'accounts') return renderAccountsTable();
  if (view === 'insights') return loadFullInsights();
  if (view === 'agents') return loadAgentActivity();
  if (view === 'hypotheses') return loadHypotheses();
  if (view === 'briefing') return loadBriefing();
}

// ── Overview ──

async function loadOverview() {
  await renderSegmentsGrid();
  await renderAccountStrip();
  await loadOverviewInsights();
}

async function renderSegmentsGrid() {
  const wrap = $('segments-grid');
  wrap.innerHTML = '';
  for (const s of App.state.segments) {
    if (App.state.activeSegment && s.slug !== App.state.activeSegment) continue;
    let h;
    try { h = await api(`/api/health/${s.slug}`); } catch(e) { continue; }
    const latest = h.latest;
    const verdict = latest?.verdict || 'unknown';
    const score = latest?.score != null ? latest.score : '—';
    wrap.innerHTML += `
      <div class="seg-card">
        <div class="seg-card-head">
          <span class="seg-icon">${s.icon}</span>
          <span class="seg-name">${s.name}</span>
          <span class="seg-score-bubble ${verdict}">${score}</span>
        </div>
        <div class="seg-verdict">${latest?.summary || 'Žiadne dáta'}</div>
        <div class="seg-meta">
          <span>📍 ${s.account_count} účtov</span>
          ${latest ? `<span>📉 ${latest.accounts_declining || 0} v poklese</span>` : ''}
        </div>
      </div>
    `;
  }
  if (!wrap.innerHTML) wrap.innerHTML = '<div class="empty-state">Žiadne segmenty s dátami. Spusti Run Agents.</div>';
}

async function renderAccountStrip() {
  const ids = [...App.state.selectedAccountIds];
  if (!ids.length) {
    $('account-strip').innerHTML = '<div class="empty-state">Vyber účty vo filtri hore (▾ Vybrať účty).</div>';
    return;
  }
  const start = daysAgoISO(App.state.period);
  const end = todayISO();
  const data = await api(`/api/metrics/account_strip?property_ids=${ids.join(',')}&start=${start}&end=${end}`);
  const wrap = $('account-strip');
  wrap.innerHTML = '';
  data.accounts.forEach((a, i) => {
    if (a.no_data) {
      wrap.innerHTML += `<div class="account-row-strip"><div class="acc-strip-name">${a.display_name}<span class="pid">${a.property_id}</span></div><div style="grid-column: 2/-1; color:var(--text-faint)">Žiadne dáta — spusti sync</div></div>`;
      return;
    }
    const k = a.kpis;
    const trendCls = a.trend.trend === 'rising' ? 'trend-up' : a.trend.trend === 'falling' ? 'trend-down' : 'trend-flat';
    const trendArrow = a.trend.trend === 'rising' ? '▲' : a.trend.trend === 'falling' ? '▼' : '●';
    const yoy = a.yoy_pct != null ? fmtPct(a.yoy_pct) : '—';
    const yoyCls = a.yoy_pct == null ? 'trend-flat' : a.yoy_pct >= 0 ? 'trend-up' : 'trend-down';
    const hs = a.health_score;
    const hsBg = hs == null ? '#475569' : hs > 65 ? '#16a34a' : hs > 45 ? '#f59e0b' : '#ef4444';
    wrap.innerHTML += `
      <div class="account-row-strip">
        <div class="acc-strip-name">${a.display_name}<span class="pid">${a.property_id}</span></div>
        <div class="acc-strip-kpi"><div class="kpi-label">Sessions</div><div class="kpi-val">${fmt(k.sessions)}</div></div>
        <div class="acc-strip-kpi"><div class="kpi-label">Users</div><div class="kpi-val">${fmt(k.users)}</div></div>
        <div class="acc-strip-kpi"><div class="kpi-label">Konv.</div><div class="kpi-val">${fmt(k.conversions)}</div></div>
        <div class="acc-strip-kpi"><div class="kpi-label">Revenue</div><div class="kpi-val">${fmt(k.revenue)}</div></div>
        <div class="acc-strip-kpi"><div class="kpi-label">Conv rate</div><div class="kpi-val">${k.conv_rate}%</div></div>
        <div class="acc-strip-spark"><canvas id="spark-${i}" height="36"></canvas></div>
        <div class="acc-strip-trend ${yoyCls}">YoY ${yoy}</div>
        <div class="acc-strip-health" style="background:${hsBg}; color:white">${hs == null ? '—' : Math.round(hs)}</div>
      </div>
    `;
  });
  // Render sparklines
  data.accounts.forEach((a, i) => {
    if (a.no_data || !a.sparkline?.length) return;
    const ctx = $(`spark-${i}`)?.getContext('2d');
    if (!ctx) return;
    new Chart(ctx, {
      type: 'line',
      data: {
        labels: a.sparkline.map((_, j) => j),
        datasets: [{
          data: a.sparkline,
          borderColor: '#6366f1', borderWidth: 1.5, fill: false,
          pointRadius: 0, tension: 0.3,
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { enabled: false } },
        scales: { x: { display: false }, y: { display: false } },
      },
    });
  });
}

async function loadOverviewInsights() {
  try {
    const ins = await api('/api/insights?limit=10');
    renderInsightList($('overview-insights'), ins.insights);
  } catch(e) {}
}

// ── Dashboard ──

async function loadDashboard() {
  await renderMultiChart();
  await renderChannelBreakdown();
  await renderDOWChart();
}

async function renderMultiChart() {
  const ids = [...App.state.selectedAccountIds];
  const start = daysAgoISO(App.state.period);
  const end = todayISO();
  if (!ids.length) {
    if (App.charts.multi) { App.charts.multi.destroy(); delete App.charts.multi; }
    $('multi-chart').parentElement.innerHTML = '<canvas id="multi-chart"></canvas><div class="empty-state">Vyber účty.</div>';
    return;
  }
  const data = await api(`/api/metrics/timeseries?property_ids=${ids.join(',')}&metric=${App.state.activeMetric}&start=${start}&end=${end}`);
  const colors = ['#6366f1', '#22c55e', '#f59e0b', '#ef4444', '#06b6d4', '#a855f7', '#ec4899', '#84cc16', '#3b82f6', '#f43f5e'];
  const datasets = data.series.map((s, i) => ({
    label: s.display_name,
    data: s.data.map(d => ({ x: d.date, y: d.value })),
    borderColor: colors[i % colors.length],
    backgroundColor: colors[i % colors.length] + '22',
    borderWidth: 2,
    fill: false,
    pointRadius: 0,
    tension: 0.2,
  }));
  if (App.charts.multi) App.charts.multi.destroy();
  const ctx = $('multi-chart').getContext('2d');
  App.charts.multi = new Chart(ctx, {
    type: 'line',
    data: { datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { position: 'bottom', labels: { color: '#e2e8f0', boxWidth: 12 } },
        tooltip: { backgroundColor: '#1a2230', borderColor: '#1f2a3a', borderWidth: 1 },
      },
      scales: {
        x: { type: 'time', time: { unit: 'day' }, ticks: { color: '#94a3b8', maxRotation: 0 }, grid: { color: '#1f2a3a' } },
        y: { ticks: { color: '#94a3b8' }, grid: { color: '#1f2a3a' } },
      },
    },
  });
}

async function renderChannelBreakdown() {
  const ids = [...App.state.selectedAccountIds];
  if (!ids.length) { $('channel-breakdown').innerHTML = '<div class="empty-state">Vyber účty.</div>'; return; }
  const start = daysAgoISO(App.state.period);
  const end = todayISO();
  const data = await api(`/api/metrics/channel?property_ids=${ids.join(',')}&start=${start}&end=${end}`);
  // Aggregate across all selected accounts
  const totals = {};
  let grandTotal = 0;
  data.breakdown.forEach(r => {
    totals[r.channel_group] = (totals[r.channel_group] || 0) + (r.sessions || 0);
    grandTotal += r.sessions || 0;
  });
  const sorted = Object.entries(totals).sort((a,b) => b[1] - a[1]);
  $('channel-breakdown').innerHTML = sorted.length
    ? sorted.map(([ch, v]) => {
        const pct = grandTotal ? (v/grandTotal*100).toFixed(1) : 0;
        return `<div class="channel-card">
          <div class="channel-name">${ch}</div>
          <div class="channel-value">${fmt(v)}</div>
          <div class="channel-share">${pct}% sessions</div>
        </div>`;
      }).join('')
    : '<div class="empty-state">Žiadne dáta. Spusti Refresh.</div>';
}

async function renderDOWChart() {
  const ids = [...App.state.selectedAccountIds];
  if (!ids.length) { return; }
  const start = daysAgoISO(App.state.period);
  const end = todayISO();
  const data = await api(`/api/correlations/dow?property_ids=${ids.join(',')}&start=${start}&end=${end}&metric=sessions`);
  if (!data.available) return;
  const labels = data.by_day.map(d => d.day);
  const values = data.by_day.map(d => d.avg_value);
  if (App.charts.dow) App.charts.dow.destroy();
  const ctx = $('dow-chart').getContext('2d');
  App.charts.dow = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: data.by_day.map(d =>
          d.day_idx === data.best_day.day_idx ? '#22c55e'
          : d.day_idx === data.worst_day.day_idx ? '#ef4444'
          : '#6366f1'
        ),
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#94a3b8' }, grid: { display: false } },
        y: { ticks: { color: '#94a3b8' }, grid: { color: '#1f2a3a' } },
      },
    },
  });
}

// ── Segments view ──

async function loadSegmentsView() {
  const wrap = $('segments-detail');
  wrap.innerHTML = '<div class="loading">…</div>';
  let html = '';
  for (const s of App.state.segments) {
    let h;
    try { h = await api(`/api/health/${s.slug}`); } catch(e) { continue; }
    const latest = h.latest;
    if (!latest) {
      html += `<div class="segment-detail-card">
        <div class="sd-left">
          <span class="sd-icon">${s.icon}</span>
          <span class="sd-name">${s.name}</span>
          <span class="sd-score-big" style="color:var(--text-faint)">—</span>
          <span class="sd-verdict">Žiadne dáta</span>
        </div>
        <div class="sd-mid">${s.account_count} účtov v segmente</div>
        <div class="sd-right">Spusti agentov pre výpočet</div>
      </div>`;
      continue;
    }
    const verdict = latest.verdict;
    const colors = { excellent: '#22c55e', good: '#16a34a', fair: '#f59e0b', poor: '#ea580c', critical: '#ef4444', unknown: '#64748b' };
    const components = latest.components || {};
    const compList = Object.entries(components).map(([k, v]) =>
      `<div class="sd-component"><span>${k}</span><span>${Math.round(v)}</span></div>`
    ).join('');
    html += `<div class="segment-detail-card">
      <div class="sd-left">
        <span class="sd-icon">${s.icon}</span>
        <span class="sd-name">${s.name}</span>
        <span class="sd-score-big" style="color:${colors[verdict]}">${latest.score}</span>
        <span class="sd-verdict">${latest.verdict}</span>
      </div>
      <div class="sd-mid">${latest.summary || ''}</div>
      <div class="sd-right">${compList}</div>
    </div>`;
  }
  wrap.innerHTML = html || '<div class="empty-state">Žiadne segmenty s dátami</div>';
}

// ── Accounts table ──

async function renderAccountsTable() {
  await loadAccounts();  // refresh
  const wrap = $('accounts-table-wrap');
  if (!App.state.accounts.length) {
    wrap.innerHTML = '<div class="empty-state">Žiadne účty. Klikni „Discover GA4 účty".</div>';
    return;
  }
  const segMap = Object.fromEntries(App.state.segments.map(s => [s.slug, s]));
  let html = `<table class="accounts-table">
    <thead><tr>
      <th>Property ID</th><th>Názov</th><th>Účet</th><th>Mena</th><th>Segmenty</th><th>Monitor</th>
    </tr></thead><tbody>`;
  App.state.accounts.forEach(a => {
    const segs = (a.segments || []).map(slug => {
      const s = segMap[slug];
      return s ? `<span class="acc-seg-pill">${s.icon} ${s.name}<span class="x" data-rm-pid="${a.property_id}" data-rm-slug="${slug}">×</span></span>` : '';
    }).join('');
    const adder = `<span class="acc-seg-pill adder" data-add-pid="${a.property_id}">+ pridať</span>`;
    html += `<tr>
      <td><code>${a.property_id}</code></td>
      <td>${a.display_name}</td>
      <td>${a.parent_account_name || ''}</td>
      <td>${a.currency_code || '—'}</td>
      <td><div class="acc-segs-cell">${segs}${adder}</div></td>
      <td><input type="checkbox" data-mon-pid="${a.property_id}" ${a.is_monitored ? 'checked' : ''}></td>
    </tr>`;
  });
  html += '</tbody></table>';
  wrap.innerHTML = html;

  // Bind monitor toggles
  wrap.querySelectorAll('[data-mon-pid]').forEach(cb => {
    cb.addEventListener('change', async e => {
      await api(`/api/accounts/${cb.dataset.monPid}/monitored`, {
        method: 'PUT',
        body: JSON.stringify({ monitored: cb.checked }),
      });
      loadStatus();
    });
  });
  // Bind segment removers
  wrap.querySelectorAll('[data-rm-pid]').forEach(x => {
    x.addEventListener('click', async () => {
      await api(`/api/accounts/${x.dataset.rmPid}/segments/${x.dataset.rmSlug}`, { method: 'DELETE' });
      renderAccountsTable();
    });
  });
  // Bind segment adders
  wrap.querySelectorAll('[data-add-pid]').forEach(b => {
    b.addEventListener('click', () => {
      const slug = prompt(`Segment slug pre pridanie:\n${App.state.segments.map(s => `${s.slug} (${s.name})`).join('\n')}`);
      if (!slug) return;
      api(`/api/accounts/${b.dataset.addPid}/segments`, {
        method: 'POST',
        body: JSON.stringify({ segment_slug: slug }),
      }).then(() => renderAccountsTable());
    });
  });
}

// ── Insights ──

async function loadFullInsights() {
  const params = new URLSearchParams({ limit: 200 });
  if (App.state.activeInsightType) params.set('insight_type', App.state.activeInsightType);
  const data = await api(`/api/insights?${params}`);
  renderInsightList($('insights-full-list'), data.insights);
}

function renderInsightList(wrap, insights) {
  if (!insights?.length) {
    wrap.innerHTML = '<div class="empty-state">Žiadne insights — spusti agentov.</div>';
    return;
  }
  const icons = { anomaly: '⚠️', trend: '📈', forecast: '🔮', health_score: '❤️', correlation: '🔗', briefing: '📰', hypothesis: '🔬' };
  wrap.innerHTML = insights.map(i => {
    const created = new Date(i.created_at).toLocaleString('cs-CZ', { dateStyle: 'short', timeStyle: 'short' });
    return `<div class="insight severity-${i.severity}">
      <div class="insight-icon">${icons[i.insight_type] || '💡'}</div>
      <div class="insight-body">
        <div class="insight-title">${i.title}</div>
        <div class="insight-desc">${i.body || ''}</div>
        <div class="insight-meta">
          <span>${i.scope}: ${i.scope_id}</span>
          <span>${created}</span>
          <span>conf: ${(i.confidence * 100).toFixed(0)}%</span>
        </div>
      </div>
      <button class="dismiss-btn" data-dismiss="${i.id}">×</button>
    </div>`;
  }).join('');
  wrap.querySelectorAll('[data-dismiss]').forEach(b => {
    b.addEventListener('click', async () => {
      await api(`/api/insights/${b.dataset.dismiss}`, { method: 'DELETE' });
      loadFullInsights();
    });
  });
}

// ── Agents ──

async function loadAgentActivity() {
  const data = await api('/api/agents/activity?limit=50');
  const wrap = $('agent-activity-list');
  if (!data.activity?.length) {
    wrap.innerHTML = '<div class="empty-state">Žiadna aktivita — spusti agenta.</div>';
    return;
  }
  wrap.innerHTML = data.activity.map(a => {
    const started = new Date(a.started_at).toLocaleTimeString('cs-CZ');
    return `<div class="activity-row">
      <span>${started}</span>
      <span><strong>${a.agent_type}</strong> ${a.scope ? a.scope + (a.scope_id ? ':' + a.scope_id : '') : ''}</span>
      <span>${a.summary || ''}</span>
      <span class="activity-status-${a.status}">${a.status === 'running' ? '⏳' : a.status === 'success' ? '✓' : '✗'} ${a.findings_count} findings</span>
    </div>`;
  }).join('');
  const running = data.activity.some(a => a.status === 'running');
  $('agents-live').style.display = running ? 'block' : 'none';
  $('kpi-agents').textContent = running ? '✓' : '0';
}

// ── Hypotheses ──

async function loadHypotheses() {
  const data = await api('/api/hypothesis?limit=30');
  const wrap = $('hypotheses-list');
  if (!data.hypotheses?.length) {
    wrap.innerHTML = '<div class="empty-state">Žiadne hypotézy zatiaľ.</div>';
    return;
  }
  wrap.innerHTML = data.hypotheses.map(h => `
    <div class="hypothesis-card">
      <div class="hypothesis-q">❓ ${h.question}</div>
      <div class="hypothesis-a">${h.answer || '<em>Pending</em>'}</div>
      <div class="hypothesis-meta">${h.scope}${h.scope_id ? ':' + h.scope_id : ''} · ${h.period_start || ''} → ${h.period_end || ''} · conf ${(h.confidence * 100).toFixed(0)}%</div>
    </div>`).join('');
}

// ── Briefing ──

async function loadBriefing() {
  const data = await api('/api/briefing');
  const wrap = $('briefing-content');
  if (!data.briefing) {
    wrap.innerHTML = '<div class="empty-state">Žiadny briefing — spusti „Daily briefing" v Agents.</div>';
    return;
  }
  const b = data.briefing;
  wrap.innerHTML = `
    <h2>${b.headline}</h2>
    <div style="color:var(--text-faint); font-size: 11px; margin-bottom: 12px">${b.briefing_date} · vygenerované ${new Date(b.generated_at).toLocaleString('cs-CZ')}</div>
    <div style="white-space: pre-wrap">${b.body}</div>
  `;
}

// ─────────────── Run ───────────────

document.addEventListener('DOMContentLoaded', init);
