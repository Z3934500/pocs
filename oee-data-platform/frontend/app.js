const state = {
  summary: null,
  daily: [],
  machines: [],
  downtime: [],
  anomalies: [],
  issues: [],
};

async function getJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${url}`);
  }
  return response.json();
}

function pct(value) {
  return `${Math.round(Number(value) * 100)}%`;
}

function renderKpis() {
  const summary = state.summary;
  const items = [
    ["Avg OEE", pct(summary.avg_oee)],
    ["Availability", pct(summary.avg_availability)],
    ["Performance", pct(summary.avg_performance)],
    ["Quality", pct(summary.avg_quality)],
  ];
  document.querySelector("#kpis").innerHTML = items
    .map(([label, value]) => `<div class="kpi"><div class="label">${label}</div><div class="value">${value}</div></div>`)
    .join("");
}

function statusTag(value) {
  if (value >= 0.75) return '<span class="tag good">Healthy</span>';
  if (value >= 0.55) return '<span class="tag warn">Watch</span>';
  return '<span class="tag bad">Risk</span>';
}

function renderDaily() {
  document.querySelector("#oeeRows").innerHTML = state.daily
    .map(
      (row) => `
      <tr>
        <td>${row.shift_date}</td>
        <td><strong>${row.machine_number}</strong><div class="muted">${row.line_name}</div></td>
        <td>${row.site_code}</td>
        <td>${statusTag(row.oee)} ${pct(row.oee)}</td>
        <td>${pct(row.availability)}</td>
        <td>${pct(row.performance)}</td>
        <td>${pct(row.quality)}</td>
        <td>${row.downtime_minutes} min</td>
      </tr>
    `,
    )
    .join("");
}

function renderMachines() {
  document.querySelector("#machineRows").innerHTML = state.machines
    .map(
      (row) => `
      <div class="item">
        <strong>${row.machine_number}</strong>
        <div class="muted">${row.site_code} · ${row.machine_type} · ${row.line_name}</div>
        <div>Avg OEE ${pct(row.avg_oee)} · Downtime ${Math.round(row.downtime_minutes)} min</div>
        <div class="bar"><span style="width:${Math.min(100, Math.round(row.avg_oee * 100))}%"></span></div>
      </div>
    `,
    )
    .join("");
}

function renderDowntime() {
  const max = Math.max(...state.downtime.map((row) => row.downtime_minutes), 1);
  document.querySelector("#downtimeRows").innerHTML = state.downtime
    .map(
      (row) => `
      <div class="item">
        <strong>${row.downtime_reason}</strong>
        <div>${Math.round(row.downtime_minutes)} minutes</div>
        <div class="bar"><span style="width:${Math.round((row.downtime_minutes / max) * 100)}%"></span></div>
      </div>
    `,
    )
    .join("");
}

function renderAnomalies() {
  if (!state.anomalies.length) {
    document.querySelector("#anomalyRows").innerHTML = '<div class="item">No anomaly alerts.</div>';
    return;
  }
  document.querySelector("#anomalyRows").innerHTML = state.anomalies
    .map(
      (alert) => `
      <div class="item">
        <strong>${alert.machine_number} · ${alert.metric_name}</strong>
        <div class="muted">${alert.site_code} · ${alert.shift_date} · ${alert.severity}</div>
        <div>${alert.message}</div>
      </div>
    `,
    )
    .join("");
}

function renderIssues() {
  if (!state.issues.length) {
    document.querySelector("#dqRows").innerHTML = '<div class="item">No data quality issues.</div>';
    return;
  }
  document.querySelector("#dqRows").innerHTML = state.issues
    .map(
      (issue) => `
      <div class="item">
        <strong>${issue.issue_type}</strong>
        <div class="muted">${issue.layer} · ${issue.severity}</div>
        <div>${issue.message}</div>
      </div>
    `,
    )
    .join("");
}

async function refresh() {
  const site = document.querySelector("#siteFilter").value;
  const suffix = site ? `?site=${site}` : "";
  state.summary = await getJson("/api/summary");
  state.daily = await getJson(`/api/oee/daily${suffix}`);
  state.machines = await getJson("/api/oee/machines");
  state.downtime = await getJson(`/api/downtime/pareto${suffix}`);
  state.anomalies = await getJson("/api/anomalies");
  state.issues = await getJson("/api/data-quality/issues");
  renderKpis();
  renderDaily();
  renderMachines();
  renderDowntime();
  renderAnomalies();
  renderIssues();
}

document.querySelector("#refreshButton").addEventListener("click", refresh);
document.querySelector("#siteFilter").addEventListener("change", refresh);
refresh();
