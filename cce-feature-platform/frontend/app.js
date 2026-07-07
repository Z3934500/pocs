const state = {
  summary: null,
  features: [],
  campaigns: [],
  issues: [],
  identityCandidates: [],
  drift: [],
};

async function getJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${url}`);
  }
  return response.json();
}

function money(value) {
  return new Intl.NumberFormat("en-SG", {
    style: "currency",
    currency: "SGD",
    maximumFractionDigits: 0,
  }).format(value);
}

function renderKpis() {
  const summary = state.summary;
  const items = [
    ["Customers", summary.total_customers],
    ["Policies", summary.total_policies],
    ["Transactions", summary.total_transactions],
    ["Eligible Pairs", summary.eligible_customer_campaign_pairs],
    ["Identity Matches", summary.identity_candidates],
    ["Drift Alerts", summary.drift_alerts],
  ];
  document.querySelector("#kpis").innerHTML = items
    .map(([label, value]) => `<div class="kpi"><div class="label">${label}</div><div class="value">${value}</div></div>`)
    .join("");
}

function renderSegments() {
  const filter = document.querySelector("#segmentFilter");
  const selected = filter.value;
  const names = state.summary.segments.map((segment) => segment.segment_name);
  filter.innerHTML = `<option value="">All segments</option>${names
    .map((name) => `<option value="${name}">${name}</option>`)
    .join("")}`;
  filter.value = selected;
}

function renderFeatures() {
  document.querySelector("#featureRows").innerHTML = state.features
    .map(
      (row) => `
      <tr>
        <td><strong>${row.primary_name}</strong><div class="muted">${row.unified_customer_key}</div></td>
        <td>${row.segment_name}</td>
        <td>${money(row.monetary_30d)}</td>
        <td>${row.tx_count_30d}</td>
        <td>${row.velocity_7d}</td>
        <td>${row.risk_score}</td>
        <td>${row.propensity_score ?? "-"}</td>
      </tr>
    `,
    )
    .join("");
}

function renderEligibility() {
  document.querySelector("#eligibilityRows").innerHTML = state.campaigns
    .map((row) => {
      const tag = row.is_eligible ? '<span class="tag ok">Eligible</span>' : '<span class="tag warn">Not eligible</span>';
      return `
        <div class="item">
          <strong>${row.primary_name}</strong>
          <div class="muted">${row.segment_name} / ${money(row.monetary_30d)}</div>
          <div>${tag} <span class="muted">${row.reason}</span></div>
        </div>
      `;
    })
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
        <div class="muted">${issue.layer} / ${issue.severity} / ${issue.entity_key}</div>
        <div>${issue.message}</div>
      </div>
    `,
    )
    .join("");
}

function renderIdentityCandidates() {
  if (!state.identityCandidates.length) {
    document.querySelector("#identityRows").innerHTML = '<div class="item">No identity candidates.</div>';
    return;
  }
  document.querySelector("#identityRows").innerHTML = state.identityCandidates
    .map(
      (candidate) => `
      <div class="item">
        <strong>${candidate.left_ref} -> ${candidate.right_ref}</strong>
        <div class="muted">score ${candidate.match_score} / ${candidate.resolution_action}</div>
        <div>${candidate.match_reason}</div>
      </div>
    `,
    )
    .join("");
}

function renderDrift() {
  document.querySelector("#driftRows").innerHTML = state.drift
    .map((row) => {
      const tag = row.severity === "high" ? "bad" : row.severity === "medium" ? "warn" : "ok";
      return `
        <div class="item">
          <strong>${row.feature_name}</strong>
          <div class="muted">baseline ${row.baseline_mean} / current ${row.current_mean}</div>
          <div><span class="tag ${tag}">${row.severity}</span> <span class="muted">drift ${row.drift_ratio}</span></div>
        </div>
      `;
    })
    .join("");
}

async function refresh() {
  const segment = document.querySelector("#segmentFilter").value;
  const campaign = document.querySelector("#campaignFilter").value;
  state.summary = await getJson("/api/summary");
  state.features = await getJson(`/api/features${segment ? `?segment=${encodeURIComponent(segment)}` : ""}`);
  state.campaigns = await getJson(`/api/campaigns/${campaign}/eligibility`);
  state.issues = await getJson("/api/data-quality/issues");
  state.identityCandidates = await getJson("/api/identity/candidates");
  state.drift = await getJson("/api/mlops/drift");
  renderKpis();
  renderSegments();
  renderFeatures();
  renderEligibility();
  renderIssues();
  renderIdentityCandidates();
  renderDrift();
}

document.querySelector("#refreshButton").addEventListener("click", refresh);
document.querySelector("#segmentFilter").addEventListener("change", refresh);
document.querySelector("#campaignFilter").addEventListener("change", refresh);
refresh();
