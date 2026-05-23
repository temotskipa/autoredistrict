const form = document.querySelector("#plan-form");
const modeSelect = document.querySelector("#mode-select");
const districtsInput = form.querySelector('input[name="districts"]');
const button = document.querySelector("#generate-button");
const statusText = document.querySelector("#status-text");
const solverText = document.querySelector("#solver-text");
const warnings = document.querySelector("#warnings");
const mapImage = document.querySelector("#map-image");
const mapPlaceholder = document.querySelector("#map-placeholder");
const metricsBody = document.querySelector("#metrics-body");
const downloads = document.querySelector("#downloads");

const summaryEls = {
  districts: document.querySelector("#summary-districts"),
  deviation: document.querySelector("#summary-deviation"),
  compactness: document.querySelector("#summary-compactness"),
  contiguous: document.querySelector("#summary-contiguous"),
};

modeSelect.addEventListener("change", syncModeFields);
districtsInput.addEventListener("input", () => {
  districtsInput.dataset.userEdited = "true";
});
syncModeFields();

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(form);
  const payload = readPayload(data);
  const endpoint = data.get("mode") === "state" ? "/api/plans/state" : "/api/plans/demo";
  setBusy(true);

  try {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      throw new Error(await readErrorMessage(response));
    }
    const plan = await response.json();
    renderPlan(plan);
    statusText.textContent = `Generated ${plan.plan_id}.`;
  } catch (error) {
    statusText.textContent = error.message;
  } finally {
    setBusy(false);
  }
});

function syncModeFields() {
  const stateMode = modeSelect.value === "state";
  document.querySelectorAll("[data-state-field]").forEach((element) => {
    element.hidden = !stateMode;
  });
  document.querySelectorAll("[data-demo-field]").forEach((element) => {
    element.hidden = stateMode;
  });
  if (stateMode && districtsInput.dataset.userEdited !== "true") {
    districtsInput.value = "";
    districtsInput.placeholder = "Auto";
  }
  if (!stateMode && districtsInput.value.trim() === "") {
    districtsInput.value = "4";
    districtsInput.placeholder = "";
  }
}

function readPayload(data) {
  const districts = optionalInteger(data.get("districts"));
  const payload = {
    algorithm: data.get("algorithm"),
    grid_size: integerValue(data.get("grid_size"), 4),
    random_seed: integerValue(data.get("random_seed"), 0),
    population_equality_weight: Number(data.get("population_equality_weight")),
    compactness_weight: Number(data.get("compactness_weight")),
    vra: data.get("vra") === "on",
    preserve_demo_coi: data.get("preserve_demo_coi") === "on",
  };
  if (districts !== null) {
    payload.districts = districts;
  } else if (data.get("mode") === "demo") {
    payload.districts = 4;
  }
  if (data.get("mode") === "state") {
    payload.state = data.get("state");
    payload.api_key = data.get("api_key");
    payload.resolution = data.get("resolution");
    delete payload.grid_size;
    delete payload.preserve_demo_coi;
  }
  return payload;
}

function setBusy(isBusy) {
  button.disabled = isBusy;
  button.textContent = isBusy ? "Generating..." : "Generate Plan";
  if (isBusy) {
    statusText.textContent = "Running local solver...";
  }
}

async function readErrorMessage(response) {
  try {
    const body = await response.json();
    if (typeof body.detail === "string") {
      return body.detail;
    }
    if (Array.isArray(body.detail) && body.detail.length > 0) {
      return body.detail.map((item) => item.msg || String(item)).join("; ");
    }
  } catch (_error) {
    // Fall through to the generic status when the response is not JSON.
  }
  return `Request failed with ${response.status}`;
}

function renderPlan(plan) {
  const summary = plan.summary;
  summaryEls.districts.textContent = `${summary.district_count}/${summary.requested_districts}`;
  summaryEls.deviation.textContent = formatPct(summary.max_abs_deviation_pct);
  summaryEls.compactness.textContent = formatNumber(summary.average_compactness);
  summaryEls.contiguous.textContent = summary.all_contiguous ? "Yes" : "No";

  mapImage.src = `${plan.urls.map_png}?t=${Date.now()}`;
  mapImage.hidden = false;
  mapPlaceholder.hidden = true;
  solverText.textContent = `${plan.solver.solver}, seed ${plan.solver.random_seed}, score ${plan.solver.objective_score}`;

  metricsBody.innerHTML = "";
  for (const item of plan.metrics) {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${item.district_id}</td>
      <td>${item.population.toLocaleString()}</td>
      <td>${formatPct(item.deviation_pct)}</td>
      <td>${formatNumber(item.compactness_polsby_popper)}</td>
      <td>${formatPct(item.partisan_dem_share * 100)}</td>
      <td>${item.unit_count}</td>
      <td>${item.contiguous ? "Yes" : "No"}</td>
    `;
    metricsBody.appendChild(row);
  }

  downloads.innerHTML = `
    <a href="${plan.urls.map_png}" target="_blank" rel="noreferrer">PNG</a>
    <a href="${plan.urls.districts_geojson}" target="_blank" rel="noreferrer">GeoJSON</a>
    <a href="${plan.urls.assignment_csv}" target="_blank" rel="noreferrer">Assignment CSV</a>
    <a href="${plan.urls.metrics_csv}" target="_blank" rel="noreferrer">Metrics CSV</a>
    <a href="${plan.urls.report_json}" target="_blank" rel="noreferrer">Report JSON</a>
  `;

  warnings.innerHTML = "";
  for (const warning of plan.warnings || []) {
    const item = document.createElement("div");
    item.className = "warning";
    item.textContent = warning;
    warnings.appendChild(item);
  }
}

function formatPct(value) {
  if (value === null || value === undefined) return "-";
  return `${Number(value).toFixed(2)}%`;
}

function formatNumber(value) {
  if (value === null || value === undefined) return "-";
  return Number(value).toFixed(3);
}

function integerValue(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function optionalInteger(value) {
  if (String(value || "").trim() === "") return null;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : null;
}
