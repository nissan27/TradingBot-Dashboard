const preview = new URLSearchParams(window.location.search).get("preview") === "1";

const previewPayload = {
  schema_version: 1,
  generated_utc: "2026-08-18T11:00:09Z",
  overall_status: "healthy",
  runtime_mode: "paper_read_only",
  account_key: "Exness-MT5Trial5:277817628",
  journal: {
    integrity: "ok", bars_captured: 78, expected_clocks: 3,
    fresh_clocks: 3, stale_clocks: 0, missing_clocks: 0, excluded_clocks: 1,
    clock_coverage: 1, last_clock_close_utc: "2026-08-18T06:00:00Z",
    last_clock_delay_seconds: 186, candidate_signals: 1, risk_vetoes: 1,
    intended_orders: 1, closed_candidate_trades: 1
  },
  safety: { kill_switch_engaged: false, reconciliation_ok: true, reconciliation_age_seconds: 108 },
  scheduler: { available: true, state: "Ready", last_result: 0, last_run_age_seconds: 128, missed_runs: 0, next_run_utc: "2026-08-18T11:03:01Z" },
  news: { configured: true, status: "fresh", age_seconds: 94 },
  refresh_seconds: 15,
  performance_blinded: true,
  broker_order_adapter_present: false
};

const byId = (id) => document.getElementById(id);
const text = (id, value) => { byId(id).textContent = value ?? "—"; };
const number = (value) => Number.isFinite(Number(value)) ? Number(value).toLocaleString() : "—";
const percent = (value) => value == null ? "n/a" : `${(Number(value) * 100).toFixed(2)}%`;
const seconds = (value) => value == null ? "n/a" : `${Math.round(Number(value))}s`;
const time = (value) => value ? new Date(value).toLocaleString([], { dateStyle: "medium", timeStyle: "short" }) : "—";

function setStatus(element, level, label) {
  element.classList.remove("status-ok", "status-attention", "status-offline");
  element.classList.add(level === "healthy" ? "status-ok" : level === "attention" ? "status-attention" : "status-offline");
  element.replaceChildren();
  const dot = document.createElement("i");
  element.append(dot, document.createTextNode(label));
}

function setLed(id, level) {
  byId(id).className = `service-led ${level}`;
}

function renderRibbon(journal) {
  const ribbon = byId("clockRibbon");
  ribbon.replaceChildren();
  const groups = [
    ["fresh", journal.fresh_clocks], ["stale", journal.stale_clocks],
    ["missing", journal.missing_clocks], ["excluded", journal.excluded_clocks]
  ];
  groups.forEach(([kind, count]) => {
    for (let i = 0; i < Number(count || 0); i += 1) {
      const bar = document.createElement("span");
      bar.className = kind;
      bar.title = `${kind} clock`;
      ribbon.appendChild(bar);
    }
  });
  if (!ribbon.children.length) {
    const empty = document.createElement("span");
    empty.className = "excluded";
    empty.title = "No clock evidence yet";
    ribbon.appendChild(empty);
  }
}

function render(payload) {
  const journal = payload.journal || {};
  const scheduler = payload.scheduler || {};
  const safety = payload.safety || {};
  const news = payload.news || {};
  const statusLabel = payload.overall_status === "healthy" ? "Healthy" : payload.overall_status === "attention" ? "Attention" : "Unavailable";

  setStatus(byId("overallStatus"), payload.overall_status, statusLabel);
  text("lastUpdated", `Updated ${time(payload.generated_utc)}`);
  text("clockCoverage", percent(journal.clock_coverage));
  byId("coverageFill").style.width = `${Math.max(0, Math.min(100, Number(journal.clock_coverage || 0) * 100))}%`;
  text("journalIntegrity", journal.integrity || "unavailable");
  text("barsCaptured", `${number(journal.bars_captured)} closed bars saved`);
  text("taskState", scheduler.available ? scheduler.state : "Unavailable");
  text("taskResult", scheduler.available ? (scheduler.last_result === 0 ? "last run successful" : `result ${scheduler.last_result}`) : "Windows task data unavailable");
  text("nextRun", `Next run ${time(scheduler.next_run_utc)}`);
  text("runtimeMode", String(payload.runtime_mode || "—").replaceAll("_", " "));
  text("freshClocks", number(journal.fresh_clocks));
  text("staleClocks", number(journal.stale_clocks));
  text("missingClocks", number(journal.missing_clocks));
  text("excludedClocks", number(journal.excluded_clocks));
  text("expectedClocks", number(journal.expected_clocks));
  text("captureLag", seconds(journal.last_clock_delay_seconds));
  text("lastClock", `Last close ${time(journal.last_clock_close_utc)}`);
  text("signalsObserved", number(journal.candidate_signals));
  text("riskVetoes", number(journal.risk_vetoes));
  text("paperIntents", number(journal.intended_orders));
  text("closedRows", number(journal.closed_candidate_trades));
  text("accountKey", payload.account_key);
  text("killSwitch", safety.available === false ? "Unavailable" : safety.kill_switch_engaged ? "ENGAGED" : "Released");
  text("reconciliation", safety.reconciliation_ok === true ? "Healthy" : safety.reconciliation_ok === false ? "Attention required" : "Not recorded");
  text("refreshInterval", payload.refresh_seconds || 15);
  renderRibbon(journal);

  const mt5Good = scheduler.available && scheduler.last_result === 0 && Number(scheduler.last_run_age_seconds) <= 900;
  setLed("mt5Led", mt5Good ? "good" : scheduler.available ? "warn" : "bad");
  text("mt5Status", mt5Good ? "Verified" : scheduler.available ? "Check" : "Unavailable");
  text("mt5Detail", mt5Good ? "Exact account verified by last observer run" : "Observer task needs review");
  setLed("newsLed", news.status === "fresh" ? "good" : news.configured ? "warn" : "");
  text("newsStatus", news.status || "Not configured");
  text("newsDetail", news.configured ? `${seconds(news.age_seconds)} since MT5 calendar export` : "Add the Common Files export path to the dashboard command");
  setLed("apiLed", "good");
}

let errorTimer;
function showError(message) {
  const toast = byId("errorToast");
  toast.textContent = message;
  toast.classList.add("visible");
  clearTimeout(errorTimer);
  errorTimer = setTimeout(() => toast.classList.remove("visible"), 5000);
}

async function refresh() {
  const button = byId("refreshButton");
  button.classList.add("loading");
  try {
    const payload = preview
      ? previewPayload
      : await fetch("/api/health", { cache: "no-store", headers: { Accept: "application/json" } }).then((response) => {
          if (!response.ok) throw new Error(`Health bridge returned ${response.status}`);
          return response.json();
        });
    render(payload);
  } catch (error) {
    setStatus(byId("overallStatus"), "offline", "Bridge unavailable");
    showError(error instanceof Error ? error.message : "Dashboard refresh failed");
  } finally {
    button.classList.remove("loading");
  }
}

byId("refreshButton").addEventListener("click", refresh);
refresh();
setInterval(refresh, 15000);
