/**
 * Universal Data Engineering Pipeline - Interactive Dashboard Engine.
 * Provides client-side reactive rendering, Chart.js visualization generation,
 * dynamic filtering, upload handling, and pipeline stage execution tracking.
 */

// Global State
let currentPayload = null;
let activeDataset = "";
let chartInstances = {};
let tableCurrentPage = 1;
const TABLE_PAGE_SIZE = 15;
let filteredRecords = [];

// Initialize Dashboard on DOM Load
document.addEventListener("DOMContentLoaded", () => {
  initEventListeners();
  checkSystemStatus();
  loadDatasetList();
});

// Event Listeners Setup
function initEventListeners() {
  // Dataset Selector
  const datasetSelect = document.getElementById("dataset-select");
  if (datasetSelect) {
    datasetSelect.addEventListener("change", (e) => {
      activeDataset = e.target.value;
      loadDashboardData(activeDataset);
    });
  }

  // Slicer Filters
  const catFilter = document.getElementById("category-filter");
  if (catFilter) {
    catFilter.addEventListener("change", applyClientFilters);
  }

  const dateStart = document.getElementById("date-filter-start");
  const dateEnd = document.getElementById("date-filter-end");
  if (dateStart) dateStart.addEventListener("change", applyClientFilters);
  if (dateEnd) dateEnd.addEventListener("change", applyClientFilters);

  // Reset Filters Button
  const btnReset = document.getElementById("btn-reset-filters");
  if (btnReset) {
    btnReset.addEventListener("click", resetFilters);
  }

  // Toggle Pipeline Tracker Accordion
  const btnToggleTracker = document.getElementById("btn-toggle-pipeline-stages");
  const trackerSection = document.getElementById("pipeline-tracker-section");
  const btnCloseTracker = document.getElementById("btn-close-tracker");

  if (btnToggleTracker && trackerSection) {
    btnToggleTracker.addEventListener("click", () => {
      trackerSection.style.display = trackerSection.style.display === "none" ? "block" : "none";
    });
  }
  if (btnCloseTracker && trackerSection) {
    btnCloseTracker.addEventListener("click", () => {
      trackerSection.style.display = "none";
    });
  }

  // Table Search
  const tableSearch = document.getElementById("table-search-input");
  if (tableSearch) {
    tableSearch.addEventListener("input", () => {
      tableCurrentPage = 1;
      applyClientFilters();
    });
  }

  // Table Pagination
  const btnPrev = document.getElementById("btn-prev-page");
  const btnNext = document.getElementById("btn-next-page");
  if (btnPrev) {
    btnPrev.addEventListener("click", () => {
      if (tableCurrentPage > 1) {
        tableCurrentPage--;
        renderTableRows();
      }
    });
  }
  if (btnNext) {
    btnNext.addEventListener("click", () => {
      const maxPages = Math.ceil(filteredRecords.length / TABLE_PAGE_SIZE) || 1;
      if (tableCurrentPage < maxPages) {
        tableCurrentPage++;
        renderTableRows();
      }
    });
  }

  // Export CSV Button
  const btnExport = document.getElementById("btn-export-csv");
  if (btnExport) {
    btnExport.addEventListener("click", exportFilteredToCSV);
  }

  // Upload Modal Handlers
  setupUploadModal();

  // Run Pipeline Modal Handlers
  setupPipelineModal();

  // Modals for Quality & Power BI
  setupInspectorModals();
}

// --------------------------------------------------------------------------
// API Calls & State Loading
// --------------------------------------------------------------------------

async function checkSystemStatus() {
  try {
    const res = await fetch("/api/status");
    const data = await res.json();

    const dbDot = document.getElementById("db-status-dot");
    const dbText = document.getElementById("db-connection-text");

    if (data.database_connected) {
      dbDot.className = "status-indicator connected";
      dbText.textContent = "PostgreSQL Connected";
      dbText.title = data.database_message;
    } else {
      dbDot.className = "status-indicator disconnected";
      dbText.textContent = "Database Unavailable (Using Cache)";
      dbText.title = data.database_message;
      showNotification(data.database_message || "PostgreSQL is offline; data loaded from local cache.", "warning");
    }

    const healthBadge = document.getElementById("health-badge");
    if (healthBadge && data.pipeline_health) {
      healthBadge.innerHTML = `<span class="pulse-dot"></span> ${data.pipeline_health}`;
    }

    if (data.latest_run && data.latest_run.stages && data.latest_run.stages.length > 0) {
      renderPipelineStages({
        run_id: data.latest_run.run_id,
        pipeline_status: data.latest_run.status,
        stages: data.latest_run.stages,
      });
      const urlParams = new URLSearchParams(window.location.search);
      const trackerSection = document.getElementById("pipeline-tracker-section");
      if (trackerSection && urlParams.get("show_stages") !== "true") {
        trackerSection.style.display = "none";
      }
    }
  } catch (err) {
    console.warn("Status check failed:", err);
  }
}

async function loadDatasetList() {
  try {
    const res = await fetch("/api/datasets");
    const data = await res.json();
    const select = document.getElementById("dataset-select");
    if (!select) return;

    select.innerHTML = "";
    if (!data.datasets || data.datasets.length === 0) {
      select.innerHTML = '<option value="" disabled>No datasets found</option>';
      return;
    }

    data.datasets.forEach((ds) => {
      const opt = document.createElement("option");
      opt.value = ds.name;
      opt.textContent = `${ds.display_name} (${ds.source.toUpperCase()})`;
      select.appendChild(opt);
    });

    // Select initial dataset (honor ?dataset= URL query param, prefer sample_sales or first available)
    const urlParams = new URLSearchParams(window.location.search);
    const requestedDs = urlParams.get("dataset");
    const preferred = (requestedDs && data.datasets.find((d) => d.name === requestedDs)) ||
                      data.datasets.find((d) => d.name.includes("sales")) ||
                      data.datasets[0];
    activeDataset = preferred.name;
    select.value = activeDataset;

    loadDashboardData(activeDataset);
  } catch (err) {
    console.error("Failed to load dataset list:", err);
    showNotification("Failed to load dataset list.", "danger");
  }
}

async function loadDashboardData(datasetName) {
  try {
    const res = await fetch(`/api/dashboard-data?dataset=${encodeURIComponent(datasetName)}`);
    const data = await res.json();
    currentPayload = data;

    // Update Source tag
    const srcTag = document.getElementById("dataset-source-tag");
    if (srcTag) {
      srcTag.textContent = `Source: ${data.source.toUpperCase()} (${data.row_count} rows)`;
    }

    // Display Warnings
    if (data.warnings && data.warnings.length > 0) {
      data.warnings.forEach((w) => showNotification(w, "info"));
    }

    // Zone 1: Configure Slicers
    configureSlicers(data);

    // Filter state
    filteredRecords = [...data.records];
    tableCurrentPage = 1;

    // Zone 2: Render KPIs
    renderKPICards(data.kpis);

    // Zone 3: Render Visuals
    renderVisualsGrid(data.charts);

    // Zone 4: Render Granular Records Table
    renderTableHead(data.columns);
    renderTableRows();

    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.get("modal") === "powerbi") {
      const btnOpenPbi = document.getElementById("btn-view-powerbi-spec");
      if (btnOpenPbi) btnOpenPbi.click();
    } else if (urlParams.get("modal") === "quality") {
      const btnOpenQuality = document.getElementById("btn-view-quality-audit");
      if (btnOpenQuality) btnOpenQuality.click();
    }

  } catch (err) {
    console.error("Failed to load dashboard payload:", err);
    showNotification(`Failed to load data for ${datasetName}: ${err.message}`, "danger");
  }
}

// --------------------------------------------------------------------------
// Zone 1: Slicers & Filters
// --------------------------------------------------------------------------

function configureSlicers(payload) {
  const catGroup = document.getElementById("category-filter-group");
  const catFilter = document.getElementById("category-filter");
  const catLabel = document.getElementById("category-filter-label");

  const dateGroup = document.getElementById("date-filter-group");
  const dateStart = document.getElementById("date-filter-start");
  const dateEnd = document.getElementById("date-filter-end");
  const dateLabel = document.getElementById("date-filter-label");

  // Reset controls
  catGroup.style.display = "none";
  dateGroup.style.display = "none";

  const categories = payload.filters?.categories || {};
  const dates = payload.filters?.dates || {};

  // If categorical filter available
  const catKeys = Object.keys(categories);
  if (catKeys.length > 0) {
    const primaryCat = catKeys[0];
    catLabel.textContent = `${primaryCat.replace("_", " ").toUpperCase()}`;
    catFilter.innerHTML = '<option value="ALL">All Categories</option>';
    categories[primaryCat].forEach((val) => {
      const opt = document.createElement("option");
      opt.value = val;
      opt.textContent = val;
      catFilter.appendChild(opt);
    });
    catFilter.dataset.column = primaryCat;
    catGroup.style.display = "flex";
  }

  // If date filter available
  const dateKeys = Object.keys(dates);
  if (dateKeys.length > 0) {
    const primaryDate = dateKeys[0];
    dateLabel.textContent = `${primaryDate.replace("_", " ").toUpperCase()}`;
    const range = dates[primaryDate];
    dateStart.value = range.min || "";
    dateStart.min = range.min || "";
    dateStart.max = range.max || "";
    dateStart.dataset.column = primaryDate;

    dateEnd.value = range.max || "";
    dateEnd.min = range.min || "";
    dateEnd.max = range.max || "";
    dateEnd.dataset.column = primaryDate;

    dateGroup.style.display = "flex";
  }
}

function applyClientFilters() {
  if (!currentPayload || !currentPayload.records) return;

  const catFilter = document.getElementById("category-filter");
  const dateStart = document.getElementById("date-filter-start");
  const dateEnd = document.getElementById("date-filter-end");
  const searchInput = document.getElementById("table-search-input");

  const selectedCat = catFilter?.value || "ALL";
  const catCol = catFilter?.dataset?.column;

  const startDate = dateStart?.value;
  const endDate = dateEnd?.value;
  const dateCol = dateStart?.dataset?.column;

  const searchTerm = searchInput?.value?.toLowerCase()?.trim() || "";

  filteredRecords = currentPayload.records.filter((row) => {
    // 1. Category match
    if (selectedCat !== "ALL" && catCol && row[catCol] !== undefined) {
      if (String(row[catCol]) !== selectedCat) return false;
    }

    // 2. Date match
    if (dateCol && row[dateCol] !== undefined) {
      const rowDate = String(row[dateCol]).substring(0, 10);
      if (startDate && rowDate < startDate) return false;
      if (endDate && rowDate > endDate) return false;
    }

    // 3. Search query match
    if (searchTerm) {
      const hasMatch = Object.values(row).some((val) =>
        String(val).toLowerCase().includes(searchTerm)
      );
      if (!hasMatch) return false;
    }

    return true;
  });

  tableCurrentPage = 1;
  renderTableRows();
  updateFilteredKPIs();
  updateFilteredCharts();
}

function resetFilters() {
  const catFilter = document.getElementById("category-filter");
  if (catFilter) catFilter.value = "ALL";

  const dateStart = document.getElementById("date-filter-start");
  const dateEnd = document.getElementById("date-filter-end");
  if (currentPayload && currentPayload.filters?.dates) {
    const dateKeys = Object.keys(currentPayload.filters.dates);
    if (dateKeys.length > 0) {
      const range = currentPayload.filters.dates[dateKeys[0]];
      if (dateStart) dateStart.value = range.min || "";
      if (dateEnd) dateEnd.value = range.max || "";
    }
  }

  const searchInput = document.getElementById("table-search-input");
  if (searchInput) searchInput.value = "";

  if (currentPayload) {
    filteredRecords = [...currentPayload.records];
    tableCurrentPage = 1;
    renderTableRows();
    renderKPICards(currentPayload.kpis);
    renderVisualsGrid(currentPayload.charts);
  }
}

// --------------------------------------------------------------------------
// Zone 2: Executive KPI Strip
// --------------------------------------------------------------------------

function renderKPICards(kpis) {
  const grid = document.getElementById("kpi-cards-grid");
  if (!grid) return;
  grid.innerHTML = "";

  if (!kpis || kpis.length === 0) {
    grid.innerHTML = '<div class="kpi-card"><div class="kpi-title">Records</div><div class="kpi-value">0</div></div>';
    return;
  }

  kpis.forEach((kpi) => {
    const card = document.createElement("div");
    let cardTypeClass = "";
    if (kpi.card_type === "quality") cardTypeClass = "kpi-quality";
    if (kpi.card_type === "rejected") cardTypeClass = "kpi-rejected";

    card.className = `kpi-card ${cardTypeClass}`;
    card.id = `kpi-card-${kpi.id}`;

    card.innerHTML = `
      <div class="kpi-header">
        <span class="kpi-title">${kpi.title}</span>
        <span class="badge ${kpi.status === 'HEALTHY' ? 'badge-success' : kpi.status === 'WARNING' ? 'badge-warning' : 'badge-primary'}">${kpi.status}</span>
      </div>
      <div class="kpi-value">${kpi.value}</div>
      <div class="kpi-subtitle">${kpi.subtitle}</div>
    `;
    grid.appendChild(card);
  });
}

function updateFilteredKPIs() {
  if (!currentPayload) return;
  // Dynamically update total records KPI
  const countCard = document.getElementById("kpi-card-total_records");
  if (countCard) {
    const valElem = countCard.querySelector(".kpi-value");
    const subElem = countCard.querySelector(".kpi-subtitle");
    if (valElem) valElem.textContent = filteredRecords.length.toLocaleString();
    if (subElem) subElem.textContent = `Filtered subset (of ${currentPayload.row_count} total)`;
  }
}

// --------------------------------------------------------------------------
// Zone 3: Primary Analytical Visuals
// --------------------------------------------------------------------------

function renderVisualsGrid(charts) {
  const grid = document.getElementById("visuals-grid");
  const emptyMsg = document.getElementById("empty-charts-message");
  if (!grid) return;

  // Destroy previous Chart instances to prevent canvas reuse errors
  Object.values(chartInstances).forEach((inst) => inst.destroy());
  chartInstances = {};
  grid.innerHTML = "";

  if (!charts || charts.length === 0) {
    grid.style.display = "none";
    if (emptyMsg) emptyMsg.style.display = "flex";
    return;
  }

  grid.style.display = "grid";
  if (emptyMsg) emptyMsg.style.display = "none";

  charts.forEach((chartSpec) => {
    const card = document.createElement("div");
    card.className = "chart-card";
    card.id = `chart-card-${chartSpec.chart_id}`;

    const canvasId = `canvas-${chartSpec.chart_id}`;
    card.innerHTML = `
      <div class="chart-header">
        <div class="chart-title-group">
          <h4 class="chart-title">${chartSpec.title}</h4>
          <span class="chart-rationale-tag">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/>
            </svg>
            ${chartSpec.rationale}
          </span>
        </div>
        <span class="badge badge-primary">${Math.round(chartSpec.confidence * 100)}% Match</span>
      </div>
      <div class="chart-canvas-wrapper">
        <canvas id="${canvasId}"></canvas>
      </div>
    `;
    grid.appendChild(card);

    // Initialize Chart.js instance
    setTimeout(() => {
      createChartInstance(canvasId, chartSpec);
    }, 10);
  });
}

function createChartInstance(canvasId, spec) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;

  const ctx = canvas.getContext("2d");
  let chartType = spec.chart_type;
  if (chartType === "histogram") chartType = "bar";

  const config = {
    type: chartType,
    data: {
      labels: spec.labels,
      datasets: spec.datasets,
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: chartType === "doughnut" || spec.datasets.length > 1,
          labels: {
            color: "#9ca3af",
            font: { family: "'Inter', sans-serif", size: 12 },
          },
        },
        tooltip: {
          backgroundColor: "rgba(17, 24, 39, 0.95)",
          titleColor: "#ffffff",
          bodyColor: "#cbd5e1",
          borderColor: "rgba(255, 255, 255, 0.1)",
          borderWidth: 1,
          padding: 10,
          boxPadding: 4,
          usePointStyle: true,
        },
      },
      scales: chartType !== "doughnut" ? {
        x: {
          grid: { color: "rgba(255, 255, 255, 0.05)" },
          ticks: { color: "#9ca3af", font: { size: 11 } },
        },
        y: {
          grid: { color: "rgba(255, 255, 255, 0.05)" },
          ticks: { color: "#9ca3af", font: { size: 11 } },
        },
      } : {},
    },
  };

  chartInstances[canvasId] = new Chart(ctx, config);
}

function updateFilteredCharts() {
  // If no filters active or empty records, return
  if (!currentPayload) return;

  // Dynamically re-aggregate bar and category charts based on filtered records
  if (currentPayload.charts && currentPayload.charts.length > 0) {
    currentPayload.charts.forEach((chartSpec) => {
      const canvasId = `canvas-${chartSpec.chart_id}`;
      const chart = chartInstances[canvasId];
      if (!chart) return;

      if (chartSpec.chart_type === "bar" && chartSpec.required_columns.length >= 2) {
        const catCol = chartSpec.required_columns[0];
        const metCol = chartSpec.required_columns[1];

        const aggregated = {};
        filteredRecords.forEach((r) => {
          const k = String(r[catCol] || "Unknown");
          const v = parseFloat(r[metCol]) || 0;
          aggregated[k] = (aggregated[k] || 0) + v;
        });

        const newLabels = Object.keys(aggregated);
        const newValues = Object.values(aggregated);

        chart.data.labels = newLabels;
        if (chart.data.datasets[0]) {
          chart.data.datasets[0].data = newValues;
        }
        chart.update();
      }
    });
  }
}

// --------------------------------------------------------------------------
// Zone 4: Granular Audit Records Table
// --------------------------------------------------------------------------

function renderTableHead(columns) {
  const thead = document.getElementById("data-table-head");
  if (!thead) return;
  thead.innerHTML = "";

  const tr = document.createElement("tr");
  columns.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col.name.replace("_", " ").toUpperCase();
    th.title = `Type: ${col.data_type} | Role: ${col.role}`;
    tr.appendChild(th);
  });
  thead.appendChild(tr);
}

function renderTableRows() {
  const tbody = document.getElementById("data-table-body");
  const counter = document.getElementById("table-row-counter");
  const pageIndicator = document.getElementById("page-indicator");
  const paginationInfo = document.getElementById("pagination-info");
  const btnPrev = document.getElementById("btn-prev-page");
  const btnNext = document.getElementById("btn-next-page");

  if (!tbody || !currentPayload) return;
  tbody.innerHTML = "";

  const total = filteredRecords.length;
  if (counter) counter.textContent = `${total} verified records`;

  if (total === 0) {
    tbody.innerHTML = `<tr><td colspan="${currentPayload.columns.length || 1}" style="text-align: center; padding: 24px; color: var(--text-muted);">No records found matching current slicers.</td></tr>`;
    if (pageIndicator) pageIndicator.textContent = "Page 1 of 1";
    if (paginationInfo) paginationInfo.textContent = "Showing 0 of 0 records";
    if (btnPrev) btnPrev.disabled = true;
    if (btnNext) btnNext.disabled = true;
    return;
  }

  const maxPages = Math.ceil(total / TABLE_PAGE_SIZE);
  if (tableCurrentPage > maxPages) tableCurrentPage = maxPages;

  const startIndex = (tableCurrentPage - 1) * TABLE_PAGE_SIZE;
  const endIndex = Math.min(startIndex + TABLE_PAGE_SIZE, total);

  const pageSlice = filteredRecords.slice(startIndex, endIndex);

  pageSlice.forEach((row) => {
    const tr = document.createElement("tr");
    currentPayload.columns.forEach((col) => {
      const td = document.createElement("td");
      const val = row[col.name];
      td.textContent = val !== null && val !== undefined ? String(val) : "—";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });

  if (pageIndicator) pageIndicator.textContent = `Page ${tableCurrentPage} of ${maxPages}`;
  if (paginationInfo) paginationInfo.textContent = `Showing ${startIndex + 1}–${endIndex} of ${total} records`;
  if (btnPrev) btnPrev.disabled = tableCurrentPage <= 1;
  if (btnNext) btnNext.disabled = tableCurrentPage >= maxPages;
}

function exportFilteredToCSV() {
  if (!currentPayload || filteredRecords.length === 0) {
    showNotification("No records available to export.", "warning");
    return;
  }

  const cols = currentPayload.columns.map((c) => c.name);
  const rows = [cols.join(",")];

  filteredRecords.forEach((r) => {
    const line = cols.map((c) => {
      let val = r[c] !== null && r[c] !== undefined ? String(r[c]) : "";
      if (val.includes(",") || val.includes('"') || val.includes("\n")) {
        val = `"${val.replace(/"/g, '""')}"`;
      }
      return val;
    }).join(",");
    rows.push(line);
  });

  const blob = new Blob([rows.join("\n")], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.setAttribute("href", url);
  link.setAttribute("download", `${activeDataset}_verified_records.csv`);
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

// --------------------------------------------------------------------------
// Upload Modal Logic
// --------------------------------------------------------------------------

function setupUploadModal() {
  const btnOpen = document.getElementById("btn-open-upload");
  const modal = document.getElementById("upload-modal");
  const btnClose = document.getElementById("btn-close-upload-modal");
  const btnCancel = document.getElementById("btn-cancel-upload");
  const dropzone = document.getElementById("upload-dropzone");
  const fileInput = document.getElementById("file-input");
  const selectedFileCard = document.getElementById("selected-file-card");
  const selectedFileName = document.getElementById("selected-file-name");
  const selectedFileMeta = document.getElementById("selected-file-meta");
  const btnRemoveFile = document.getElementById("btn-remove-file");
  const btnSubmit = document.getElementById("btn-submit-upload");
  const feedback = document.getElementById("upload-feedback");

  let currentFile = null;

  const openModal = () => {
    modal.style.display = "flex";
    currentFile = null;
    fileInput.value = "";
    selectedFileCard.style.display = "none";
    dropzone.style.display = "flex";
    btnSubmit.disabled = true;
    feedback.style.display = "none";
  };

  const closeModal = () => {
    modal.style.display = "none";
  };

  if (btnOpen) btnOpen.addEventListener("click", openModal);
  if (btnClose) btnClose.addEventListener("click", closeModal);
  if (btnCancel) btnCancel.addEventListener("click", closeModal);

  if (dropzone && fileInput) {
    dropzone.addEventListener("click", () => fileInput.click());

    dropzone.addEventListener("dragover", (e) => {
      e.preventDefault();
      dropzone.classList.add("drag-active");
    });
    dropzone.addEventListener("dragleave", () => {
      dropzone.classList.remove("drag-active");
    });
    dropzone.addEventListener("drop", (e) => {
      e.preventDefault();
      dropzone.classList.remove("drag-active");
      if (e.dataTransfer.files && e.dataTransfer.files[0]) {
        handleFileSelection(e.dataTransfer.files[0]);
      }
    });

    fileInput.addEventListener("change", (e) => {
      if (e.target.files && e.target.files[0]) {
        handleFileSelection(e.target.files[0]);
      }
    });
  }

  function handleFileSelection(file) {
    const ext = file.name.split(".").pop().toLowerCase();
    if (!["csv", "xlsx", "json"].includes(ext)) {
      showModalFeedback("Unsupported file format. Please upload CSV, XLSX, or JSON.", "danger");
      return;
    }

    currentFile = file;
    selectedFileName.textContent = file.name;
    const sizeKB = Math.round(file.size / 1024);
    selectedFileMeta.textContent = `${sizeKB} KB • ${ext.toUpperCase()}`;

    dropzone.style.display = "none";
    selectedFileCard.style.display = "flex";
    btnSubmit.disabled = false;
    feedback.style.display = "none";
  }

  if (btnRemoveFile) {
    btnRemoveFile.addEventListener("click", () => {
      currentFile = null;
      fileInput.value = "";
      selectedFileCard.style.display = "none";
      dropzone.style.display = "flex";
      btnSubmit.disabled = true;
    });
  }

  if (btnSubmit) {
    btnSubmit.addEventListener("click", async () => {
      if (!currentFile) return;

      btnSubmit.disabled = true;
      btnSubmit.textContent = "Uploading...";

      const formData = new FormData();
      formData.append("file", currentFile);

      try {
        const res = await fetch("/api/upload", {
          method: "POST",
          body: formData,
        });
        const data = await res.json();

        if (data.success) {
          showNotification(data.message, "success");
          closeModal();
          // Reload datasets list
          await loadDatasetList();
          // Prompt user to run pipeline
          showNotification(`File '${data.filename}' landed in data/raw/. Click 'Run Pipeline' to process it!`, "info");
        } else {
          showModalFeedback(data.error || "Upload failed.", "danger");
        }
      } catch (err) {
        showModalFeedback(`Upload error: ${err.message}`, "danger");
      } finally {
        btnSubmit.disabled = false;
        btnSubmit.textContent = "Upload Dataset";
      }
    });
  }

  function showModalFeedback(msg, type) {
    feedback.textContent = msg;
    feedback.className = `modal-feedback notification-banner notification-${type}`;
    feedback.style.display = "block";
  }
}

// --------------------------------------------------------------------------
// Run Pipeline Modal Logic
// --------------------------------------------------------------------------

function setupPipelineModal() {
  const btnOpen = document.getElementById("btn-run-pipeline");
  const modal = document.getElementById("pipeline-modal");
  const btnClose = document.getElementById("btn-close-pipeline-modal");
  const btnCancel = document.getElementById("btn-cancel-run");
  const btnConfirm = document.getElementById("btn-confirm-run");
  const chkReset = document.getElementById("chk-reset-checkpoints");
  const statusElem = document.getElementById("pipeline-modal-status");

  const openModal = () => {
    modal.style.display = "flex";
    statusElem.style.display = "none";
    btnConfirm.disabled = false;
  };

  const closeModal = () => {
    modal.style.display = "none";
  };

  if (btnOpen) btnOpen.addEventListener("click", openModal);
  if (btnClose) btnClose.addEventListener("click", closeModal);
  if (btnCancel) btnCancel.addEventListener("click", closeModal);

  if (btnConfirm) {
    btnConfirm.addEventListener("click", async () => {
      btnConfirm.disabled = true;
      statusElem.style.display = "flex";

      const resetCheckpoints = chkReset ? chkReset.checked : false;

      try {
        const res = await fetch("/api/run-pipeline", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ reset_checkpoints: resetCheckpoints }),
        });
        const data = await res.json();

        if (data.success) {
          showNotification(`Pipeline executed successfully in ${data.duration_seconds}s! Status: ${data.pipeline_status}`, "success");
          closeModal();

          // Render stage flow in tracker accordion
          renderPipelineStages(data.summary);

          // Refresh status and dashboard
          await checkSystemStatus();
          await loadDatasetList();
          if (activeDataset) {
            await loadDashboardData(activeDataset);
          }
        } else {
          showNotification(`Pipeline run error: ${data.error || data.message}`, "danger");
        }
      } catch (err) {
        showNotification(`Failed to invoke pipeline: ${err.message}`, "danger");
      } finally {
        statusElem.style.display = "none";
        btnConfirm.disabled = false;
      }
    });
  }
}

function renderPipelineStages(summary) {
  const trackerSection = document.getElementById("pipeline-tracker-section");
  const container = document.getElementById("stages-flow-container");
  const runIdText = document.getElementById("pipeline-run-id-text");
  const overallBadge = document.getElementById("pipeline-overall-status");

  if (!container || !summary) return;
  trackerSection.style.display = "block";

  if (runIdText) runIdText.textContent = `Run ID: ${summary.run_id}`;
  if (overallBadge) {
    overallBadge.textContent = summary.pipeline_status;
    overallBadge.className = `badge ${summary.pipeline_status === 'COMPLETED' ? 'badge-success' : 'badge-warning'}`;
  }

  container.innerHTML = "";
  const stages = summary.stages || [];

  stages.forEach((st) => {
    const pill = document.createElement("div");
    pill.className = "stage-pill";

    const isSuccess = st.status === "SUCCESS";
    const badgeClass = isSuccess ? "badge-success" : st.status === "SKIPPED" ? "badge-primary" : "badge-danger";

    pill.innerHTML = `
      <div class="stage-pill-header">
        <span class="stage-name">${st.stage_name}</span>
        <span class="stage-status-badge badge ${badgeClass}">${st.status}</span>
      </div>
      <span class="stage-duration">${st.duration_seconds || 0}s</span>
    `;
    container.appendChild(pill);
  });
}

// --------------------------------------------------------------------------
// Quality & Power BI Inspector Modals
// --------------------------------------------------------------------------

function setupInspectorModals() {
  // Quality Audit Modal
  const btnOpenQuality = document.getElementById("btn-view-quality-audit");
  const qualityModal = document.getElementById("quality-modal");
  const btnCloseQuality = document.getElementById("btn-close-quality-modal");
  const btnCloseQuality2 = document.getElementById("btn-close-quality-btn");
  const qualityContent = document.getElementById("quality-modal-content");

  if (btnOpenQuality && qualityModal) {
    btnOpenQuality.addEventListener("click", () => {
      qualityModal.style.display = "flex";
      renderQualityModalContent(qualityContent);
    });
  }
  if (btnCloseQuality) btnCloseQuality.addEventListener("click", () => qualityModal.style.display = "none");
  if (btnCloseQuality2) btnCloseQuality2.addEventListener("click", () => qualityModal.style.display = "none");

  // Power BI Spec Modal
  const btnOpenPbi = document.getElementById("btn-view-powerbi-spec");
  const pbiModal = document.getElementById("powerbi-modal");
  const btnClosePbi = document.getElementById("btn-close-powerbi-modal");
  const btnClosePbi2 = document.getElementById("btn-close-powerbi-btn");
  const pbiContent = document.getElementById("powerbi-modal-content");

  if (btnOpenPbi && pbiModal) {
    btnOpenPbi.addEventListener("click", () => {
      pbiModal.style.display = "flex";
      renderPowerBIModalContent(pbiContent);
    });
  }
  if (btnClosePbi) btnClosePbi.addEventListener("click", () => pbiModal.style.display = "none");
  if (btnClosePbi2) btnClosePbi2.addEventListener("click", () => pbiModal.style.display = "none");
}

function renderQualityModalContent(container) {
  if (!container || !currentPayload) return;
  const q = currentPayload.quality_metrics || {};

  container.innerHTML = `
    <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px;">
      <div class="kpi-card">
        <span class="kpi-title">Quality Score</span>
        <div class="kpi-value" style="font-size: 1.4rem;">${q.quality_score}%</div>
      </div>
      <div class="kpi-card">
        <span class="kpi-title">Total Validated</span>
        <div class="kpi-value" style="font-size: 1.4rem;">${q.valid_rows} / ${q.total_rows}</div>
      </div>
      <div class="kpi-card">
        <span class="kpi-title">Rule Violations</span>
        <div class="kpi-value" style="font-size: 1.4rem;">${q.total_failures}</div>
      </div>
      <div class="kpi-card">
        <span class="kpi-title">Quarantined</span>
        <div class="kpi-value" style="font-size: 1.4rem;">${q.rejected_records_count || 0}</div>
      </div>
    </div>

    <h4 style="font-size: 0.9rem; margin-bottom: 8px;">Executed Validation Rules</h4>
    <div style="display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 16px;">
      ${(q.rules_executed || []).map((r) => `<span class="badge badge-primary">${r}</span>`).join("") || '<span style="color: var(--text-muted);">Standard quality rules applied</span>'}
    </div>

    <h4 style="font-size: 0.9rem; margin-bottom: 8px;">Detailed Violation Breakdown</h4>
    <table class="data-table" style="font-size: 0.78rem;">
      <thead>
        <tr>
          <th>Violation Category</th>
          <th>Count</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody>
        <tr><td>Nullability Violations</td><td>${q.null_violations}</td><td>${q.null_violations === 0 ? '✓ CLEAN' : '⚠ VIOLATED'}</td></tr>
        <tr><td>Uniqueness Violations</td><td>${q.uniqueness_violations}</td><td>${q.uniqueness_violations === 0 ? '✓ CLEAN' : '⚠ VIOLATED'}</td></tr>
        <tr><td>Range & Boundary Violations</td><td>${q.range_violations}</td><td>${q.range_violations === 0 ? '✓ CLEAN' : '⚠ VIOLATED'}</td></tr>
        <tr><td>Format & Regex Violations</td><td>${q.regex_violations}</td><td>${q.regex_violations === 0 ? '✓ CLEAN' : '⚠ VIOLATED'}</td></tr>
        <tr><td>Data Type Violations</td><td>${q.type_violations}</td><td>${q.type_violations === 0 ? '✓ CLEAN' : '⚠ VIOLATED'}</td></tr>
        <tr><td>Schema Violations</td><td>${q.schema_violations}</td><td>${q.schema_violations === 0 ? '✓ CLEAN' : '⚠ VIOLATED'}</td></tr>
      </tbody>
    </table>
  `;
}

function renderPowerBIModalContent(container) {
  if (!container || !currentPayload) return;
  const pbi = currentPayload.powerbi_info || {};

  container.innerHTML = `
    <div class="notification-banner notification-info" style="margin-bottom: 16px;">
      <span><strong>ℹ️ Power BI Architecture Disclaimer:</strong> ${pbi.disclaimer}</span>
    </div>

    <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 20px;">
      <div class="kpi-card">
        <span class="kpi-title">Table Archetype</span>
        <div class="kpi-value" style="font-size: 1.3rem;">${pbi.table_archetype || 'TABULAR'}</div>
      </div>
      <div class="kpi-card">
        <span class="kpi-title">Semantic Discovery</span>
        <div class="kpi-value" style="font-size: 1.3rem;">${pbi.is_in_semantic_model ? 'Indexed' : 'Dynamic'}</div>
      </div>
      <div class="kpi-card">
        <span class="kpi-title">Power BI Visual Specs</span>
        <div class="kpi-value" style="font-size: 1.3rem;">${(pbi.recommendations || []).length}</div>
      </div>
    </div>

    <h4 style="font-size: 0.9rem; margin-bottom: 8px;">Recommended Visualizations from <code>dashboard_spec.json</code></h4>
    <div style="display: flex; flex-direction: column; gap: 8px;">
      ${(pbi.recommendations || []).map((r) => `
        <div style="background: #1e293b; padding: 10px 14px; border-radius: 6px; border: 1px solid var(--border-subtle); display: flex; justify-content: space-between; align-items: center;">
          <div>
            <div style="font-weight: 600; font-size: 0.85rem;">${r.title}</div>
            <div style="font-size: 0.74rem; color: var(--text-muted);">${r.rationale}</div>
          </div>
          <span class="badge badge-primary">${r.visualization_type.toUpperCase()}</span>
        </div>
      `).join("") || '<p style="color: var(--text-muted); font-size: 0.82rem;">No pre-indexed visuals in powerbi/dashboard_spec.json. Rendered using dynamic column discovery engine.</p>'}
    </div>
  `;
}

// --------------------------------------------------------------------------
// Utility Notifications
// --------------------------------------------------------------------------

function showNotification(msg, type = "info") {
  const container = document.getElementById("notification-container");
  if (!container) return;

  const banner = document.createElement("div");
  banner.className = `notification-banner notification-${type}`;
  banner.innerHTML = `
    <span>${msg}</span>
    <button style="background: transparent; border: none; color: inherit; cursor: pointer; font-size: 1.1rem;" onclick="this.parentElement.remove()">&times;</button>
  `;

  container.appendChild(banner);

  // Auto remove after 7 seconds
  setTimeout(() => {
    if (banner.parentElement) banner.remove();
  }, 7000);
}
