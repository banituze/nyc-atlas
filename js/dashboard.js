/* ═══════════════════════════════════════════════════════════════════════
   NYC TAXI CARTOGRAPHIC ATLAS
   ═══════════════════════════════════════════════════════════════════════ */
const INK = '#000000';
const INK_SOFT = '#1a1a1a';
const INK_FADED = '#555555';
const INK_GHOST = '#999999';
const PAPER = '#ffffff';
const PAPER_DEEP = '#f5f5f5';
const DAYS_SHORT = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN'];
const DAYS_FULL = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
const charts = {};
let currentPage = 1;
const loadedViews = { overview: false, rhythms: false, atlas: false, ledger: false, insights: false };

// ─── Chart.js global defaults ──────────────────────────────────────────
Chart.defaults.font.family = "'DM Mono', monospace";
Chart.defaults.font.size = 10;
Chart.defaults.color = INK;
Chart.defaults.borderColor = INK;
Chart.defaults.plugins.legend.display = false;
Chart.defaults.plugins.tooltip.backgroundColor = INK;
Chart.defaults.plugins.tooltip.titleColor = PAPER;
Chart.defaults.plugins.tooltip.bodyColor = PAPER;
Chart.defaults.plugins.tooltip.titleFont = { family: "'DM Mono', monospace", size: 10, weight: '700' };
Chart.defaults.plugins.tooltip.bodyFont = { family: "'DM Mono', monospace", size: 11 };
Chart.defaults.plugins.tooltip.padding = 10;
Chart.defaults.plugins.tooltip.cornerRadius = 0;
Chart.defaults.plugins.tooltip.displayColors = false;
Chart.defaults.plugins.tooltip.titleAlign = 'center';
Chart.defaults.plugins.tooltip.bodyAlign = 'center';

const paperBackgroundPlugin = {
  id: 'paperBackground',
  beforeDraw: (chart) => {
    const { ctx } = chart;
    ctx.save();
    ctx.globalCompositeOperation = 'destination-over';
    ctx.fillStyle = '#ffffff';
    ctx.fillRect(0, 0, chart.width, chart.height);
    ctx.restore();
  }
};
Chart.register(paperBackgroundPlugin);

// ─── Utilities ─────────────────────────────────────────────────────────
function fmt(n) {
  if (n == null) return '—';
  if (Number.isInteger(n)) return String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  return n.toFixed(1).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}
function fmtFull(n) {
  if (n == null) return '—';
  return Math.round(n).toLocaleString();
}
async function fetchJSON(url) {
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(res.statusText);
    return await res.json();
  } catch (e) {
    console.error('Fetch failed:', url, e);
    return null;
  }
}
function paperAxes() {
  return {
    x: {
      grid: { display: false, drawBorder: true, color: INK },
      border: { color: INK, width: 1 },
      ticks: { color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: 6 }
    },
    y: {
      grid: { display: true, color: INK_GHOST, lineWidth: 0.5, drawTicks: false, drawBorder: false },
      border: { display: false },
      ticks: { color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: 10, callback: v => fmt(v) }
    }
  };
}

// ─── SHEET A: GENERAL SURVEY ───────────────────────────────────────────
async function loadOverview() {
  if (loadedViews.overview) return;
  loadedViews.overview = true;
  const stats = await fetchJSON('/api/stats');
  if (stats) {
    document.querySelectorAll('[data-stat]').forEach(el => {
      const key = el.dataset.stat;
      const v = stats[key];
      if (v == null) return;
      if (key === 'total_trips') el.textContent = fmt(v);
      else if (key === 'total_hours') el.textContent = fmt(v);
      else if (key === 'avg_distance_km') el.textContent = v.toFixed(1);
      else if (key === 'avg_speed_kmh') el.textContent = v.toFixed(1);
      else el.textContent = fmt(v);
    });
  }
  const hourly = await fetchJSON('/api/hourly');
  if (hourly) {
    new Chart(document.getElementById('chart-hourly'), {
      type: 'bar',
      data: {
        labels: hourly.map(h => String(h.hour_of_day).padStart(2, '0')),
        datasets: [{
          data: hourly.map(h => h.count),
          backgroundColor: hourly.map(h => {
            const isRush = (h.hour_of_day >= 7 && h.hour_of_day <= 9) || (h.hour_of_day >= 17 && h.hour_of_day <= 19);
            return isRush ? INK : INK_GHOST;
          }),
          borderWidth: 0, barPercentage: 0.78, categoryPercentage: 0.92,
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
        plugins: {
          tooltip: { callbacks: { title: c => `${c[0].label}:00h`, label: c => `${fmtFull(c.parsed.y)} trips` } },
          legend: {
            display: true, position: 'top', align: 'end',
            labels: {
              color: INK, font: { family: "'DM Mono', monospace", size: 9 },
              usePointStyle: true, pointStyle: 'circle', boxWidth: 10, boxHeight: 10, padding: 12,
              generateLabels: () => [
                { text: 'Rush hour', fillStyle: INK, strokeStyle: INK },
                { text: 'Off-peak', fillStyle: INK_GHOST, strokeStyle: INK_GHOST },
              ]
            }
          }
        },
        scales: {
          ...paperAxes(),
          x: { ...paperAxes().x, title: { display: true, text: 'Hour of day', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { top: 6 } } },
          y: { ...paperAxes().y, title: { display: true, text: 'Trip count', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { bottom: 6 } } }
        }
      }
    });
  }
  const durDist = await fetchJSON('/api/duration_distribution');
  if (durDist) {
    new Chart(document.getElementById('chart-duration'), {
      type: 'bar',
      data: {
        labels: durDist.map(d => d.label),
        datasets: [{ data: durDist.map(d => d.count), backgroundColor: INK_SOFT, borderColor: INK, borderWidth: 1, barPercentage: 0.85, categoryPercentage: 0.95 }]
      },
      options: {
        responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
        plugins: { tooltip: { callbacks: { title: c => c[0].label, label: c => `${fmtFull(c.parsed.y)} trips` } } },
        scales: {
          ...paperAxes(),
          x: { ...paperAxes().x, title: { display: true, text: 'Duration bucket', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { top: 6 } } },
          y: { ...paperAxes().y, title: { display: true, text: 'Trip count', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { bottom: 6 } } }
        }
      }
    });
  }
  const speedDist = await fetchJSON('/api/speed_distribution');
  if (speedDist) {
    new Chart(document.getElementById('chart-speed'), {
      type: 'bar',
      data: {
        labels: speedDist.map(d => d.label),
        datasets: [{ data: speedDist.map(d => d.count), backgroundColor: INK, borderColor: INK, borderWidth: 0, barPercentage: 0.85, categoryPercentage: 0.95 }]
      },
      options: {
        responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
        plugins: { tooltip: { callbacks: { title: c => c[0].label, label: c => `${fmtFull(c.parsed.y)} trips` } } },
        scales: {
          ...paperAxes(),
          x: { ...paperAxes().x, title: { display: true, text: 'Speed bucket', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { top: 6 } } },
          y: { ...paperAxes().y, title: { display: true, text: 'Trip count', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { bottom: 6 } } }
        }
      }
    });
  }
}

// ─── SHEET B: TEMPORAL CHARTS ──────────────────────────────────────────
async function loadRhythms() {
  if (loadedViews.rhythms) return;
  loadedViews.rhythms = true;
  const heatData = await fetchJSON('/api/heatmap');
  if (heatData) buildHeatmap(heatData);
  const daily = await fetchJSON('/api/daily');
  if (daily) {
    new Chart(document.getElementById('chart-daily'), {
      type: 'bar',
      data: {
        labels: daily.map(d => DAYS_SHORT[d.day_of_week]),
        datasets: [{ data: daily.map(d => d.count), backgroundColor: daily.map((d, i) => i >= 5 ? INK_FADED : INK), borderWidth: 0, barPercentage: 0.7 }]
      },
      options: {
        responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
        plugins: {
          tooltip: { callbacks: { title: c => DAYS_FULL[daily[c[0].dataIndex].day_of_week], label: c => `${fmtFull(c.parsed.y)} trips` } },
          legend: {
            display: true, position: 'top', align: 'end',
            labels: {
              color: INK, font: { family: "'DM Mono', monospace", size: 9 },
              usePointStyle: true, pointStyle: 'circle', boxWidth: 10, boxHeight: 10, padding: 12,
              generateLabels: () => [
                { text: 'Weekday', fillStyle: INK, strokeStyle: INK },
                { text: 'Weekend', fillStyle: INK_FADED, strokeStyle: INK_FADED },
              ]
            }
          }
        },
        scales: {
          ...paperAxes(),
          x: { ...paperAxes().x, title: { display: true, text: 'Day of week', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { top: 6 } } },
          y: { ...paperAxes().y, title: { display: true, text: 'Trip count', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { bottom: 6 } } }
        }
      }
    });
  }
  const monthly = await fetchJSON('/api/monthly');
  if (monthly) {
    new Chart(document.getElementById('chart-monthly'), {
      type: 'line',
      data: {
        labels: monthly.map(m => m.month_name),
        datasets: [{
          data: monthly.map(m => m.count),
          borderColor: INK, backgroundColor: 'rgba(42, 37, 32, 0.08)', borderWidth: 2,
          tension: 0.3, pointRadius: 5, pointBackgroundColor: PAPER, pointBorderColor: INK,
          pointBorderWidth: 2, pointHoverRadius: 8, pointHoverBackgroundColor: INK,
          pointHoverBorderColor: PAPER, fill: true,
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
        plugins: { tooltip: { callbacks: { title: c => c[0].label, label: c => `${fmtFull(c.parsed.y)} trips` } } },
        scales: {
          ...paperAxes(),
          x: { ...paperAxes().x, title: { display: true, text: 'Month', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { top: 6 } } },
          y: { ...paperAxes().y, title: { display: true, text: 'Trip count', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { bottom: 6 } } }
        }
      }
    });
  }
  const passengers = await fetchJSON('/api/passengers');
  if (passengers) {
    new Chart(document.getElementById('chart-passengers'), {
      type: 'bar',
      data: {
        labels: passengers.map(p => p.passenger_count + ' pax'),
        datasets: [{ data: passengers.map(p => p.count), backgroundColor: INK, borderWidth: 0, barPercentage: 0.75 }]
      },
      options: {
        indexAxis: 'y', responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
        plugins: { tooltip: { callbacks: { label: c => `${fmtFull(c.parsed.x)} trips` } } },
        scales: {
          x: {
            grid: { color: INK_GHOST, lineWidth: 0.5, drawBorder: false }, border: { display: false },
            ticks: { color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, callback: v => fmt(v) },
            title: { display: true, text: 'Trip count', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { top: 6 } }
          },
          y: {
            grid: { display: false }, border: { color: INK },
            ticks: { color: INK, font: { family: "'Cormorant Garamond', serif", size: 13, style: 'italic' } },
            title: { display: true, text: 'Passenger count', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { bottom: 6 } }
          }
        }
      }
    });
  }
  const vendors = await fetchJSON('/api/vendor_comparison');
  if (vendors) {
    new Chart(document.getElementById('chart-vendor'), {
      type: 'bar',
      data: {
        labels: ['Duration (min)', 'Distance (km)', 'Velocity (km/h)'],
        datasets: vendors.map((v, i) => ({
          label: 'Vendor ' + (v.vendor_id === 1 ? 'I' : 'II'),
          data: [Math.round(v.avg_duration_min * 10) / 10, Math.round(v.avg_distance * 100) / 100, Math.round(v.avg_speed * 10) / 10],
          backgroundColor: i === 0 ? INK : INK_GHOST,
          borderWidth: 0, barPercentage: 0.75, categoryPercentage: 0.75,
        }))
      },
      options: {
        responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
        plugins: {
          legend: {
            display: true, position: 'top', align: 'end',
            labels: { color: INK, font: { family: "'DM Mono', monospace", size: 9, style: 'italic' }, usePointStyle: true, pointStyle: 'circle', boxWidth: 12, boxHeight: 12, padding: 14 }
          }
        },
        scales: {
          ...paperAxes(),
          x: { ...paperAxes().x, title: { display: true, text: 'Metric', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { top: 6 } } },
          y: { ...paperAxes().y, title: { display: true, text: 'Value', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { bottom: 6 } } }
        }
      }
    });
  }
}

// ─── Heatmap (Sheet B) ─────────────────────────────────────────────────
function buildHeatmap(data) {
  const grid = {};
  let maxCount = 0;
  data.forEach(d => {
    grid[`${d.hour_of_day}-${d.day_of_week}`] = d.count;
    if (d.count > maxCount) maxCount = d.count;
  });
  const container = document.getElementById('heatmap');
  container.innerHTML = '<canvas id="heatmap-canvas"></canvas><div id="heatmap-tip" class="hm-tip"></div>';
  const canvas = document.getElementById('heatmap-canvas');
  const tooltip = document.getElementById('heatmap-tip');
  const colLabel = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
  const rowLabelW = 38, colLabelH = 20;
  const cssWidth = container.clientWidth || 760;
  const cellW = (cssWidth - rowLabelW) / 7;
  const cellH = 13;
  const cssHeight = colLabelH + cellH * 24;
  const dpr = Math.max(1, window.devicePixelRatio || 1);
  canvas.width = Math.round(cssWidth * dpr);
  canvas.height = Math.round(cssHeight * dpr);
  canvas.style.width = cssWidth + 'px';
  canvas.style.height = cssHeight + 'px';
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.fillStyle = '#ffffff';
  ctx.fillRect(0, 0, cssWidth, cssHeight);
  ctx.fillStyle = '#000000';
  ctx.font = "600 9px 'DM Mono', monospace";
  ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
  for (let d = 0; d < 7; d++) {
    ctx.fillText(colLabel[d].toUpperCase(), rowLabelW + cellW * (d + 0.5), colLabelH / 2);
  }
  ctx.strokeStyle = '#000000'; ctx.lineWidth = 0.5;
  ctx.beginPath();
  ctx.moveTo(rowLabelW, colLabelH - 0.5);
  ctx.lineTo(cssWidth, colLabelH - 0.5);
  ctx.stroke();
  for (let h = 0; h < 24; h++) {
    const y = colLabelH + h * cellH;
    if (h % 3 === 0) {
      ctx.fillStyle = '#555555';
      ctx.font = "9px 'DM Mono', monospace";
      ctx.textAlign = 'right';
      ctx.fillText(String(h).padStart(2, '0') + 'h', rowLabelW - 6, y + cellH / 2);
    }
    for (let d = 0; d < 7; d++) {
      const count = grid[`${h}-${d}`] || 0;
      const t = Math.sqrt(count / maxCount);
      const v = Math.round(255 - 255 * t);
      ctx.fillStyle = `rgb(${v},${v},${v})`;
      const x = rowLabelW + d * cellW;
      ctx.fillRect(x + 0.5, y + 0.5, cellW - 1, cellH - 1);
      ctx.strokeStyle = '#cccccc'; ctx.lineWidth = 0.5;
      ctx.strokeRect(x + 0.5, y + 0.5, cellW - 1, cellH - 1);
    }
  }
  canvas.onmousemove = (e) => {
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left, y = e.clientY - rect.top;
    if (x < rowLabelW || y < colLabelH) { tooltip.style.opacity = '0'; return; }
    const d = Math.floor((x - rowLabelW) / cellW);
    const h = Math.floor((y - colLabelH) / cellH);
    if (d < 0 || d > 6 || h < 0 || h > 23) { tooltip.style.opacity = '0'; return; }
    const count = grid[`${h}-${d}`] || 0;
    tooltip.textContent = `${colLabel[d]} ${String(h).padStart(2, '0')}h · ${fmtFull(count)} trips`;
    tooltip.style.opacity = '1';
    tooltip.style.left = (x + 14) + 'px';
    tooltip.style.top = (y - 8) + 'px';
  };
  canvas.onmouseleave = () => { tooltip.style.opacity = '0'; };
}

// ─── SHEET C: BOROUGH PLATES ───────────────────────────────────────────
async function loadAtlas() {
  if (loadedViews.atlas) return;
  loadedViews.atlas = true;
  const zones = await fetchJSON('/api/zones');
  if (!zones) return;
  new Chart(document.getElementById('chart-zones'), {
    type: 'bar',
    data: {
      labels: zones.map(z => z.zone_name),
      datasets: [{
        data: zones.map(z => z.trip_count),
        backgroundColor: zones.map((z, i) => i < 4 ? INK : INK_GHOST),
        borderWidth: 0, barPercentage: 0.8,
      }]
    },
    options: {
      indexAxis: 'y', responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
      plugins: {
        tooltip: { callbacks: { title: c => c[0].label, label: c => `${fmtFull(c.parsed.x)} pickups` } },
        legend: {
          display: true, position: 'top', align: 'end',
          labels: {
            color: INK, font: { family: "'DM Mono', monospace", size: 9 },
            usePointStyle: true, pointStyle: 'circle', boxWidth: 10, boxHeight: 10, padding: 12,
            generateLabels: () => [
              { text: 'Top zones', fillStyle: INK, strokeStyle: INK },
              { text: 'Other zones', fillStyle: INK_GHOST, strokeStyle: INK_GHOST },
            ]
          }
        }
      },
      scales: {
        x: {
          grid: { color: INK_GHOST, lineWidth: 0.5, drawBorder: false }, border: { display: false },
          ticks: { color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, callback: v => fmt(v) },
          title: { display: true, text: 'Pickup count', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { top: 6 } }
        },
        y: {
          grid: { display: false }, border: { color: INK },
          ticks: { autoSkip: false, color: INK, font: { family: "'Cormorant Garamond', serif", size: 13, style: 'italic' }, padding: 6 },
          title: { display: true, text: 'Zone', color: INK_FADED, font: { family: "'DM Mono', monospace", size: 9 }, padding: { bottom: 6 } }
        }
      }
    }
  });

  const total = zones.reduce((s, z) => s + z.trip_count, 0);
  // Build a proper <table> so mobile card layout works cleanly
  let html = '<table class="sr-table"><thead><tr>';
  html += '<th class="sr-th sr-right">#</th>';
  html += '<th class="sr-th">Sector</th>';
  html += '<th class="sr-th sr-right">Trips</th>';
  html += '<th class="sr-th sr-right">Share</th>';
  html += '<th class="sr-th sr-right">Duration</th>';
  html += '<th class="sr-th sr-right">Velocity</th>';
  html += '</tr></thead><tbody>';
  zones.forEach((z, i) => {
    const pct = total ? ((z.trip_count / total) * 100).toFixed(3) : '0.0';
    html += `<tr class="sr-row${i < 4 ? ' sr-top' : ''}">`;
    html += `<td class="sr-td sr-rank" data-label="#">${String(i + 1).padStart(2, '0')}</td>`;
    html += `<td class="sr-td sr-name" data-label="Sector">${z.zone_name}</td>`;
    html += `<td class="sr-td sr-right sr-mono" data-label="Trips">${fmt(z.trip_count)}</td>`;
    html += `<td class="sr-td sr-right sr-dim" data-label="Share">${pct}%</td>`;
    html += `<td class="sr-td sr-right sr-dim" data-label="Duration">${z.avg_duration_min ? z.avg_duration_min.toFixed(1) + ' min' : '—'}</td>`;
    html += `<td class="sr-td sr-right sr-dim" data-label="Velocity">${z.avg_speed ? z.avg_speed.toFixed(1) + ' km/h' : '—'}</td>`;
    html += '</tr>';
  });
  html += '</tbody></table>';
  document.getElementById('sector-register').innerHTML = html;
}

// ─── SHEET D: FIELD NOTEBOOK ──────────────────────────────────────────
async function loadLedger(page = 1) {
  currentPage = page;
  const params = new URLSearchParams({ page, per_page: 40 });

  const tripId     = document.getElementById('filter-id').value.trim();
  const hour       = document.getElementById('filter-hour').value;
  const day        = document.getElementById('filter-day').value;
  const month      = document.getElementById('filter-month').value;
  const vendor     = document.getElementById('filter-vendor').value;
  const passengers = document.getElementById('filter-passengers').value;
  const sortBy     = document.getElementById('filter-sort').value;
  const order      = document.getElementById('filter-order').value;

  if (tripId)     params.set('trip_id', tripId);
  if (hour)       params.set('hour', hour);
  if (day)        params.set('day', day);
  if (month)      params.set('month', month);
  if (vendor)     params.set('vendor', vendor);
  if (passengers) params.set('passengers', passengers);
  params.set('sort', sortBy);
  params.set('order', order);

  const data = await fetchJSON('/api/trips?' + params);
  if (!data) {
    document.getElementById('ledger-table').innerHTML = '<p style="padding:40px;text-align:center;color:#756c5b;font-style:italic">No records to show. Run the ETL pipeline first.</p>';
    return;
  }
  let html = '<table><thead><tr>';
  html += '<th>Trip ID</th><th>Vendor</th><th>Pickup</th><th>Pax</th><th>Duration</th><th>Distance</th><th>Velocity</th>';
  html += '</tr></thead><tbody>';
  data.trips.forEach(t => {
    html += `<tr>
      <td class="mono">${t.trip_id.slice(0, 12)}</td>
      <td>${t.vendor_id === 1 ? 'I' : 'II'}</td>
      <td class="mono">${t.pickup_datetime}</td>
      <td>${t.passenger_count}</td>
      <td>${(t.trip_duration / 60).toFixed(1)} min</td>
      <td>${t.distance_km.toFixed(2)} km</td>
      <td>${t.speed_kmh.toFixed(1)} km/h</td>
    </tr>`;
  });
  html += '</tbody></table>';
  document.getElementById('ledger-table').innerHTML = html;
  const totalPages = Math.ceil(data.total / data.per_page);
  document.getElementById('ledger-pagination').innerHTML = `
    <span>${fmtFull(data.total)} records · page ${data.page} of ${totalPages}</span>
    <div style="display:flex;gap:8px">
      <button class="page-btn" ${page <= 1 ? 'disabled' : ''} onclick="loadLedger(${page - 1})">← prev</button>
      <button class="page-btn" ${page >= totalPages ? 'disabled' : ''} onclick="loadLedger(${page + 1})">next →</button>
    </div>
  `;
}

// ─── SHEET E: MARGINALIA ───────────────────────────────────────────────
async function loadInsights() {
  if (loadedViews.insights) return;
  loadedViews.insights = true;
  const insights = await fetchJSON('/api/insights');
  if (!insights) return;
  const sections = [];
  if (insights[0]) {
    const rows = insights[0].data;
    const rush = rows.find(r => r.period === 'Rush Hour') || {};
    const off  = rows.find(r => r.period === 'Off-Peak') || {};
    const speedDelta = (off.avg_speed && rush.avg_speed) ? (((rush.avg_speed - off.avg_speed) / off.avg_speed) * 100).toFixed(1) : '0';
    const durDelta   = (off.avg_duration && rush.avg_duration) ? (((rush.avg_duration - off.avg_duration) / off.avg_duration) * 100).toFixed(1) : '0';
    sections.push(`
      <article class="margin-card">
        <div class="margin-numeral">I.</div>
        <div class="margin-content">
          <div class="margin-tag">Marginal note 01 · the rush hour tax</div>
          <h3 class="margin-title">${insights[0].title}</h3>
          <p class="margin-prose">${insights[0].interpretation}</p>
          <table class="margin-table">
            <thead><tr><th>Period</th><th>Velocity</th><th>Duration</th><th>Trips</th></tr></thead>
            <tbody>
              <tr>
                <td>Off-Peak</td>
                <td>${off.avg_speed != null ? off.avg_speed.toFixed(1) : '—'}<span class="unit">km/h</span></td>
                <td>${off.avg_duration != null ? off.avg_duration.toFixed(1) : '—'}<span class="unit">min</span></td>
                <td>${fmt(off.trips || 0)}</td>
              </tr>
              <tr>
                <td>Rush Hour</td>
                <td>${rush.avg_speed != null ? rush.avg_speed.toFixed(1) : '—'}<span class="unit">km/h</span></td>
                <td>${rush.avg_duration != null ? rush.avg_duration.toFixed(2) : '—'}<span class="unit">min</span></td>
                <td>${fmt(rush.trips || 0)}</td>
              </tr>
              <tr class="delta-row">
                <td>Δ</td>
                <td>${parseFloat(speedDelta) > 0 ? '+' : ''}${speedDelta}%</td>
                <td>${parseFloat(durDelta) > 0 ? '+' : ''}${durDelta}%</td>
                <td>—</td>
              </tr>
            </tbody>
          </table>
        </div>
      </article>`);
  }
  if (insights[1]) {
    const rows = insights[1].data;
    const wkday = rows.find(r => r.type === 'Weekday') || {};
    const wkend = rows.find(r => r.type === 'Weekend') || {};
    sections.push(`
      <article class="margin-card">
        <div class="margin-numeral">II.</div>
        <div class="margin-content">
          <div class="margin-tag">Marginal note 02 · the weekend atlas</div>
          <h3 class="margin-title">${insights[1].title}</h3>
          <p class="margin-prose">${insights[1].interpretation}</p>
          <table class="margin-table">
            <thead><tr><th>Type</th><th>Distance</th><th>Duration</th><th>Velocity</th><th>Trips</th></tr></thead>
            <tbody>
              <tr>
                <td>Weekday</td>
                <td>${wkday.avg_distance != null ? wkday.avg_distance.toFixed(2) : '—'}<span class="unit">km</span></td>
                <td>${wkday.avg_duration != null ? wkday.avg_duration.toFixed(1) : '—'}<span class="unit">min</span></td>
                <td>${wkday.avg_speed != null ? wkday.avg_speed.toFixed(1) : '—'}<span class="unit">km/h</span></td>
                <td>${fmt(wkday.trips || 0)}</td>
              </tr>
              <tr>
                <td>Weekend</td>
                <td>${wkend.avg_distance != null ? wkend.avg_distance.toFixed(2) : '—'}<span class="unit">km</span></td>
                <td>${wkend.avg_duration != null ? wkend.avg_duration.toFixed(1) : '—'}<span class="unit">min</span></td>
                <td>${wkend.avg_speed != null ? wkend.avg_speed.toFixed(1) : '—'}<span class="unit">km/h</span></td>
                <td>${fmt(wkend.trips || 0)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </article>`);
  }
  if (insights[2]) {
    const rows = insights[2].data;
    const totalTop = rows.reduce((s, r) => s + (r.trip_count || 0), 0);
    const tableRows = rows.map((r, i) => `
      <tr>
        <td>${String(i + 1).padStart(2, '0')}</td>
        <td>${r.zone_name}</td>
        <td>${fmt(r.trip_count)}</td>
        <td>${totalTop ? ((r.trip_count / totalTop) * 100).toFixed(1) : '—'}%</td>
        <td>${r.avg_speed != null ? r.avg_speed.toFixed(1) : '—'}<span class="unit">km/h</span></td>
      </tr>`).join('');
    sections.push(`
      <article class="margin-card">
        <div class="margin-numeral">III.</div>
        <div class="margin-content">
          <div class="margin-tag">Marginal note 03 · sector dominance</div>
          <h3 class="margin-title">${insights[2].title}</h3>
          <p class="margin-prose">${insights[2].interpretation}</p>
          <table class="margin-table">
            <thead><tr><th>Rank</th><th>Sector</th><th>Trips</th><th>Share</th><th>Velocity</th></tr></thead>
            <tbody>${tableRows}</tbody>
          </table>
        </div>
      </article>`);
  }
  document.getElementById('insights-container').innerHTML = sections.join('');
}