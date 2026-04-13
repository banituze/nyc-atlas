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