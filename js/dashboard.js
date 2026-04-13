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