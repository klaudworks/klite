(function () {
  var raw = [
    {m:1,avg:11.1,p50:11,p95:15,p99:37},
    {m:2,avg:11.2,p50:11,p95:15,p99:37},
    {m:3,avg:10.9,p50:11,p95:15,p99:28},
    {m:4,avg:11.0,p50:11,p95:15,p99:36},
    {m:5,avg:11.2,p50:11,p95:15,p99:35},
    {m:6,avg:11.2,p50:11,p95:15,p99:35},
    {m:7,avg:11.1,p50:11,p95:15,p99:39},
    {m:8,avg:11.2,p50:11,p95:15,p99:40},
    {m:9,avg:11.0,p50:11,p95:15,p99:46},
    {m:10,avg:11.1,p50:11,p95:15,p99:28},
    {m:11,avg:11.1,p50:11,p95:15,p99:31},
    {m:12,avg:11.1,p50:11,p95:15,p99:33},
    {m:13,avg:11.1,p50:11,p95:15,p99:31},
    {m:14,avg:11.1,p50:11,p95:15,p99:30},
    {m:15,avg:11.3,p50:11,p95:15,p99:39},
    {m:16,avg:11.2,p50:11,p95:15,p99:39},
    {m:17,avg:11.2,p50:11,p95:15,p99:42},
    {m:18,avg:11.1,p50:11,p95:15,p99:26},
    {m:19,avg:11.3,p50:11,p95:15,p99:35},
    {m:20,avg:11.2,p50:11,p95:15,p99:35},
    {m:21,avg:11.2,p50:11,p95:15,p99:37},
    {m:22,avg:11.1,p50:11,p95:15,p99:37},
    {m:23,avg:11.2,p50:11,p95:15,p99:33},
    {m:24,avg:11.3,p50:11,p95:15,p99:38},
    {m:25,avg:11.1,p50:11,p95:15,p99:27},
    {m:26,avg:11.0,p50:11,p95:15,p99:34},
    {m:27,avg:11.0,p50:11,p95:15,p99:27},
    {m:28,avg:11.0,p50:11,p95:15,p99:26},
    {m:29,avg:11.0,p50:11,p95:15,p99:29},
    {m:30,avg:11.2,p50:11,p95:15,p99:34},
    {m:31,avg:11.1,p50:11,p95:15,p99:27},
    {m:32,avg:11.2,p50:11,p95:15,p99:35},
    {m:33,avg:11.4,p50:11,p95:15,p99:37},
    {m:34,avg:11.2,p50:11,p95:15,p99:42},
    {m:35,avg:11.0,p50:11,p95:15,p99:26},
    {m:36,avg:11.2,p50:11,p95:15,p99:36},
    {m:37,avg:11.2,p50:11,p95:15,p99:37},
    {m:38,avg:11.1,p50:11,p95:15,p99:32},
    {m:39,avg:11.3,p50:11,p95:15,p99:41},
    {m:40,avg:11.1,p50:11,p95:15,p99:34},
    {m:41,avg:11.3,p50:11,p95:15,p99:33},
    {m:42,avg:11.2,p50:11,p95:15,p99:43},
    {m:43,avg:11.2,p50:11,p95:15,p99:34},
    {m:44,avg:11.0,p50:11,p95:15,p99:30},
    {m:45,avg:11.3,p50:11,p95:15,p99:34},
    {m:46,avg:11.1,p50:11,p95:15,p99:28},
    {m:47,avg:11.1,p50:11,p95:15,p99:34},
    {m:48,avg:11.0,p50:11,p95:15,p99:34},
    {m:49,avg:11.1,p50:11,p95:15,p99:29},
    {m:50,avg:11.2,p50:11,p95:15,p99:31},
    {m:51,avg:11.3,p50:11,p95:15,p99:36},
    {m:52,avg:11.0,p50:11,p95:15,p99:29},
    {m:53,avg:11.1,p50:11,p95:15,p99:29},
    {m:54,avg:11.1,p50:11,p95:15,p99:33},
    {m:55,avg:11.2,p50:11,p95:15,p99:39},
    {m:56,avg:11.2,p50:11,p95:15,p99:36},
    {m:57,avg:11.2,p50:11,p95:15,p99:36},
    {m:58,avg:11.1,p50:11,p95:15,p99:34},
    {m:59,avg:11.2,p50:11,p95:15,p99:34},
    {m:60,avg:11.6,p50:11,p95:15,p99:38}
  ];

  var labels = raw.map(function (d) { return d.m; });
  var charts = {};

  function mkOpts(label) {
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 400 },
      plugins: {
        legend: { display: false },
        subtitle: {
          display: true,
          text: 'End-to-end latency: full round-trip from produce through consume.',
          color: '#64748b',
          font: { size: 11, style: 'italic' },
          padding: { bottom: 10 }
        },
        tooltip: {
          backgroundColor: 'rgba(15,23,42,0.9)',
          titleColor: '#e2e8f0',
          bodyColor: '#e2e8f0',
          borderColor: 'rgba(148,163,184,0.2)',
          borderWidth: 1,
          padding: 10,
          cornerRadius: 6,
          callbacks: {
            title: function (items) { return 'Minute ' + items[0].label; },
            label: function (item) { return label + ': ' + item.parsed.y + ' ms'; }
          }
        }
      },
      scales: {
        x: {
          title: { display: true, text: 'Elapsed (min)', color: '#94a3b8', font: { size: 12 } },
          ticks: {
            color: '#94a3b8',
            callback: function (val, index) {
              var label = this.getLabelForValue(val);
              return label % 5 === 0 ? label : '';
            },
            autoSkip: false,
            maxRotation: 0
          },
          grid: { color: 'rgba(148,163,184,0.08)' }
        },
        y: {
          title: { display: true, text: 'End-to-end latency (ms)', color: '#94a3b8', font: { size: 12 } },
          ticks: { color: '#94a3b8' },
          grid: { color: 'rgba(148,163,184,0.12)' },
          beginAtZero: true
        }
      },
      elements: {
        point: { radius: 2, hoverRadius: 5, borderWidth: 0 },
        line: { tension: 0.35, borderWidth: 2.5 }
      }
    };
  }

  function gradientFill(ctx, color) {
    var g = ctx.createLinearGradient(0, 0, 0, 320);
    g.addColorStop(0, color + '30');
    g.addColorStop(1, color + '02');
    return g;
  }

  function mkChart(id, data, color, label, note) {
    var el = document.getElementById(id);
    if (!el) return;
    var ctx = el.getContext('2d');
    var opts = mkOpts(label);
    if (note) {
      opts.plugins.title = {
        display: true,
        text: note,
        color: '#64748b',
        font: { size: 11, style: 'italic', weight: 'normal' },
        padding: { bottom: 2 }
      };
    }
    charts[id] = new Chart(ctx, {
      type: 'line',
      data: {
        labels: labels,
        datasets: [{
          label: label,
          data: data,
          borderColor: color,
          backgroundColor: gradientFill(ctx, color),
          fill: true,
          pointBackgroundColor: color
        }]
      },
      options: opts
    });
  }

  var flat = 'Yes, these are the real numbers. Only p99 fluctuates noticeably. p50 and p95 are rock solid.';
  mkChart('chart-avg', raw.map(function (d) { return d.avg; }), '#06b6d4', 'avg');
  mkChart('chart-p50', raw.map(function (d) { return d.p50; }), '#22c55e', 'p50', flat);
  mkChart('chart-p95', raw.map(function (d) { return d.p95; }), '#3b82f6', 'p95', flat);
  mkChart('chart-p99', raw.map(function (d) { return d.p99; }), '#f59e0b', 'p99');

  // Starlight tabs hide canvases — resize charts when a tab becomes visible.
  var observer = new MutationObserver(function () {
    Object.keys(charts).forEach(function (k) { charts[k].resize(); });
  });
  document.querySelectorAll('[role="tabpanel"]').forEach(function (panel) {
    observer.observe(panel, { attributes: true, attributeFilter: ['hidden'] });
  });
})();
