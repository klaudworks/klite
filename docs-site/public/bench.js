(function () {
  var raw = [
    {m:1,p50:7,p95:10,p99:52,avg:7.51},
    {m:2,p50:7,p95:10,p99:56,avg:7.64},
    {m:3,p50:7,p95:10,p99:64,avg:7.80},
    {m:4,p50:7,p95:10,p99:67,avg:7.84},
    {m:5,p50:7,p95:10,p99:71,avg:7.95},
    {m:6,p50:7,p95:10,p99:82,avg:8.66},
    {m:7,p50:7,p95:10,p99:90,avg:8.72},
    {m:8,p50:7,p95:10,p99:99,avg:9.07},
    {m:9,p50:7,p95:13,p99:98,avg:9.18},
    {m:10,p50:7,p95:10,p99:109,avg:9.24},
    {m:11,p50:7,p95:14,p99:120,avg:9.70},
    {m:12,p50:7,p95:13,p99:126,avg:9.91},
    {m:13,p50:7,p95:17,p99:138,avg:10.43},
    {m:14,p50:7,p95:27,p99:161,avg:11.64},
    {m:15,p50:7,p95:53,p99:179,avg:13.61},
    {m:16,p50:7,p95:62,p99:182,avg:14.08},
    {m:17,p50:7,p95:37,p99:186,avg:12.41},
    {m:18,p50:7,p95:41,p99:198,avg:12.96},
    {m:19,p50:7,p95:53,p99:215,avg:14.27},
    {m:20,p50:7,p95:47,p99:221,avg:13.95},
    {m:21,p50:7,p95:53,p99:230,avg:14.56},
    {m:22,p50:7,p95:61,p99:248,avg:15.61},
    {m:23,p50:7,p95:80,p99:271,avg:17.27},
    {m:24,p50:7,p95:78,p99:268,avg:16.98},
    {m:25,p50:7,p95:102,p99:282,avg:18.61},
    {m:26,p50:7,p95:114,p99:303,avg:19.55},
    {m:27,p50:7,p95:101,p99:297,avg:19.20},
    {m:28,p50:7,p95:136,p99:332,avg:21.37},
    {m:29,p50:7,p95:146,p99:343,avg:22.13},
    {m:30,p50:7,p95:157,p99:355,avg:23.29},
    {m:31,p50:7,p95:196,p99:374,avg:25.61},
    {m:32,p50:7,p95:192,p99:392,avg:25.64},
    {m:33,p50:7,p95:207,p99:408,avg:26.83},
    {m:34,p50:7,p95:281,p99:522,avg:33.59},
    {m:35,p50:7,p95:302,p99:503,avg:33.81},
    {m:36,p50:7,p95:297,p99:498,avg:33.93},
    {m:37,p50:7,p95:316,p99:516,avg:35.23},
    {m:38,p50:7,p95:293,p99:487,avg:33.12},
    {m:39,p50:7,p95:255,p99:460,avg:30.68},
    {m:40,p50:7,p95:272,p99:456,avg:32.10},
    {m:41,p50:7,p95:208,p99:409,avg:27.02},
    {m:42,p50:7,p95:185,p99:386,avg:25.52},
    {m:43,p50:7,p95:165,p99:360,avg:23.33},
    {m:44,p50:7,p95:156,p99:355,avg:22.89},
    {m:45,p50:7,p95:163,p99:361,avg:23.57},
    {m:46,p50:7,p95:172,p99:375,avg:24.15},
    {m:47,p50:7,p95:10,p99:296,avg:13.40},
    {m:48,p50:7,p95:16,p99:314,avg:14.90},
    {m:49,p50:7,p95:10,p99:273,avg:12.70},
    {m:50,p50:7,p95:10,p99:298,avg:14.11},
    {m:51,p50:7,p95:21,p99:331,avg:15.57},
    {m:52,p50:7,p95:16,p99:317,avg:14.26},
    {m:53,p50:7,p95:27,p99:327,avg:15.44},
    {m:54,p50:7,p95:17,p99:324,avg:14.47},
    {m:55,p50:7,p95:19,p99:348,avg:16.06},
    {m:56,p50:7,p95:32,p99:365,avg:16.86},
    {m:57,p50:7,p95:10,p99:307,avg:13.83},
    {m:58,p50:7,p95:14,p99:305,avg:14.50},
    {m:59,p50:7,p95:13,p99:328,avg:14.48}
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
          ticks: { color: '#94a3b8', maxTicksLimit: 12 },
          grid: { color: 'rgba(148,163,184,0.08)' }
        },
        y: {
          title: { display: true, text: 'Latency (ms)', color: '#94a3b8', font: { size: 12 } },
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

  function mkChart(id, data, color, label) {
    var el = document.getElementById(id);
    if (!el) return;
    var ctx = el.getContext('2d');
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
      options: mkOpts(label)
    });
  }

  mkChart('chart-p50', raw.map(function (d) { return d.p50; }), '#22c55e', 'p50');
  mkChart('chart-p95', raw.map(function (d) { return d.p95; }), '#3b82f6', 'p95');
  mkChart('chart-p99', raw.map(function (d) { return d.p99; }), '#f59e0b', 'p99');
  mkChart('chart-avg', raw.map(function (d) { return d.avg; }), '#06b6d4', 'avg');

  // Starlight tabs hide canvases — resize charts when a tab becomes visible.
  var observer = new MutationObserver(function () {
    Object.keys(charts).forEach(function (k) { charts[k].resize(); });
  });
  document.querySelectorAll('[role="tabpanel"]').forEach(function (panel) {
    observer.observe(panel, { attributes: true, attributeFilter: ['hidden'] });
  });
})();
