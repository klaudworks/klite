(function () {
  // 1 billion records, 16 producers, 16 consumers, 16 partitions, acks=1, no rate limit.
  // 1-minute windows. Run 20260307T073028.
  var raw = [
    {m:1, mbs:544.9, avg:26.8, p50:27, p95:37, p99:86},
    {m:2, mbs:537.6, avg:27.2, p50:27, p95:36, p99:84},
    {m:3, mbs:540.4, avg:27.0, p50:27, p95:37, p99:74},
    {m:4, mbs:540.8, avg:26.9, p50:27, p95:36, p99:83},
    {m:5, mbs:537.0, avg:27.1, p50:27, p95:36, p99:89},
    {m:6, mbs:540.0, avg:26.9, p50:26, p95:37, p99:81},
    {m:7, mbs:539.4, avg:27.1, p50:27, p95:36, p99:75},
    {m:8, mbs:534.5, avg:27.4, p50:27, p95:36, p99:98},
    {m:9, mbs:537.3, avg:27.3, p50:27, p95:36, p99:85},
    {m:10, mbs:540.5, avg:27.0, p50:27, p95:36, p99:80},
    {m:11, mbs:535.3, avg:27.2, p50:27, p95:37, p99:75},
    {m:12, mbs:543.7, avg:26.6, p50:26, p95:37, p99:96},
    {m:13, mbs:543.1, avg:26.9, p50:26, p95:36, p99:103},
    {m:14, mbs:538.3, avg:27.1, p50:27, p95:37, p99:84},
    {m:15, mbs:544.5, avg:26.6, p50:26, p95:35, p99:87},
    {m:16, mbs:543.3, avg:26.7, p50:26, p95:36, p99:78},
    {m:17, mbs:539.1, avg:27.2, p50:27, p95:36, p99:89},
    {m:18, mbs:538.6, avg:27.1, p50:27, p95:37, p99:78},
    {m:19, mbs:546.4, avg:26.6, p50:26, p95:37, p99:95},
    {m:20, mbs:537.7, avg:27.2, p50:27, p95:37, p99:97},
    {m:21, mbs:540.2, avg:27.0, p50:26, p95:36, p99:86},
    {m:22, mbs:536.5, avg:27.2, p50:27, p95:37, p99:74},
    {m:23, mbs:541.9, avg:26.9, p50:27, p95:36, p99:78},
    {m:24, mbs:541.0, avg:27.1, p50:27, p95:36, p99:88},
    {m:25, mbs:538.0, avg:27.2, p50:27, p95:36, p99:80},
    {m:26, mbs:539.6, avg:27.2, p50:26, p95:37, p99:87},
    {m:27, mbs:536.9, avg:27.1, p50:27, p95:36, p99:87},
    {m:28, mbs:538.2, avg:27.2, p50:27, p95:36, p99:96},
    {m:29, mbs:540.6, avg:26.9, p50:26, p95:36, p99:90},
    {m:30, mbs:542.7, avg:26.8, p50:26, p95:37, p99:82},
    {m:31, mbs:106.1, avg:27.0, p50:25, p95:31, p99:73}
  ];

  // System metrics from sar (5s samples averaged per minute, 31 minutes).
  // Minute 31 is partial (44s) -- producers finishing up.
  // Memory in absolute GB (includes process memory + page cache + buffers). 62 GB total.
  var sar = [
    {m:1,diskW:340.2,cpuUsr:5.5,cpuSys:7.4,cpuIo:2.8,memGB:12.8,netRx:340.7,netTx:656.4},
    {m:2,diskW:363.8,cpuUsr:6.1,cpuSys:8.0,cpuIo:3.1,memGB:13.9,netRx:364.5,netTx:732.3},
    {m:3,diskW:365.8,cpuUsr:5.9,cpuSys:7.8,cpuIo:3.1,memGB:14.7,netRx:366.5,netTx:733.7},
    {m:4,diskW:363.5,cpuUsr:5.8,cpuSys:8.2,cpuIo:3.1,memGB:15.1,netRx:364.3,netTx:733.6},
    {m:5,diskW:364.4,cpuUsr:6.0,cpuSys:8.0,cpuIo:3.1,memGB:14.7,netRx:365.0,netTx:732.8},
    {m:6,diskW:363.4,cpuUsr:5.6,cpuSys:7.9,cpuIo:3.1,memGB:15.3,netRx:364.1,netTx:729.6},
    {m:7,diskW:364.0,cpuUsr:6.0,cpuSys:8.1,cpuIo:3.1,memGB:14.9,netRx:364.7,netTx:742.0},
    {m:8,diskW:362.0,cpuUsr:6.0,cpuSys:8.3,cpuIo:3.1,memGB:14.4,netRx:362.8,netTx:721.0},
    {m:9,diskW:363.7,cpuUsr:6.2,cpuSys:7.9,cpuIo:3.1,memGB:14.3,netRx:364.3,netTx:733.6},
    {m:10,diskW:366.7,cpuUsr:6.1,cpuSys:7.9,cpuIo:3.1,memGB:14.2,netRx:367.5,netTx:735.5},
    {m:11,diskW:359.9,cpuUsr:6.2,cpuSys:8.5,cpuIo:3.0,memGB:14.6,netRx:360.5,netTx:736.6},
    {m:12,diskW:367.1,cpuUsr:5.7,cpuSys:7.9,cpuIo:3.1,memGB:14.6,netRx:367.9,netTx:727.1},
    {m:13,diskW:366.7,cpuUsr:6.2,cpuSys:8.2,cpuIo:3.1,memGB:13.8,netRx:367.5,netTx:739.3},
    {m:14,diskW:364.8,cpuUsr:6.2,cpuSys:7.6,cpuIo:3.1,memGB:13.7,netRx:365.5,netTx:732.2},
    {m:15,diskW:365.1,cpuUsr:5.8,cpuSys:7.8,cpuIo:3.1,memGB:14.5,netRx:366.0,netTx:736.4},
    {m:16,diskW:367.1,cpuUsr:6.1,cpuSys:7.9,cpuIo:3.1,memGB:14.4,netRx:367.9,netTx:739.9},
    {m:17,diskW:364.4,cpuUsr:5.8,cpuSys:8.1,cpuIo:3.1,memGB:14.3,netRx:365.1,netTx:735.8},
    {m:18,diskW:365.2,cpuUsr:5.7,cpuSys:8.2,cpuIo:3.1,memGB:14.0,netRx:365.9,netTx:733.1},
    {m:19,diskW:368.3,cpuUsr:5.8,cpuSys:7.7,cpuIo:3.1,memGB:14.5,netRx:368.9,netTx:738.8},
    {m:20,diskW:364.2,cpuUsr:6.0,cpuSys:8.0,cpuIo:3.1,memGB:14.4,netRx:364.9,netTx:732.7},
    {m:21,diskW:364.3,cpuUsr:5.9,cpuSys:8.0,cpuIo:3.1,memGB:14.7,netRx:364.9,netTx:733.4},
    {m:22,diskW:363.0,cpuUsr:6.0,cpuSys:7.9,cpuIo:3.1,memGB:14.6,netRx:363.6,netTx:728.9},
    {m:23,diskW:366.3,cpuUsr:6.3,cpuSys:7.9,cpuIo:3.1,memGB:14.8,netRx:367.0,netTx:735.8},
    {m:24,diskW:365.1,cpuUsr:5.8,cpuSys:7.9,cpuIo:3.1,memGB:14.7,netRx:365.9,netTx:735.1},
    {m:25,diskW:365.3,cpuUsr:5.8,cpuSys:7.5,cpuIo:3.1,memGB:13.9,netRx:365.9,netTx:733.2},
    {m:26,diskW:364.2,cpuUsr:5.9,cpuSys:8.0,cpuIo:3.1,memGB:15.3,netRx:364.9,netTx:731.9},
    {m:27,diskW:363.7,cpuUsr:5.9,cpuSys:8.2,cpuIo:3.1,memGB:14.4,netRx:364.5,netTx:733.0},
    {m:28,diskW:364.2,cpuUsr:6.1,cpuSys:8.5,cpuIo:3.0,memGB:14.7,netRx:364.9,netTx:734.2},
    {m:29,diskW:364.8,cpuUsr:5.7,cpuSys:8.0,cpuIo:3.1,memGB:15.5,netRx:365.6,netTx:735.3},
    {m:30,diskW:366.1,cpuUsr:5.9,cpuSys:8.0,cpuIo:3.1,memGB:14.4,netRx:366.9,netTx:737.1},
    {m:31,diskW:81.9,cpuUsr:1.4,cpuSys:1.9,cpuIo:1.0,memGB:14.6,netRx:81.6,netTx:189.9}
  ];

  var benchLabels = raw.map(function (d) { return d.m; });
  var sarLabels = sar.map(function (d) { return d.m; });
  var charts = {};

  var tooltipStyle = {
    backgroundColor: 'rgba(15,23,42,0.9)',
    titleColor: '#e2e8f0',
    bodyColor: '#e2e8f0',
    borderColor: 'rgba(148,163,184,0.2)',
    borderWidth: 1,
    padding: 10,
    cornerRadius: 6
  };

  var xAxis = function (labels) {
    return {
      title: { display: true, text: 'Elapsed (min)', color: '#94a3b8', font: { size: 12 } },
      ticks: {
        color: '#94a3b8',
        callback: function (val) {
          var l = this.getLabelForValue(val);
          return l % 5 === 0 ? l : '';
        },
        autoSkip: false,
        maxRotation: 0
      },
      grid: { color: 'rgba(148,163,184,0.08)' }
    };
  };

  var lineStyle = { point: { radius: 2, hoverRadius: 5, borderWidth: 0 }, line: { tension: 0.35, borderWidth: 2.5 } };

  function gradientFill(ctx, color) {
    var g = ctx.createLinearGradient(0, 0, 0, 320);
    g.addColorStop(0, color + '30');
    g.addColorStop(1, color + '02');
    return g;
  }

  function mkChart(id, labels, data, color, dsLabel, yLabel, unit, note) {
    var el = document.getElementById(id);
    if (!el) return;
    var ctx = el.getContext('2d');
    var opts = {
      responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
      plugins: {
        legend: { display: false },
        tooltip: Object.assign({}, tooltipStyle, {
          callbacks: {
            title: function (items) { return 'Minute ' + items[0].label; },
            label: function (item) { return dsLabel + ': ' + item.parsed.y + ' ' + unit; }
          }
        })
      },
      scales: { x: xAxis(labels), y: {
        title: { display: true, text: yLabel + ' (' + unit + ')', color: '#94a3b8', font: { size: 12 } },
        ticks: { color: '#94a3b8' }, grid: { color: 'rgba(148,163,184,0.12)' }, beginAtZero: true
      }},
      elements: lineStyle
    };
    if (note) {
      opts.plugins.subtitle = { display: true, text: note, color: '#64748b', font: { size: 11, style: 'italic' }, padding: { bottom: 10 } };
    }
    charts[id] = new Chart(ctx, {
      type: 'line',
      data: { labels: labels, datasets: [{ label: dsLabel, data: data, borderColor: color, backgroundColor: gradientFill(ctx, color), fill: true, pointBackgroundColor: color }] },
      options: opts
    });
  }

  function mkMultiChart(id, labels, datasets, yLabel, unit, note, stacked) {
    var el = document.getElementById(id);
    if (!el) return;
    var ctx = el.getContext('2d');
    var ds = datasets.map(function (d) {
      return {
        label: d.label, data: d.data, borderColor: d.color, backgroundColor: gradientFill(ctx, d.color),
        fill: stacked ? 'origin' : false, pointBackgroundColor: d.color, pointRadius: 2, pointHoverRadius: 5,
        pointBorderWidth: 0, tension: 0.35, borderWidth: 2.5
      };
    });
    var opts = {
      responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
      plugins: {
        legend: { display: true, labels: { color: '#94a3b8', boxWidth: 12, padding: 16 } },
        tooltip: Object.assign({}, tooltipStyle, {
          callbacks: {
            title: function (items) { return 'Minute ' + items[0].label; },
            label: function (item) { return item.dataset.label + ': ' + item.parsed.y + ' ' + unit; }
          }
        })
      },
      scales: { x: xAxis(labels), y: {
        title: { display: true, text: yLabel + ' (' + unit + ')', color: '#94a3b8', font: { size: 12 } },
        ticks: { color: '#94a3b8' }, grid: { color: 'rgba(148,163,184,0.12)' },
        stacked: !!stacked, beginAtZero: true
      }},
      elements: lineStyle
    };
    if (note) {
      opts.plugins.subtitle = { display: true, text: note, color: '#64748b', font: { size: 11, style: 'italic' }, padding: { bottom: 10 } };
    }
    charts[id] = new Chart(ctx, { type: 'line', data: { labels: labels, datasets: ds }, options: opts });
  }

  // Benchmark charts
  mkChart('stress-throughput', benchLabels, raw.map(function (d) { return d.mbs; }), '#8b5cf6', 'throughput', 'Throughput', 'MB/s',
    'Application-layer throughput (1 KB records, acks=1). Snappy compresses to ~68% on the wire.');
  mkChart('stress-avg', benchLabels, raw.map(function (d) { return d.avg; }), '#14b8a6', 'avg', 'Produce latency', 'ms',
    'Average produce acknowledgement latency per window.');
  mkChart('stress-p50', benchLabels, raw.map(function (d) { return d.p50; }), '#22c55e', 'p50', 'End-to-end latency', 'ms',
    'End-to-end latency: full round-trip from produce through consume.');
  mkChart('stress-p95', benchLabels, raw.map(function (d) { return d.p95; }), '#3b82f6', 'p95', 'End-to-end latency', 'ms',
    'End-to-end latency: full round-trip from produce through consume.');
  mkChart('stress-p99', benchLabels, raw.map(function (d) { return d.p99; }), '#f59e0b', 'p99', 'End-to-end latency', 'ms',
    'End-to-end latency: full round-trip from produce through consume.');

  // System metrics charts
  mkChart('stress-disk', sarLabels, sar.map(function (d) { return d.diskW; }), '#ec4899', 'disk write', 'Disk write throughput', 'MB/s',
    'Actual bytes written to EBS. Instance limit: 625 MB/s sustained, 1250 MB/s burst.');
  mkMultiChart('stress-cpu', sarLabels, [
    { label: 'user', data: sar.map(function (d) { return d.cpuUsr; }), color: '#06b6d4' },
    { label: 'system', data: sar.map(function (d) { return d.cpuSys; }), color: '#f59e0b' },
    { label: 'iowait', data: sar.map(function (d) { return d.cpuIo; }), color: '#ef4444' }
  ], 'CPU', '%', '16 vCPUs total. ~83% idle at max throughput.', true);
  mkMultiChart('stress-net', sarLabels, [
    { label: 'receive (from clients)', data: sar.map(function (d) { return d.netRx; }), color: '#22c55e' },
    { label: 'transmit (to clients + S3)', data: sar.map(function (d) { return d.netTx; }), color: '#3b82f6' }
  ], 'Network', 'MB/s',     'Instance baseline: ~938 MB/s (7.5 Gbps), burst up to 12.5 Gbps.', false);
  mkChart('stress-mem', sarLabels, sar.map(function (d) { return d.memGB; }), '#a855f7', 'memory', 'Memory used', 'GB',
    '64 GB total. Includes page cache and kernel buffers.');

  // Starlight tabs hide canvases -- resize charts when a tab becomes visible.
  var observer = new MutationObserver(function () {
    Object.keys(charts).forEach(function (k) { charts[k].resize(); });
  });
  document.querySelectorAll('[role="tabpanel"]').forEach(function (panel) {
    observer.observe(panel, { attributes: true, attributeFilter: ['hidden'] });
  });
})();
