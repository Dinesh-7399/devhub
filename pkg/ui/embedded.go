package ui

// DashboardHTML contains the complete single-page application for the DevHub Developer Cockpit.
const DashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>DevHub Cockpit | Microservice Switchboard & Debugger</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    body { background-color: #070a12; color: #cbd5e1; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
    .glass-card { background: rgba(13, 20, 36, 0.85); backdrop-filter: blur(12px); border: 1px solid rgba(30, 41, 59, 0.8); }
    .badge-2xx { background: #064e3b; color: #34d399; border: 1px solid #059669; }
    .badge-3xx { background: #1e3a8a; color: #60a5fa; border: 1px solid #2563eb; }
    .badge-4xx { background: #78350f; color: #fbbf24; border: 1px solid #d97706; }
    .badge-5xx { background: #7f1d1d; color: #f87171; border: 1px solid #dc2626; }
    .tab-active { border-bottom: 2px solid #38bdf8; color: #38bdf8; text-shadow: 0 0 10px rgba(56, 189, 248, 0.5); }
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #0b0f19; }
    ::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
    .btn-action { transition: all 0.15s ease-in-out; }
    .btn-action:hover { transform: translateY(-1px); }
  </style>
</head>
<body class="p-3 md:p-5 flex flex-col h-screen overflow-hidden">
  <!-- Top Navigation Header -->
  <header class="flex flex-wrap items-center justify-between border-b border-slate-800/80 pb-3 mb-2.5 flex-shrink-0">
    <div class="flex items-center space-x-3">
      <div class="h-9 w-9 rounded-lg bg-gradient-to-br from-cyan-500 to-indigo-600 flex items-center justify-center shadow-lg shadow-cyan-500/20">
        <span class="text-xl text-white">⚡</span>
      </div>
      <div>
        <div class="flex items-center space-x-2">
          <h1 class="text-lg font-bold bg-gradient-to-r from-cyan-400 via-sky-300 to-indigo-400 bg-clip-text text-transparent tracking-tight">DEVHUB COCKPIT</h1>
          <span class="text-[10px] px-2 py-0.5 rounded-full bg-cyan-950/80 text-cyan-300 border border-cyan-700/60 font-semibold shadow-inner">v2.5 PRO</span>
        </div>
        <p class="text-[10px] text-slate-500">Zero-Alloc L7 Ingress • Mesh Egress Cascades • Webhook HMAC Re-Signer • Mock Mode Fallback</p>
      </div>
    </div>

    <!-- Live Status & Stats -->
    <div class="flex items-center space-x-2.5 mt-2 md:mt-0">
      <div id="ws-status" class="flex items-center space-x-2 text-xs text-yellow-400 bg-slate-900/90 px-3 py-1.5 rounded-lg border border-slate-800 shadow-inner">
        <span class="h-2 w-2 rounded-full bg-yellow-400 animate-pulse"></span>
        <span class="font-medium">Connecting...</span>
      </div>
      <div class="text-xs bg-slate-900/90 px-3 py-1.5 rounded-lg border border-slate-800 text-slate-400">
        <span id="event-count" class="font-bold text-cyan-400">0</span> traces captured
      </div>
    </div>
  </header>

  <!-- Upstream Service Live Health Heartbeat Bar -->
  <div class="bg-slate-950/90 border border-slate-800/80 rounded-lg p-2 mb-2.5 flex items-center space-x-2 overflow-x-auto text-[11px] flex-shrink-0 shadow-inner">
    <span class="text-slate-500 font-bold uppercase text-[10px] flex items-center space-x-1 pl-1 pr-2 border-r border-slate-800 flex-shrink-0">
      <i class="fa-solid fa-heart-pulse text-rose-500 animate-pulse"></i>
      <span>Mesh Health</span>
    </span>
    <div id="health-pills" class="flex items-center space-x-2 flex-nowrap">
      <span class="text-slate-600">Probing mesh upstreams...</span>
    </div>
  </div>

  <!-- Navigation Tabs -->
  <div class="flex items-center space-x-6 border-b border-slate-800/80 mb-2.5 text-xs font-semibold uppercase tracking-wider flex-shrink-0">
    <button onclick="switchTab('traces')" id="tab-btn-traces" class="pb-2 tab-active flex items-center space-x-2 transition">
      <i class="fa-solid fa-satellite-dish"></i>
      <span>Traffic Switchboard</span>
    </button>
    <button onclick="switchTab('replay')" id="tab-btn-replay" class="pb-2 text-slate-400 hover:text-slate-200 flex items-center space-x-2 transition">
      <i class="fa-solid fa-bolt text-yellow-400"></i>
      <span>1-Click Replay & Webhook Signer</span>
    </button>
    <button onclick="switchTab('mocks')" id="tab-btn-mocks" class="pb-2 text-slate-400 hover:text-slate-200 flex items-center space-x-2 transition">
      <i class="fa-solid fa-masks-theater text-emerald-400"></i>
      <span>Mock Mode Fallback</span>
    </button>
    <button onclick="switchTab('logs')" id="tab-btn-logs" class="pb-2 text-slate-400 hover:text-slate-200 flex items-center space-x-2 transition">
      <i class="fa-brands fa-docker text-sky-400"></i>
      <span>Docker Container Logs</span>
    </button>
    <button onclick="switchTab('waterfall')" id="tab-btn-waterfall" class="pb-2 text-slate-400 hover:text-slate-200 flex items-center space-x-2 transition">
      <i class="fa-solid fa-chart-gantt text-purple-400"></i>
      <span>Waterfall X-Ray</span>
    </button>
  </div>

  <!-- Main View Container -->
  <main class="flex-1 overflow-hidden">
    <!-- VIEW 1: TRACES SWITCHBOARD -->
    <div id="view-traces" class="h-full grid grid-cols-1 lg:grid-cols-3 gap-3">
      <!-- Left Column: Search Filter & List -->
      <div class="lg:col-span-1 glass-card rounded-xl p-3 flex flex-col h-full overflow-hidden shadow-2xl">
        <!-- Filter Controls -->
        <div class="space-y-2 mb-2.5 flex-shrink-0">
          <div class="relative">
            <input id="filter-text" oninput="applyFilters()" type="text" placeholder="Filter by path, method, status, body..."
              class="w-full bg-slate-950/90 border border-slate-800 rounded-lg px-3 py-1.5 text-xs text-slate-200 placeholder-slate-600 focus:outline-none focus:border-cyan-500 transition pl-8 shadow-inner">
            <i class="fa-solid fa-magnifying-glass absolute left-2.5 top-2.5 text-slate-600 text-xs"></i>
          </div>
          <div class="flex items-center justify-between text-[11px] text-slate-400">
            <div class="flex items-center space-x-1">
              <button onclick="setStatusFilter('all')" class="px-2 py-0.5 rounded bg-slate-800 text-slate-200 font-bold" id="filter-btn-all">All</button>
              <button onclick="setStatusFilter('2xx')" class="px-2 py-0.5 rounded hover:bg-slate-800" id="filter-btn-2xx">2xx</button>
              <button onclick="setStatusFilter('4xx')" class="px-2 py-0.5 rounded hover:bg-slate-800" id="filter-btn-4xx">4xx</button>
              <button onclick="setStatusFilter('5xx')" class="px-2 py-0.5 rounded hover:bg-slate-800" id="filter-btn-5xx">5xx</button>
            </div>
            <div class="flex items-center space-x-2">
              <button onclick="togglePause()" id="pause-btn" class="hover:text-cyan-400 btn-action" title="Pause Live Stream"><i class="fa-solid fa-pause"></i></button>
              <button onclick="clearTraces()" class="hover:text-rose-400 btn-action" title="Clear Trace Buffer"><i class="fa-solid fa-trash"></i></button>
            </div>
          </div>
        </div>
        <!-- Scrollable Trace Item List -->
        <div id="trace-list" class="flex-1 overflow-y-auto space-y-1.5 pr-1"></div>
      </div>

      <!-- Right Column: Detailed Trace Inspector -->
      <div class="lg:col-span-2 glass-card rounded-xl p-4 flex flex-col h-full overflow-hidden shadow-2xl">
        <div id="trace-detail-header" class="flex items-center justify-between border-b border-slate-800 pb-2.5 mb-2.5 flex-shrink-0">
          <h2 class="text-xs font-bold uppercase tracking-wider text-slate-400 flex items-center space-x-2">
            <i class="fa-solid fa-magnifying-glass-chart text-cyan-400"></i>
            <span>Inspection Details</span>
          </h2>
          <div id="trace-actions" class="hidden flex items-center space-x-2">
            <button onclick="copyAsCurl()" class="text-xs bg-slate-800/90 hover:bg-slate-700 text-cyan-300 px-2.5 py-1 rounded border border-slate-700 btn-action">
              <i class="fa-solid fa-terminal mr-1"></i> Copy cURL
            </button>
            <button onclick="openInReplay()" class="text-xs bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 text-white px-3 py-1 rounded font-bold shadow-md btn-action">
              <i class="fa-solid fa-bolt mr-1"></i> Replay
            </button>
          </div>
        </div>
        <div id="trace-detail-content" class="flex-1 overflow-y-auto text-xs text-slate-400 flex items-center justify-center border border-dashed border-slate-800/80 rounded-lg p-4">
          Select any incoming request from the live switchboard to inspect headers, JSON payloads, and timings
        </div>
      </div>
    </div>

    <!-- VIEW 2: 1-CLICK REPLAY & WEBHOOK SIGNER -->
    <div id="view-replay" class="hidden h-full grid grid-cols-1 lg:grid-cols-2 gap-3">
      <div class="glass-card rounded-xl p-4 flex flex-col h-full overflow-y-auto shadow-2xl space-y-3">
        <div class="flex items-center justify-between border-b border-slate-800 pb-2">
          <h2 class="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center space-x-2">
            <i class="fa-solid fa-pen-to-square text-cyan-400"></i>
            <span>Request Composer</span>
          </h2>
          <button onclick="sendReplayRequest()" class="bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 text-white text-xs px-4 py-1.5 rounded-lg font-bold shadow-lg shadow-cyan-500/20 btn-action flex items-center space-x-1.5">
            <i class="fa-solid fa-play"></i>
            <span>⚡ Send Replay</span>
          </button>
        </div>

        <!-- Quick Presets -->
        <div class="flex items-center space-x-1.5 overflow-x-auto pb-1 text-[10px]">
          <span class="text-slate-500 font-bold uppercase">Presets:</span>
          <button onclick="loadPreset('stripe')" class="px-2 py-0.5 rounded bg-slate-800/90 text-cyan-300 hover:bg-slate-700">Stripe Webhook</button>
          <button onclick="loadPreset('github')" class="px-2 py-0.5 rounded bg-slate-800/90 text-purple-300 hover:bg-slate-700">GitHub Push</button>
          <button onclick="loadPreset('razorpay')" class="px-2 py-0.5 rounded bg-slate-800/90 text-emerald-300 hover:bg-slate-700">Razorpay</button>
          <button onclick="loadPreset('auth_login')" class="px-2 py-0.5 rounded bg-slate-800/90 text-amber-300 hover:bg-slate-700">Auth Login</button>
        </div>

        <div class="flex space-x-2">
          <select id="replay-method" class="bg-slate-950 border border-slate-800 rounded-lg px-3 py-1.5 text-xs text-cyan-400 font-bold focus:outline-none focus:border-cyan-500 shadow-inner">
            <option value="GET">GET</option>
            <option value="POST">POST</option>
            <option value="PUT">PUT</option>
            <option value="DELETE">DELETE</option>
            <option value="PATCH">PATCH</option>
          </select>
          <input id="replay-url" type="text" placeholder="/api/v1/resource" class="flex-1 bg-slate-950 border border-slate-800 rounded-lg px-3 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-cyan-500 shadow-inner font-mono">
        </div>

        <!-- Webhook Re-Signing Tool -->
        <div class="bg-slate-950/80 p-2.5 rounded-lg border border-slate-800 space-y-2 shadow-inner">
          <div class="flex items-center justify-between text-[11px]">
            <span class="text-yellow-400 font-bold flex items-center space-x-1">
              <i class="fa-solid fa-key"></i>
              <span>Webhook Signature Re-Signer</span>
            </span>
            <label class="flex items-center space-x-1.5 text-slate-400 cursor-pointer">
              <input type="checkbox" id="replay-bypass-sig" class="rounded bg-slate-900 border-slate-700 text-cyan-500 focus:ring-0">
              <span>Bypass / Strip Signatures</span>
            </label>
          </div>
          <div class="grid grid-cols-2 gap-2">
            <div>
              <label class="block text-[9px] text-slate-500 uppercase mb-0.5">Provider</label>
              <select id="replay-webhook-provider" class="w-full bg-slate-900 border border-slate-800 rounded px-2 py-1 text-[11px] text-slate-200 focus:outline-none focus:border-cyan-500">
                <option value="">None (Standard HTTP)</option>
                <option value="stripe">Stripe (Stripe-Signature)</option>
                <option value="github">GitHub (X-Hub-Signature-256)</option>
                <option value="razorpay">Razorpay (X-Razorpay-Signature)</option>
                <option value="shopify">Shopify (X-Shopify-Hmac-Sha256)</option>
                <option value="slack">Slack (X-Slack-Signature)</option>
              </select>
            </div>
            <div>
              <label class="block text-[9px] text-slate-500 uppercase mb-0.5">Signing Secret</label>
              <input id="replay-webhook-secret" type="password" placeholder="whsec_... or secret" class="w-full bg-slate-900 border border-slate-800 rounded px-2 py-1 text-[11px] text-slate-200 focus:outline-none focus:border-cyan-500">
            </div>
          </div>
        </div>

        <div>
          <label class="block text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1">Custom Request Headers (JSON)</label>
          <textarea id="replay-headers" rows="3" class="w-full bg-slate-950 border border-slate-800 rounded-lg p-2 text-[11px] text-slate-300 font-mono focus:outline-none focus:border-cyan-500 shadow-inner">{\n  "Content-Type": "application/json"\n}</textarea>
        </div>

        <div class="flex-1 flex flex-col">
          <label class="block text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1">Request Body (JSON / Raw)</label>
          <textarea id="replay-body" class="w-full flex-1 min-h-[120px] bg-slate-950 border border-slate-800 rounded-lg p-2 text-[11px] text-emerald-400 font-mono focus:outline-none focus:border-cyan-500 shadow-inner">{\n  "item": "book",\n  "quantity": 1\n}</textarea>
        </div>
      </div>

      <!-- Replay Response Viewer -->
      <div class="glass-card rounded-xl p-4 flex flex-col h-full overflow-hidden shadow-2xl">
        <div class="flex items-center justify-between border-b border-slate-800 pb-2 mb-3">
          <h2 class="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center space-x-2">
            <i class="fa-solid fa-reply text-purple-400"></i>
            <span>Replay Execution Response</span>
          </h2>
          <div id="replay-status-badge" class="text-xs"></div>
        </div>
        <div id="replay-response-content" class="flex-1 overflow-y-auto text-xs text-slate-400 flex items-center justify-center border border-dashed border-slate-800/80 rounded-lg p-4 font-mono">
          Execute a replay to view instant upstream status and response payload
        </div>
      </div>
    </div>

    <!-- VIEW 3: MOCK MODE FALLBACK MANAGER -->
    <div id="view-mocks" class="hidden h-full glass-card rounded-xl p-4 flex flex-col shadow-2xl">
      <div class="flex items-center justify-between border-b border-slate-800 pb-2 mb-3">
        <div>
          <h2 class="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center space-x-2">
            <i class="fa-solid fa-masks-theater text-emerald-400"></i>
            <span>Mock Mode & Offline Fallback Interceptor</span>
          </h2>
          <p class="text-[11px] text-slate-500">Unblock frontend developers by returning instant 200 OK mocks when microservices are broken or offline.</p>
        </div>
        <button onclick="saveMockRule()" class="bg-emerald-600 hover:bg-emerald-500 text-white text-xs px-3.5 py-1.5 rounded-lg font-bold transition shadow-md shadow-emerald-600/20 btn-action">
          <i class="fa-solid fa-plus mr-1"></i> Add Mock Rule
        </button>
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-3 gap-3 flex-1 overflow-hidden">
        <!-- Mock Rules List -->
        <div class="lg:col-span-1 bg-slate-950/80 p-3 rounded-lg border border-slate-800 overflow-y-auto space-y-2 shadow-inner" id="mock-rules-list">
          <span class="text-slate-500 text-xs">Loading active mock rules...</span>
        </div>

        <!-- Mock Rule Form -->
        <div class="lg:col-span-2 bg-slate-950/80 p-3 rounded-lg border border-slate-800 flex flex-col space-y-3 shadow-inner">
          <h3 class="text-xs font-bold text-slate-300 uppercase">Mock Rule Editor</h3>
          <div class="grid grid-cols-2 gap-2">
            <div>
              <label class="block text-[10px] text-slate-400 uppercase mb-1">Route Prefix</label>
              <input id="mock-prefix" type="text" placeholder="/api/payment" class="w-full bg-slate-900 border border-slate-800 rounded px-2.5 py-1.5 text-xs text-slate-200 font-mono">
            </div>
            <div>
              <label class="block text-[10px] text-slate-400 uppercase mb-1">Mode</label>
              <select id="mock-mode" class="w-full bg-slate-900 border border-slate-800 rounded px-2.5 py-1.5 text-xs text-slate-200">
                <option value="always">Always Mock (Intercept All)</option>
                <option value="on_error">On-Error Fallback (Only if target is 502/down)</option>
              </select>
            </div>
          </div>
          <div class="grid grid-cols-2 gap-2">
            <div>
              <label class="block text-[10px] text-slate-400 uppercase mb-1">Status Code</label>
              <input id="mock-status" type="number" value="200" class="w-full bg-slate-900 border border-slate-800 rounded px-2.5 py-1.5 text-xs text-slate-200 font-mono">
            </div>
            <div class="flex items-center pt-5">
              <label class="flex items-center space-x-2 text-xs text-slate-300 cursor-pointer">
                <input type="checkbox" id="mock-enabled" checked class="rounded bg-slate-900 border-slate-700 text-emerald-500">
                <span>Enable Mock Rule</span>
              </label>
            </div>
          </div>
          <div class="flex-1 flex flex-col">
            <label class="block text-[10px] text-slate-400 uppercase mb-1">Mock JSON Response Body</label>
            <textarea id="mock-body" class="w-full flex-1 bg-slate-900 border border-slate-800 rounded p-2 text-[11px] text-emerald-400 font-mono focus:outline-none focus:border-cyan-500">{\n  "status": "mocked_success",\n  "message": "Unblocked frontend payload"\n}</textarea>
          </div>
        </div>
      </div>
    </div>

    <!-- VIEW 4: DOCKER LOGS CONSOLE -->
    <div id="view-logs" class="hidden h-full glass-card rounded-xl p-4 flex flex-col shadow-2xl">
      <div class="flex items-center justify-between border-b border-slate-800 pb-2 mb-3">
        <h2 class="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center space-x-2">
          <i class="fa-brands fa-docker text-sky-400 text-sm"></i>
          <span>Container Output Stream (Demultiplexed)</span>
        </h2>
        <div class="flex items-center space-x-3 text-xs">
          <label class="flex items-center space-x-1.5 text-slate-400 cursor-pointer text-[11px]">
            <input type="checkbox" id="logs-autoscroll" checked class="rounded bg-slate-900 border-slate-700 text-cyan-500">
            <span>Auto-scroll</span>
          </label>
          <button onclick="clearDockerLogs()" class="text-slate-400 hover:text-rose-400 btn-action"><i class="fa-solid fa-trash mr-1"></i> Clear</button>
        </div>
      </div>
      <div id="docker-logs-console" class="flex-1 overflow-y-auto bg-slate-950/90 p-3 rounded-lg border border-slate-800 font-mono text-[11px] space-y-1 shadow-inner">
        <div class="text-slate-600">Waiting for Docker container logs...</div>
      </div>
    </div>

    <!-- VIEW 5: WATERFALL GRAPH -->
    <div id="view-waterfall" class="hidden h-full glass-card rounded-xl p-4 flex flex-col shadow-2xl">
      <div class="flex items-center justify-between border-b border-slate-800 pb-2 mb-3">
        <h2 class="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center space-x-2">
          <i class="fa-solid fa-chart-gantt text-purple-400"></i>
          <span>Distributed Trace Execution Hierarchy</span>
        </h2>
        <span class="text-[11px] text-purple-400/90 bg-purple-950/50 border border-purple-800/60 px-2 py-0.5 rounded">W3C traceparent Linked</span>
      </div>
      <div id="waterfall-container" class="flex-1 overflow-y-auto p-2 space-y-2">
        <div class="text-slate-500 text-xs text-center mt-12">Select a trace with sub-requests to render the waterfall timeline</div>
      </div>
    </div>
  </main>

  <script>
    var allTraces = [];
    var filteredTraces = [];
    var selectedTrace = null;
    var isPaused = false;
    var statusFilter = 'all';
    var dockerLogs = [];
    var serviceHealth = {};
    var mockRules = [];

    var traceListEl = document.getElementById('trace-list');
    var traceDetailEl = document.getElementById('trace-detail-content');
    var traceActionsEl = document.getElementById('trace-actions');
    var wsStatusEl = document.getElementById('ws-status');
    var eventCountEl = document.getElementById('event-count');
    var dockerConsoleEl = document.getElementById('docker-logs-console');
    var healthPillsEl = document.getElementById('health-pills');

    var authToken = new URLSearchParams(window.location.search).get('token') || '';
    if (authToken) {
      var origFetch = window.fetch;
      window.fetch = function(url, opts) {
        opts = opts || {};
        opts.headers = opts.headers || {};
        if (opts.headers instanceof Headers) {
          opts.headers.set('Authorization', 'Bearer ' + authToken);
        } else {
          opts.headers['Authorization'] = 'Bearer ' + authToken;
        }
        return origFetch(url, opts);
      };
    }

    function connectWS() {
      var proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      var wsUrl = proto + '//' + window.location.host + '/ws' + (authToken ? '?token=' + encodeURIComponent(authToken) : '');
      var ws = new WebSocket(wsUrl);

      ws.onopen = function() {
        wsStatusEl.innerHTML = '<span class="h-2 w-2 rounded-full bg-emerald-500 animate-pulse"></span><span class="text-emerald-400 font-bold">Mesh Active</span>';
        fetchHealth();
        fetchMocks();
      };

      ws.onmessage = function(e) {
        if (isPaused) return;
        try {
          var payload = JSON.parse(e.data);
          if (payload.type === 'log') {
            appendDockerLog(payload);
          } else if (payload.type === 'health') {
            updateHealthPill(payload.target);
          } else {
            handleIncomingTrace(payload);
          }
        } catch(err) {}
      };

      ws.onclose = function() {
        wsStatusEl.innerHTML = '<span class="h-2 w-2 rounded-full bg-rose-500"></span><span class="text-rose-400">Disconnected</span>';
        setTimeout(connectWS, 2000);
      };
    }

    function fetchHealth() {
      fetch('/api/health')
        .then(function(r){ return r.json(); })
        .then(function(targets){
          healthPillsEl.innerHTML = '';
          targets.forEach(function(t){
            serviceHealth[t.url] = t;
            renderHealthPill(t);
          });
        }).catch(function(){});
    }

    function renderHealthPill(t) {
      var pill = document.createElement('div');
      var statusColor = t.healthy ? 'bg-emerald-950/80 text-emerald-300 border-emerald-700/60' : 'bg-rose-950/80 text-rose-300 border-rose-700/60';
      var dotColor = t.healthy ? 'bg-emerald-400' : 'bg-rose-500';
      pill.className = 'px-2 py-0.5 rounded border flex items-center space-x-1.5 text-[10px] flex-shrink-0 shadow-sm cursor-pointer ' + statusColor;
      pill.id = 'health-pill-' + sanitizeId(t.host);
      pill.title = t.url + ' • Last latency: ' + t.latency_ms + 'ms';
      pill.innerHTML = '<span class="h-1.5 w-1.5 rounded-full ' + dotColor + '"></span>' +
        '<span class="font-bold font-mono">' + escapeHtml(t.host) + '</span>' +
        '<span class="text-[9px] opacity-75">' + t.latency_ms + 'ms</span>';
      pill.onclick = function() {
        document.getElementById('mock-prefix').value = '/' + t.host.split(':')[0];
        switchTab('mocks');
      };
      healthPillsEl.appendChild(pill);
    }

    function updateHealthPill(t) {
      serviceHealth[t.url] = t;
      var el = document.getElementById('health-pill-' + sanitizeId(t.host));
      if (el) {
        var statusColor = t.healthy ? 'bg-emerald-950/80 text-emerald-300 border-emerald-700/60' : 'bg-rose-950/80 text-rose-300 border-rose-700/60';
        var dotColor = t.healthy ? 'bg-emerald-400' : 'bg-rose-500';
        el.className = 'px-2 py-0.5 rounded border flex items-center space-x-1.5 text-[10px] flex-shrink-0 shadow-sm cursor-pointer ' + statusColor;
        el.innerHTML = '<span class="h-1.5 w-1.5 rounded-full ' + dotColor + '"></span>' +
          '<span class="font-bold font-mono">' + escapeHtml(t.host) + '</span>' +
          '<span class="text-[9px] opacity-75">' + t.latency_ms + 'ms</span>';
      } else {
        renderHealthPill(t);
      }
    }

    function sanitizeId(str) {
      return str.replace(/[^a-zA-Z0-9]/g, '_');
    }

    function fetchMocks() {
      fetch('/api/mocks')
        .then(function(r){ return r.json(); })
        .then(function(rules){
          mockRules = rules;
          renderMockRules();
        }).catch(function(){});
    }

    function renderMockRules() {
      var list = document.getElementById('mock-rules-list');
      list.innerHTML = '';
      if (!mockRules || mockRules.length === 0) {
        list.innerHTML = '<span class="text-slate-600 text-xs">No active mock rules configured.</span>';
        return;
      }
      mockRules.forEach(function(m){
        var item = document.createElement('div');
        item.className = 'p-2 rounded bg-slate-900/90 border border-slate-800 flex justify-between items-center text-xs';
        item.innerHTML = '<div>' +
          '<span class="font-bold text-emerald-400 font-mono">' + escapeHtml(m.prefix) + '</span>' +
          '<span class="text-[10px] text-slate-500 block">Mode: ' + m.mode + ' (' + m.status_code + ')</span>' +
        '</div>' +
        '<button onclick="toggleMock(\'' + escapeHtml(m.prefix) + '\', ' + !m.enabled + ')" class="px-2 py-0.5 rounded text-[10px] font-bold ' + (m.enabled ? 'bg-emerald-900 text-emerald-300 border border-emerald-700' : 'bg-slate-800 text-slate-500') + '">' +
          (m.enabled ? 'MOCK ON' : 'OFF') +
        '</button>';
        list.appendChild(item);
      });
    }

    function saveMockRule() {
      var prefix = document.getElementById('mock-prefix').value;
      var mode = document.getElementById('mock-mode').value;
      var status = parseInt(document.getElementById('mock-status').value) || 200;
      var enabled = document.getElementById('mock-enabled').checked;
      var body = document.getElementById('mock-body').value;

      fetch('/api/mocks', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prefix: prefix, mode: mode, status_code: status, enabled: enabled, body: body })
      }).then(function(){
        fetchMocks();
      });
    }

    function toggleMock(prefix, state) {
      fetch('/api/mocks/toggle', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prefix: prefix, enabled: state })
      }).then(function(){
        fetchMocks();
      });
    }

    function handleIncomingTrace(ev) {
      allTraces.unshift(ev);
      if (allTraces.length > 500) allTraces.pop();
      eventCountEl.textContent = allTraces.length;
      applyFilters();
    }

    function applyFilters() {
      var query = document.getElementById('filter-text').value.toLowerCase();
      filteredTraces = allTraces.filter(function(ev) {
        if (statusFilter === '2xx' && (ev.status_code < 200 || ev.status_code >= 300)) return false;
        if (statusFilter === '4xx' && (ev.status_code < 400 || ev.status_code >= 500)) return false;
        if (statusFilter === '5xx' && ev.status_code < 500) return false;

        if (!query) return true;
        return ev.path.toLowerCase().includes(query) ||
               ev.method.toLowerCase().includes(query) ||
               String(ev.status_code).includes(query) ||
               (ev.request_body && ev.request_body.toLowerCase().includes(query)) ||
               (ev.response_body && ev.response_body.toLowerCase().includes(query));
      });
      renderTraceList();
    }

    function setStatusFilter(status) {
      statusFilter = status;
      ['all', '2xx', '4xx', '5xx'].forEach(function(s) {
        var btn = document.getElementById('filter-btn-' + s);
        if (s === status) {
          btn.className = 'px-2 py-0.5 rounded bg-slate-800 text-cyan-400 font-bold border border-slate-700';
        } else {
          btn.className = 'px-2 py-0.5 rounded hover:bg-slate-800 text-slate-400';
        }
      });
      applyFilters();
    }

    function renderTraceList() {
      traceListEl.innerHTML = '';
      filteredTraces.forEach(function(ev) {
        var badgeClass = ev.status_code >= 500 ? 'badge-5xx' : (ev.status_code >= 400 ? 'badge-4xx' : (ev.status_code >= 300 ? 'badge-3xx' : 'badge-2xx'));
        var latencyColor = ev.duration_ms < 100 ? 'text-emerald-400' : (ev.duration_ms < 500 ? 'text-yellow-400' : 'text-rose-400');
        var item = document.createElement('div');
        item.className = 'p-2.5 bg-slate-950/80 hover:bg-slate-800/80 border border-slate-800/80 rounded-lg cursor-pointer transition shadow-sm';
        item.onclick = function() { selectTrace(ev); };
        item.innerHTML = '<div class="flex items-center justify-between mb-1">' +
            '<div class="flex items-center space-x-2">' +
              '<span class="px-1.5 py-0.5 rounded text-[10px] font-bold font-mono ' + badgeClass + '">' + ev.status_code + '</span>' +
              '<span class="font-bold text-slate-200 text-xs font-mono">' + ev.method + '</span>' +
            '</div>' +
            '<span class="text-[10px] font-mono ' + latencyColor + '">' + ev.duration_ms + 'ms</span>' +
          '</div>' +
          '<div class="text-xs text-cyan-400 truncate font-mono mb-1">' + escapeHtml(ev.path) + '</div>' +
          '<div class="flex items-center justify-between text-[10px] text-slate-500">' +
            '<span class="truncate max-w-[130px] font-mono">' + escapeHtml(ev.target) + '</span>' +
            '<span class="font-mono">' + ev.timestamp + '</span>' +
          '</div>';
        traceListEl.appendChild(item);
      });
    }

    function selectTrace(ev) {
      selectedTrace = ev;
      traceActionsEl.classList.remove('hidden');
      traceDetailEl.className = 'flex-1 overflow-y-auto text-xs text-slate-300 space-y-4 font-mono pr-1';
      var html = '<div class="grid grid-cols-2 md:grid-cols-4 gap-2 bg-slate-950/90 p-3 rounded-lg border border-slate-800 shadow-inner">' +
          '<div><span class="text-slate-500 text-[10px] block uppercase">Trace ID</span><span class="text-purple-400 font-bold">' + ev.trace_id + '</span></div>' +
          '<div><span class="text-slate-500 text-[10px] block uppercase">Duration</span><span class="text-emerald-400 font-bold">' + ev.duration_ms + ' ms</span></div>' +
          '<div><span class="text-slate-500 text-[10px] block uppercase">Status</span><span class="font-bold">' + ev.status_code + '</span></div>' +
          '<div><span class="text-slate-500 text-[10px] block uppercase">Time</span><span>' + ev.timestamp + '</span></div>' +
        '</div>' +
        '<div class="bg-slate-950/90 p-2.5 rounded-lg border border-slate-800 text-[11px] shadow-inner">' +
          '<span class="text-slate-500 text-[10px] block uppercase mb-1">Upstream Target</span>' +
          '<span class="text-cyan-400 font-mono">' + escapeHtml(ev.target) + '</span>' +
        '</div>' +
        '<div>' +
          '<div class="flex justify-between items-center mb-1">' +
            '<h3 class="font-bold text-slate-400 text-[10px] uppercase tracking-wider">Request Headers</h3>' +
            '<button onclick="copyToClip(\'' + escapeHtml(JSON.stringify(ev.req_headers)) + '\')" class="text-[10px] text-slate-400 hover:text-cyan-400"><i class="fa-solid fa-copy mr-1"></i>Copy</button>' +
          '</div>' +
          '<pre class="bg-slate-950/90 p-2.5 rounded-lg border border-slate-800 text-[11px] overflow-x-auto text-slate-300 font-mono shadow-inner">' + JSON.stringify(ev.req_headers, null, 2) + '</pre>' +
        '</div>';

      if (ev.request_body) {
        html += '<div>' +
          '<div class="flex justify-between items-center mb-1">' +
            '<h3 class="font-bold text-slate-400 text-[10px] uppercase tracking-wider">Request Body</h3>' +
            '<button onclick="copyToClip(\'' + escapeHtml(ev.request_body) + '\')" class="text-[10px] text-slate-400 hover:text-cyan-400"><i class="fa-solid fa-copy mr-1"></i>Copy</button>' +
          '</div>' +
          '<pre class="bg-slate-950/90 p-2.5 rounded-lg border border-slate-800 text-[11px] text-emerald-400 overflow-x-auto max-h-48 font-mono shadow-inner">' + formatJSON(ev.request_body) + '</pre>' +
        '</div>';
      }

      html += '<div>' +
          '<div class="flex justify-between items-center mb-1">' +
            '<h3 class="font-bold text-slate-400 text-[10px] uppercase tracking-wider">Response Headers</h3>' +
            '<button onclick="copyToClip(\'' + escapeHtml(JSON.stringify(ev.resp_headers)) + '\')" class="text-[10px] text-slate-400 hover:text-cyan-400"><i class="fa-solid fa-copy mr-1"></i>Copy</button>' +
          '</div>' +
          '<pre class="bg-slate-950/90 p-2.5 rounded-lg border border-slate-800 text-[11px] overflow-x-auto text-slate-300 font-mono shadow-inner">' + JSON.stringify(ev.resp_headers, null, 2) + '</pre>' +
        '</div>';

      if (ev.response_body) {
        html += '<div>' +
          '<div class="flex justify-between items-center mb-1">' +
            '<h3 class="font-bold text-slate-400 text-[10px] uppercase tracking-wider">Response Body</h3>' +
            '<button onclick="copyToClip(\'' + escapeHtml(ev.response_body) + '\')" class="text-[10px] text-slate-400 hover:text-cyan-400"><i class="fa-solid fa-copy mr-1"></i>Copy</button>' +
          '</div>' +
          '<pre class="bg-slate-950/90 p-2.5 rounded-lg border border-slate-800 text-[11px] text-sky-400 overflow-x-auto max-h-56 font-mono shadow-inner">' + formatJSON(ev.response_body) + '</pre>' +
        '</div>';
      }

      traceDetailEl.innerHTML = html;
      renderWaterfall(ev.trace_id);
    }

    function formatJSON(str) {
      try {
        var parsed = JSON.parse(str);
        return escapeHtml(JSON.stringify(parsed, null, 2));
      } catch(e) {
        return escapeHtml(str);
      }
    }

    function appendDockerLog(entry) {
      if (dockerLogs.length === 0) dockerConsoleEl.innerHTML = '';
      dockerLogs.push(entry);
      var colorClass = entry.stream === 'stderr' ? 'text-rose-400' : 'text-slate-300';
      var logLine = document.createElement('div');
      logLine.className = 'flex items-start space-x-2 py-0.5 border-b border-slate-900/60 font-mono';
      logLine.innerHTML = '<span class="text-slate-600 text-[10px]">' + entry.timestamp + '</span>' +
        '<span class="text-sky-400 text-[10px] font-bold">[' + escapeHtml(entry.container) + ']</span>' +
        '<span class="' + colorClass + '">' + escapeHtml(entry.message) + '</span>';
      dockerConsoleEl.appendChild(logLine);

      if (document.getElementById('logs-autoscroll').checked) {
        dockerConsoleEl.scrollTop = dockerConsoleEl.scrollHeight;
      }
    }

    function clearDockerLogs() {
      dockerLogs = [];
      dockerConsoleEl.innerHTML = '<div class="text-slate-600">Docker logs cleared</div>';
    }

    function openInReplay() {
      if (!selectedTrace) return;
      document.getElementById('replay-method').value = selectedTrace.method;
      document.getElementById('replay-url').value = selectedTrace.path;
      document.getElementById('replay-headers').value = JSON.stringify(selectedTrace.req_headers || {}, null, 2);
      document.getElementById('replay-body').value = selectedTrace.request_body || '';

      var provider = '';
      if (selectedTrace.req_headers) {
        for (var k in selectedTrace.req_headers) {
          var lk = k.toLowerCase();
          if (lk === 'stripe-signature') provider = 'stripe';
          else if (lk === 'x-hub-signature-256') provider = 'github';
          else if (lk === 'x-razorpay-signature') provider = 'razorpay';
          else if (lk === 'x-shopify-hmac-sha256') provider = 'shopify';
          else if (lk === 'x-slack-signature') provider = 'slack';
        }
      }
      document.getElementById('replay-webhook-provider').value = provider;
      switchTab('replay');
    }

    function loadPreset(type) {
      if (type === 'stripe') {
        document.getElementById('replay-method').value = 'POST';
        document.getElementById('replay-url').value = '/webhook/stripe';
        document.getElementById('replay-webhook-provider').value = 'stripe';
        document.getElementById('replay-webhook-secret').value = 'whsec_testsecret123';
        document.getElementById('replay-headers').value = '{\n  "Content-Type": "application/json"\n}';
        document.getElementById('replay-body').value = '{\n  "id": "evt_101",\n  "type": "payment_intent.succeeded",\n  "amount": 4900,\n  "currency": "usd"\n}';
      } else if (type === 'github') {
        document.getElementById('replay-method').value = 'POST';
        document.getElementById('replay-url').value = '/webhook/github';
        document.getElementById('replay-webhook-provider').value = 'github';
        document.getElementById('replay-webhook-secret').value = 'gh_sec_999';
        document.getElementById('replay-headers').value = '{\n  "Content-Type": "application/json",\n  "X-GitHub-Event": "push"\n}';
        document.getElementById('replay-body').value = '{\n  "ref": "refs/heads/main",\n  "commits": [{"id": "a1b2c3d", "message": "feat: mesh upgrade"}]\n}';
      } else if (type === 'razorpay') {
        document.getElementById('replay-method').value = 'POST';
        document.getElementById('replay-url').value = '/api/payment/webhook';
        document.getElementById('replay-webhook-provider').value = 'razorpay';
        document.getElementById('replay-webhook-secret').value = 'rzp_sec_mock';
        document.getElementById('replay-headers').value = '{\n  "Content-Type": "application/json"\n}';
        document.getElementById('replay-body').value = '{\n  "event": "payment.authorized",\n  "payload": {"payment": {"entity": {"id": "pay_999", "amount": 50000}}}\n}';
      } else if (type === 'auth_login') {
        document.getElementById('replay-method').value = 'POST';
        document.getElementById('replay-url').value = '/auth/login';
        document.getElementById('replay-webhook-provider').value = '';
        document.getElementById('replay-headers').value = '{\n  "Content-Type": "application/json"\n}';
        document.getElementById('replay-body').value = '{\n  "email": "user@example.com",\n  "password": "Password123!"\n}';
      }
    }

    function sendReplayRequest() {
      var method = document.getElementById('replay-method').value;
      var path = document.getElementById('replay-url').value;
      var headers = {};
      try { headers = JSON.parse(document.getElementById('replay-headers').value); } catch(e) {}
      var body = document.getElementById('replay-body').value;
      var provider = document.getElementById('replay-webhook-provider').value;
      var secret = document.getElementById('replay-webhook-secret').value;
      var bypassSig = document.getElementById('replay-bypass-sig').checked;

      var replayBadge = document.getElementById('replay-status-badge');
      var replayContent = document.getElementById('replay-response-content');
      replayBadge.innerHTML = '<span class="text-yellow-400 font-mono">Executing...</span>';

      fetch('/api/replay', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          method: method,
          url: path,
          headers: headers,
          body: body,
          resign_provider: provider,
          webhook_secret: secret,
          bypass_signatures: bypassSig
        })
      })
      .then(function(r) { return r.json(); })
      .then(function(res) {
        var badgeClass = res.status_code >= 500 ? 'badge-5xx' : (res.status_code >= 400 ? 'badge-4xx' : 'badge-2xx');
        replayBadge.innerHTML = '<span class="px-2 py-0.5 rounded text-xs font-bold font-mono ' + badgeClass + '">' + res.status_code + ' (' + res.duration_ms + 'ms)</span>';
        replayContent.className = 'flex-1 overflow-y-auto text-xs text-slate-300 space-y-3 font-mono p-1';
        replayContent.innerHTML = '<div><h4 class="text-slate-400 text-[10px] uppercase font-bold mb-1">Response Headers</h4>' +
          '<pre class="bg-slate-950 p-2 rounded border border-slate-800">' + JSON.stringify(res.headers, null, 2) + '</pre></div>' +
          '<div><h4 class="text-slate-400 text-[10px] uppercase font-bold mb-1">Response Body</h4>' +
          '<pre class="bg-slate-950 p-2 rounded border border-slate-800 text-sky-400 max-h-72 overflow-x-auto">' + formatJSON(res.body) + '</pre></div>';
      })
      .catch(function(err) {
        replayBadge.innerHTML = '<span class="text-rose-400 font-bold font-mono">Failed</span>';
        replayContent.innerHTML = '<span class="text-rose-400 font-mono">' + escapeHtml(err.message) + '</span>';
      });
    }

    function renderWaterfall(traceID) {
      var related = allTraces.filter(function(t) { return t.trace_id === traceID; });
      var container = document.getElementById('waterfall-container');
      if (related.length === 0) {
        container.innerHTML = '<div class="text-slate-500 text-xs text-center mt-12 font-mono">No related sub-spans found for Trace ID: ' + traceID + '</div>';
        return;
      }
      var maxDur = Math.max.apply(null, related.map(function(t) { return t.duration_ms; })) || 1;
      var html = '<div class="bg-slate-950/90 p-3 rounded-lg border border-slate-800 mb-3 shadow-inner">' +
        '<span class="text-purple-400 font-bold text-xs font-mono">Trace ID: ' + traceID + '</span> (' + related.length + ' hops recorded)' +
        '</div><div class="space-y-2.5">';

      related.forEach(function(t, idx) {
        var pct = Math.max(10, (t.duration_ms / maxDur) * 100);
        html += '<div class="bg-slate-950/80 p-2.5 rounded-lg border border-slate-800 font-mono text-xs shadow-sm">' +
          '<div class="flex justify-between items-center mb-1.5">' +
            '<span class="font-bold text-cyan-400">' + t.method + ' ' + escapeHtml(t.path) + '</span>' +
            '<span class="text-slate-400 font-bold">' + t.duration_ms + ' ms</span>' +
          '</div>' +
          '<div class="w-full bg-slate-900 h-2 rounded-full overflow-hidden mb-1">' +
            '<div class="bg-gradient-to-r from-cyan-500 via-sky-400 to-indigo-500 h-full rounded-full" style="width: ' + pct + '%"></div>' +
          '</div>' +
          '<div class="text-[10px] text-slate-500 flex justify-between">' +
            '<span>Target: ' + escapeHtml(t.target) + '</span>' +
            '<span>Status: ' + t.status_code + '</span>' +
          '</div>' +
        '</div>';
      });
      html += '</div>';
      container.innerHTML = html;
    }

    function copyAsCurl() {
      if (!selectedTrace) return;
      var curl = 'curl -X ' + selectedTrace.method + ' "http://localhost:4000' + selectedTrace.path + '"';
      if (selectedTrace.req_headers) {
        for (var k in selectedTrace.req_headers) {
          curl += ' -H "' + k + ': ' + selectedTrace.req_headers[k] + '"';
        }
      }
      if (selectedTrace.request_body) {
        curl += " -d '" + selectedTrace.request_body.replace(/'/g, "\\'") + "'";
      }
      navigator.clipboard.writeText(curl);
      alert('cURL command copied to clipboard!');
    }

    function copyToClip(text) {
      navigator.clipboard.writeText(text);
    }

    function switchTab(tab) {
      ['traces', 'replay', 'mocks', 'logs', 'waterfall'].forEach(function(t) {
        document.getElementById('view-' + t).classList.add('hidden');
        document.getElementById('tab-btn-' + t).className = 'pb-2 text-slate-400 hover:text-slate-200 flex items-center space-x-2 transition';
      });
      document.getElementById('view-' + tab).classList.remove('hidden');
      document.getElementById('tab-btn-' + tab).className = 'pb-2 tab-active flex items-center space-x-2 transition';
      if (tab === 'mocks') fetchMocks();
      if (tab === 'waterfall') {
        if (selectedTrace) {
          renderWaterfall(selectedTrace.trace_id);
        } else if (allTraces.length > 0) {
          renderWaterfall(allTraces[0].trace_id);
        }
      }
    }

    function togglePause() {
      isPaused = !isPaused;
      var btn = document.getElementById('pause-btn');
      btn.innerHTML = isPaused ? '<i class="fa-solid fa-play text-yellow-400"></i>' : '<i class="fa-solid fa-pause"></i>';
    }

    function clearTraces() {
      allTraces = [];
      filteredTraces = [];
      renderTraceList();
      eventCountEl.textContent = '0';
      traceActionsEl.classList.add('hidden');
      traceDetailEl.className = 'flex-1 overflow-y-auto text-xs text-slate-400 flex items-center justify-center border border-dashed border-slate-800/80 rounded-lg p-4';
      traceDetailEl.innerHTML = 'Select any incoming request from the live switchboard to inspect headers, JSON payloads, and timings';
    }

    function escapeHtml(str) {
      if (!str) return '';
      return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    connectWS();
  </script>
</body>
</html>`
