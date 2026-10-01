/* Lockstep dashboard: one WebSocket in, everything on screen out. No framework, no sample data. */
"use strict";
(() => {
  const $ = (id) => document.getElementById(id);
  const onHttp = location.protocol.startsWith("http");
  const HTTP_BASE = onHttp ? "" : "http://127.0.0.1:8000";
  const WS_URL = onHttp ? `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws/stream` : "ws://127.0.0.1:8000/ws/stream";
  const SPEEDS = [1, 2, 5, 10, 30];
  const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const VERDICT = {
    NORMAL: "Yes — your assets are still acting as separate bets.",
    WARNING: "Weakening — your assets are starting to move together.",
    CONTAGION: "No — your assets are moving as one.",
    WARMING_UP: "Not yet — learning what is normal for this basket.",
  };
  const BAND = { NORMAL: "rgba(155,232,112,0.07)", WARNING: "rgba(242,177,52,0.18)", CONTAGION: "rgba(255,107,97,0.24)", WARMING_UP: "rgba(0,0,0,0)" };
  const DOT = { NORMAL: "#9BE870", WARNING: "#F2B134", CONTAGION: "#FF6B61", WARMING_UP: "#6f8278" };
  const STOPS = [[0.3, [31, 111, 85]], [0.7, [201, 162, 39]], [0.92, [229, 72, 61]]];

  const state = { session: null, snap: null, history: [], events: [], wsOpen: false, backendDown: false, everConnected: false, retry: 0, lastStatus: null, heatGeom: null };

  /* ---------------- formatting ---------------- */
  const pad = (n) => String(n).padStart(2, "0");
  function fmtTime(ms, withDate = true) {
    if (ms == null) return "—";
    const d = new Date(ms);
    const hm = `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())} UTC`;
    return withDate ? `${hm} · ${d.getUTCDate()} ${MONTHS[d.getUTCMonth()]} ${d.getUTCFullYear()}` : hm;
  }
  const fmtPct = (x, d = 0) => (x == null ? "—" : `${(x * 100).toFixed(d)}%`);
  const fmtSigned = (x, d = 1) => (x == null ? "—" : `${x > 0 ? "+" : x < 0 ? "−" : ""}${Math.abs(x * 100).toFixed(d)}%`);
  const fmtNum = (x, d = 2) => (x == null ? "—" : Number(x).toFixed(d));

  function toast(text) {
    const t = $("toast");
    t.textContent = text;
    t.classList.add("on");
    clearTimeout(toast.timer);
    toast.timer = setTimeout(() => t.classList.remove("on"), 4500);
  }

  /* ---------------- connection ---------------- */
  function connect() {
    let ws;
    try { ws = new WebSocket(WS_URL); } catch (e) { scheduleReconnect(); return; }
    ws.onopen = () => { state.wsOpen = true; state.everConnected = true; state.backendDown = false; state.retry = 0; renderConn(); renderBanner(); };
    ws.onmessage = (ev) => { let msg; try { msg = JSON.parse(ev.data); } catch (e) { return; } handle(msg); };
    ws.onclose = () => { state.wsOpen = false; renderConn(); renderBanner(); scheduleReconnect(); };
  }
  function scheduleReconnect() {
    state.retry += 1;
    const delay = Math.min(10000, 1000 * 2 ** Math.min(state.retry - 1, 4));
    fetch(`${HTTP_BASE}/api/health`, { cache: "no-store" })
      .then((r) => { state.backendDown = !r.ok; })
      .catch(() => { state.backendDown = true; })
      .finally(() => { renderConn(); renderBanner(); });
    setTimeout(connect, delay);
  }
  function handle(msg) {
    if (msg.type === "hello") {
      state.session = msg.session; state.snap = msg.state; state.history = msg.history || []; state.events = msg.events || [];
      state.lastStatus = msg.state ? msg.state.status : null;
      renderAll();
    } else if (msg.type === "snapshot") {
      state.snap = msg; addHistory(msg); renderSnapshot();
    } else if (msg.type === "event") {
      state.events.push(msg);
      if (state.events.length > 300) state.events.shift();
      renderEvents();
      if (msg.kind === "system" && msg.level !== "info") toast(msg.message);
    } else if (msg.type === "session") {
      state.session = msg; renderSession(); renderConn(); renderBanner();
      if (msg.message) toast(msg.message);
    }
  }
  function addHistory(s) {
    const last = state.history[state.history.length - 1];
    if (last && last.ts >= s.ts) return;
    state.history.push({ ts: s.ts, time: s.time, status: s.status, effective_bets: s.effective_bets, z_score: s.z_score, exposure: s.exposure, basket_return_60m: s.basket_return_60m });
    if (state.history.length > 720) state.history.shift();
  }

  /* ---------------- rendering ---------------- */
  function renderAll() { renderSession(); renderSnapshot(); renderEvents(); renderConn(); renderBanner(); }

  function setStatusClass(st) {
    const cls = { NORMAL: "st-normal", WARNING: "st-warning", CONTAGION: "st-contagion" }[st] || "st-warming";
    document.body.classList.remove("st-loading", "st-normal", "st-warning", "st-contagion", "st-warming");
    document.body.classList.add(cls);
  }

  function renderSnapshot() {
    const s = state.snap;
    if (!s) {
      setStatusClass("WARMING_UP");
      const live = state.session && state.session.mode === "live";
      $("statusWord").textContent = "—";
      $("verdict").textContent = live ? "Waiting for the first live minute…" : "No data yet.";
      $("explanation").textContent = state.session && state.session.feed_state === "offline" ? "Replay data file is missing. Run: python -m backend.fetch_replay" : "";
      $("exposureVal").textContent = "—"; $("asOf").textContent = ""; $("warmup").hidden = true;
      ["betsVal", "nAssets", "basketRet", "avgCorr", "dataQuality"].forEach((id) => { $(id).textContent = "—"; });
      $("leaderList").innerHTML = ""; $("betsDots").innerHTML = "";
      drawHeatmap(); drawTimeline(); renderTech(null); renderProgress(null);
      return;
    }
    setStatusClass(s.status);
    $("statusWord").textContent = s.status === "WARMING_UP" ? "WARMING UP" : s.status;
    $("verdict").textContent = VERDICT[s.status] || "";
    $("explanation").textContent = s.explanation || "";
    $("exposureVal").textContent = fmtPct(s.exposure);
    $("asOf").textContent = `as of ${fmtTime(s.ts)} · ${s.mode === "live" ? "live Binance data" : "replay of real Binance data"}`;
    const w = s.warmup || {};
    const warming = s.status === "WARMING_UP" && w.need;
    $("warmup").hidden = !warming;
    if (warming) {
      $("warmupFill").style.width = `${Math.min(100, (100 * w.have) / w.need)}%`;
      $("warmupText").textContent = `Collecting history: ${w.have} / ${w.need} minutes`;
    }
    renderBets(s); drawHeatmap(); renderLeaders(s); renderBasket(s); drawTimeline(); renderTech(s); renderProgress(s);
    if (state.lastStatus && state.lastStatus !== s.status) {
      const hero = $("hero");
      hero.classList.remove("pulse"); void hero.offsetWidth; hero.classList.add("pulse");
    }
    state.lastStatus = s.status;
  }

  function renderBets(s) {
    const n = s.n_assets || 0;
    const b = s.effective_bets;
    $("nAssets").textContent = n || "—";
    $("betsVal").textContent = b == null ? "—" : b.toFixed(1);
    const box = $("betsDots");
    if (box.children.length !== n) {
      box.innerHTML = "";
      box.style.gridTemplateColumns = `repeat(${n || 12}, 1fr)`;
      for (let i = 0; i < n; i += 1) {
        const d = document.createElement("div"); d.className = "bet"; d.appendChild(document.createElement("span")); box.appendChild(d);
      }
    }
    Array.from(box.children).forEach((d, i) => {
      const f = b == null ? 0 : Math.max(0, Math.min(1, b - i));
      d.firstChild.style.width = `${f * 100}%`;
    });
  }

  function heatColor(v) {
    if (v <= STOPS[0][0]) return `rgb(${STOPS[0][1].join(",")})`;
    if (v >= STOPS[2][0]) return `rgb(${STOPS[2][1].join(",")})`;
    const [a, b] = v < STOPS[1][0] ? [STOPS[0], STOPS[1]] : [STOPS[1], STOPS[2]];
    const t = (v - a[0]) / (b[0] - a[0]);
    return `rgb(${a[1].map((c, k) => Math.round(c + (b[1][k] - c) * t)).join(",")})`;
  }
  function setupCanvas(cv) {
    const dpr = window.devicePixelRatio || 1;
    const w = cv.clientWidth; const h = cv.clientHeight;
    if (!w || !h) return null;
    if (cv.width !== Math.round(w * dpr) || cv.height !== Math.round(h * dpr)) { cv.width = Math.round(w * dpr); cv.height = Math.round(h * dpr); }
    const ctx = cv.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);
    return { ctx, w, h };
  }
  function drawHeatmap() {
    const c = setupCanvas($("heatmap"));
    if (!c) return;
    const { ctx, w, h } = c;
    const s = state.snap;
    ctx.font = "12px Inter, system-ui, sans-serif"; ctx.textAlign = "left"; ctx.textBaseline = "alphabetic";
    if (!s || !s.correlation) {
      ctx.fillStyle = "#6f8278"; ctx.fillText("The heatmap appears after 2 hours of 1-minute data.", 12, 24);
      state.heatGeom = null; $("heatAvg").textContent = ""; return;
    }
    const C = s.correlation; const n = C.length; const labels = s.short_symbols || s.symbols;
    const L = 44; const T = 22;
    const cell = Math.min((w - L - 2) / n, (h - T - 2) / n);
    state.heatGeom = { L, T, cell, n };
    for (let i = 0; i < n; i += 1) {
      for (let j = 0; j < n; j += 1) {
        ctx.fillStyle = i === j ? "#2a3530" : heatColor(C[i][j]);
        ctx.fillRect(L + j * cell + 0.5, T + i * cell + 0.5, cell - 1, cell - 1);
      }
    }
    ctx.fillStyle = "#9fb3a8";
    ctx.font = `${Math.max(9, Math.min(12, cell * 0.38))}px Inter, system-ui, sans-serif`;
    ctx.textAlign = "right"; ctx.textBaseline = "middle";
    for (let i = 0; i < n; i += 1) ctx.fillText(labels[i], L - 6, T + i * cell + cell / 2);
    ctx.textAlign = "center"; ctx.textBaseline = "bottom";
    for (let j = 0; j < n; j += 1) ctx.fillText(labels[j], L + j * cell + cell / 2, T - 4);
    $("heatAvg").textContent = s.avg_correlation == null ? "" : `average ${fmtNum(s.avg_correlation)}`;
  }
  function onHeatMove(e) {
    const g = state.heatGeom; const s = state.snap; const tip = $("heatTip");
    if (!g || !s || !s.correlation) { tip.hidden = true; return; }
    const r = $("heatmap").getBoundingClientRect();
    const x = e.clientX - r.left; const y = e.clientY - r.top;
    const j = Math.floor((x - g.L) / g.cell); const i = Math.floor((y - g.T) / g.cell);
    if (i < 0 || j < 0 || i >= g.n || j >= g.n) { tip.hidden = true; return; }
    const labels = s.short_symbols || s.symbols;
    const v = s.correlation[i][j];
    const word = i === j ? "same asset" : v >= 0.7 ? "moving together" : v >= 0.4 ? "partly linked" : "moving separately";
    tip.textContent = `${labels[i]} ↔ ${labels[j]} · ${v.toFixed(2)} · ${word}`;
    tip.hidden = false;
    tip.style.left = `${Math.max(0, Math.min(x + 14, r.width - tip.offsetWidth - 4))}px`;
    tip.style.top = `${y + 14}px`;
  }

  function renderLeaders(s) {
    const ol = $("leaderList"); ol.innerHTML = "";
    const list = s.leaders || [];
    if (!list.length) { const li = document.createElement("li"); li.textContent = "—"; ol.appendChild(li); return; }
    const max = Math.max(...list.map((x) => x.loading || 0), 1e-9);
    list.forEach((x, k) => {
      const li = document.createElement("li"); li.title = `loading on the shared move: ${fmtNum(x.loading, 3)}`;
      const rk = document.createElement("span"); rk.className = "rk"; rk.textContent = String(k + 1);
      const nm = document.createElement("span"); nm.className = "nm"; nm.textContent = x.short || x.symbol;
      const bar = document.createElement("span"); bar.className = "bar";
      const fill = document.createElement("i"); fill.style.width = `${(100 * (x.loading || 0)) / max}%`; bar.appendChild(fill);
      li.append(rk, nm, bar); ol.appendChild(li);
    });
  }

  function renderBasket(s) {
    const br = s.basket_return_60m; const el = $("basketRet");
    el.textContent = fmtSigned(br);
    el.className = br != null && br <= -0.03 ? "bad" : br != null && br <= -0.01 ? "warn" : "";
    $("avgCorr").textContent = fmtNum(s.avg_correlation);
    const dq = $("dataQuality"); const warns = s.data_warnings || []; const filled = s.filled || [];
    if (warns.length) { dq.textContent = warns.join(" · "); dq.className = "warn"; }
    else if (filled.length) { dq.textContent = `Forward-filled: ${filled.map((x) => x.replace(/USDT$/, "")).join(", ")}`; dq.className = "warn"; }
    else { dq.textContent = "All assets fresh"; dq.className = ""; }
  }

  function drawTimeline() {
    const c = setupCanvas($("timeline"));
    if (!c) return;
    const { ctx, w, h } = c;
    const pts = state.history.filter((p) => p.effective_bets != null);
    ctx.font = "12px Inter, system-ui, sans-serif"; ctx.textAlign = "left"; ctx.textBaseline = "alphabetic";
    if (pts.length < 2) { ctx.fillStyle = "#6f8278"; ctx.fillText("The timeline fills in as minutes are processed.", 12, 24); return; }
    const padL = 30; const padR = 12; const padT = 10; const padB = 22;
    const W = w - padL - padR; const H = h - padT - padB;
    const maxB = Math.max(6, Math.ceil(Math.max(...pts.map((p) => p.effective_bets))));
    const x = (i) => padL + (i / (pts.length - 1)) * W;
    const y = (b) => padT + H - ((Math.min(b, maxB) - 1) / (maxB - 1)) * H;
    pts.forEach((p, i) => {
      const x0 = x(Math.max(0, i - 0.5)); const x1 = x(Math.min(pts.length - 1, i + 0.5));
      ctx.fillStyle = BAND[p.status] || BAND.WARMING_UP; ctx.fillRect(x0, padT, Math.max(1, x1 - x0), H);
    });
    ctx.strokeStyle = "#1f3128"; ctx.lineWidth = 1; ctx.fillStyle = "#6f8278"; ctx.textAlign = "right"; ctx.textBaseline = "middle";
    const step = maxB > 8 ? 2 : 1;
    for (let b = 1; b <= maxB; b += step) {
      ctx.beginPath(); ctx.moveTo(padL, y(b)); ctx.lineTo(w - padR, y(b)); ctx.stroke(); ctx.fillText(String(b), padL - 6, y(b));
    }
    [[2.0, "#F2B134"], [1.7, "#FF6B61"]].forEach(([b, col]) => {
      ctx.save(); ctx.setLineDash([5, 4]); ctx.strokeStyle = col; ctx.beginPath(); ctx.moveTo(padL, y(b)); ctx.lineTo(w - padR, y(b)); ctx.stroke(); ctx.restore();
    });
    ctx.strokeStyle = "#e8f1ec"; ctx.lineWidth = 1.6; ctx.beginPath();
    pts.forEach((p, i) => { const px = x(i); const py = y(p.effective_bets); if (i) ctx.lineTo(px, py); else ctx.moveTo(px, py); });
    ctx.stroke();
    const last = pts[pts.length - 1];
    ctx.fillStyle = DOT[last.status] || "#e8f1ec"; ctx.beginPath(); ctx.arc(x(pts.length - 1), y(last.effective_bets), 4, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = "#6f8278"; ctx.textBaseline = "alphabetic";
    ctx.textAlign = "left"; ctx.fillText(fmtTime(pts[0].ts, false), padL, h - 5);
    ctx.textAlign = "right"; ctx.fillText(fmtTime(last.ts, false), w - padR, h - 5);
  }

  function renderEvents() {
    const ul = $("eventList"); ul.innerHTML = "";
    const evs = state.events.slice(-80).reverse();
    $("eventCount").textContent = state.events.length ? `${state.events.length} events` : "";
    if (!evs.length) { const li = document.createElement("li"); li.className = "empty"; li.textContent = "No status changes yet."; ul.appendChild(li); return; }
    evs.forEach((e) => {
      const li = document.createElement("li"); li.className = e.kind === "system" ? "system" : "status";
      const t = document.createElement("span"); t.className = "t"; t.textContent = fmtTime(e.ts, false);
      const chip = document.createElement("span"); chip.className = `chip ${e.to || ""}`;
      chip.textContent = e.to || (e.level === "warning" ? "NOTICE" : "INFO");
      const m = document.createElement("span"); m.className = "m"; m.textContent = e.message;
      li.append(t, chip, m); ul.appendChild(li);
    });
  }

  function renderTech(s) {
    const sess = state.session;
    const rows = [
      ["Absorption ratio", s ? `${fmtNum(s.absorption_ratio, 3)} — share of all movement in the strongest common factor (λ₁ / Σλ)` : "—"],
      ["Unusualness (z-score)", s ? `${fmtNum(s.z_score, 2)} — versus this basket’s previous 24 h` : "—"],
      ["Active triggers", s && s.triggers && s.triggers.length ? s.triggers.join(", ").replace(/_/g, " ") : "none"],
      ["Rules", "WARNING: z &gt; 1.5 or bets &lt; 2.0 · CONTAGION: z &gt; 2.5 or bets &lt; 1.7 · hysteresis on exit"],
      ["Window / baseline", "120 × 1-minute log returns · 1,440-minute baseline (24 h)"],
      ["Formulas", "<code>C = XᵀX / W</code> · <code>AR = λ₁ / Σλ</code> · <code>bets = exp(−Σ pᵢ ln pᵢ)</code> · <code>exposure = 1 − sigmoid((z − 2) / 0.5)</code>"],
      ["Warm-up", s && s.warmup ? `${s.warmup.have} / ${s.warmup.need} minutes` : "—"],
      ["Latest bar", s ? `opened ${fmtTime(s.bar_open)} → closed ${fmtTime(s.ts)}` : "—"],
      ["Source", s ? (s.mode === "live" ? "Binance public 1-minute klines (live)" : `Saved replay file data/${s.dataset}.json`) : "—"],
      ["Storage", sess ? (sess.storage_ok ? "SQLite — snapshots + events" : "Degraded — in memory only") : "—"],
    ];
    $("techGrid").innerHTML = rows.map(([k, v]) => `<div class="k">${k}</div><div class="v">${v}</div>`).join("");
  }

  function renderProgress(s) {
    const p = s && s.progress;
    if (!p || !p.total) { $("progressFill").style.width = "0%"; $("progressText").textContent = s ? fmtTime(s.ts) : "—"; return; }
    $("progressFill").style.width = `${Math.min(100, (100 * p.index) / p.total)}%`;
    $("progressText").textContent = `${fmtTime(s.ts)} · ${p.index}/${p.total} min`;
  }

  function renderSession() {
    const s = state.session;
    if (!s) return;
    document.querySelectorAll("#modeSwitch button").forEach((b) => b.classList.toggle("on", b.dataset.mode === s.mode));
    $("replayBar").hidden = s.mode !== "replay";
    const sel = $("datasetSelect");
    const ids = (s.datasets || []).map((d) => d.id).join(",");
    if (sel.dataset.ids !== ids) {
      sel.innerHTML = "";
      (s.datasets || []).forEach((d) => { const o = document.createElement("option"); o.value = d.id; o.textContent = d.label; sel.appendChild(o); });
      sel.dataset.ids = ids;
    }
    if (s.mode === "replay") sel.value = s.dataset;
    $("playBtn").textContent = s.playing ? "❚❚ Pause" : "▶ Play";
    $("playBtn").disabled = !!s.finished;
    $("speedChips").querySelectorAll("button").forEach((b) => b.classList.toggle("on", Number(b.dataset.speed) === s.speed));
    $("pausedBadge").hidden = !(s.mode === "replay" && !s.playing && !s.finished && state.snap);
    $("doneCard").hidden = !(s.mode === "replay" && s.finished);
    $("autopauseBtn").classList.toggle("on", !!s.autopause);
    $("autopauseBtn").setAttribute("aria-pressed", String(!!s.autopause));
    renderTech(state.snap);
  }

  function renderConn() {
    const s = state.session;
    let cls = "warn"; let text = "Connecting…";
    if (!state.wsOpen) {
      cls = state.backendDown ? "bad" : "warn";
      text = state.backendDown ? "Backend unavailable" : state.everConnected ? "Reconnecting…" : "Connecting…";
    } else if (s && s.mode === "live") {
      const map = { connected: ["ok", "Live · connected"], backfilling: ["warn", "Live · loading history"], connecting: ["warn", "Live · connecting"], reconnecting: ["bad", "Live · reconnecting"], offline: ["bad", "Live · offline"] };
      [cls, text] = map[s.feed_state] || ["warn", `Live · ${s.feed_state}`];
    } else if (s) {
      cls = "ok"; text = s.playing ? "Replay · playing" : s.finished ? "Replay · complete" : "Replay · paused";
    }
    $("connPill").className = `pill ${cls}`;
    $("connText").textContent = text;
  }

  function renderBanner() {
    const b = $("banner"); const s = state.session;
    let msg = null; let cls = "warn";
    if (!state.wsOpen && state.backendDown) {
      msg = "Backend not running. Start it with ./run.sh (or: python -m uvicorn backend.main:app --port 8000). Retrying automatically…"; cls = "crit";
    } else if (!state.wsOpen && state.everConnected) {
      msg = "Connection to the Lockstep engine lost — reconnecting…";
    } else if (s) {
      if (s.mode === "live" && (s.feed_state === "reconnecting" || s.feed_state === "offline")) msg = "Live feed interrupted — reconnecting to Binance. Replay stays available (switch to Replay).";
      else if (s.mode === "live" && (s.feed_state === "backfilling" || s.feed_state === "connecting")) { msg = "Loading the last 26 hours of 1-minute prices from Binance…"; cls = "info"; }
      else if (s.mode === "replay" && s.feed_state === "offline") { msg = "Replay data missing. Run: python -m backend.fetch_replay"; cls = "crit"; }
      if (!s.storage_ok) msg = `${msg ? `${msg} ` : ""}Storage degraded — events are kept in memory only.`;
    }
    b.hidden = !msg;
    if (msg) { b.textContent = msg; b.className = `banner ${cls}`; }
  }

  /* ---------------- controls ---------------- */
  async function post(path, body) {
    try {
      const r = await fetch(`${HTTP_BASE}${path}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      if (!r.ok) {
        let detail = "";
        try { detail = (await r.json()).detail; } catch (e) { detail = ""; }
        toast(detail ? `Backend: ${detail}` : `Backend error ${r.status}`);
        return;
      }
      state.session = await r.json();
      renderSession(); renderConn();
    } catch (e) {
      toast("Couldn't reach the Lockstep backend.");
    }
  }

  async function runSelfCheck() {
    const out = $("selfCheckOut"); const btn = $("selfCheckBtn");
    btn.disabled = true;
    out.textContent = "Running the same engine over every saved dataset… (about 10–30 s the first time)";
    try {
      const r = await fetch(`${HTTP_BASE}/api/validation`);
      if (!r.ok) throw new Error(String(r.status));
      const rows = await r.json();
      const head = "<tr><th>Dataset</th><th>Outcome</th><th>Lead</th><th>WARNING episodes</th><th>CONTAGION episodes</th><th>Worst 60-min basket</th></tr>";
      const body = rows.map((x) => {
        const lead = x.lead_minutes == null ? "—" : x.lead_minutes > 0 ? `${x.lead_minutes} min before` : `${Math.abs(x.lead_minutes)} min after`;
        const outcome = x.error ? x.error : x.role === "calm" ? "false-alarm check" : (x.outcome || "—");
        return `<tr><td>${x.label}</td><td>${outcome}</td><td>${lead}</td><td>${x.warning_episodes ?? "—"}</td><td>${x.contagion_episodes ?? "—"}</td><td>${fmtSigned(x.worst_60m ?? null)}</td></tr>`;
      }).join("");
      out.innerHTML = `<table>${head}${body}</table><p class="note">Lead = crash start (first minute the equal-weight basket is down 3% over 60 minutes) minus the first WARNING. Small sample; the bets floor was added after seeing May 2021.</p>`;
    } catch (e) {
      out.textContent = "Self-check failed — is the backend running? Check the server terminal.";
    } finally {
      btn.disabled = false;
    }
  }

  function bindControls() {
    SPEEDS.forEach((sp) => {
      const b = document.createElement("button");
      b.type = "button"; b.dataset.speed = String(sp); b.textContent = `${sp}×`;
      b.onclick = () => post("/api/replay", { action: "speed", speed: sp });
      $("speedChips").appendChild(b);
    });
    $("playBtn").onclick = () => post("/api/replay", { action: state.session && state.session.playing ? "pause" : "play" });
    $("restartBtn").onclick = () => post("/api/replay", { action: "restart" });
    $("doneRestart").onclick = () => post("/api/replay", { action: "restart" });
    $("autopauseBtn").onclick = () => post("/api/replay", { action: "autopause", enabled: !(state.session && state.session.autopause) });
    $("datasetSelect").onchange = (e) => post("/api/replay", { action: "load", dataset: e.target.value });
    document.querySelectorAll("#modeSwitch button").forEach((b) => {
      b.onclick = () => { if (!state.session || state.session.mode !== b.dataset.mode) post("/api/mode", { mode: b.dataset.mode }); };
    });
    document.addEventListener("keydown", (e) => {
      const tag = document.activeElement ? document.activeElement.tagName : "";
      if (["INPUT", "SELECT", "TEXTAREA", "BUTTON", "SUMMARY"].includes(tag)) return;
      if (!state.session || state.session.mode !== "replay") return;
      if (e.code === "Space") { e.preventDefault(); $("playBtn").click(); }
      else if (e.key === "r" || e.key === "R") { $("restartBtn").click(); }
    });
    $("heatmap").addEventListener("mousemove", onHeatMove);
    $("heatmap").addEventListener("mouseleave", () => { $("heatTip").hidden = true; });
    $("selfCheckBtn").onclick = runSelfCheck;
    let raf = 0;
    window.addEventListener("resize", () => { cancelAnimationFrame(raf); raf = requestAnimationFrame(() => { drawHeatmap(); drawTimeline(); }); });
  }

  bindControls();
  renderConn();
  connect();
})();