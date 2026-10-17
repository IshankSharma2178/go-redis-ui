/**
 * go-redis UI — Application Controller & Interactive Playground
 * Inspired by px0.ai zero-latency developer experience
 */

(function () {
  "use strict";

  // =========================================================================
  // 1. Redis Command Definitions
  // =========================================================================
  const COMMANDS = [
    {
      name: "PING",
      tag: "server",
      category: "Server & Status",
      desc: "Check server vitality. Returns PONG or echoes the passed message.",
      example: "PING",
      complexity: "O(1)",
    },
    {
      name: "SET",
      tag: "string",
      category: "Strings",
      desc: "Store a string value at key. Supports EX (seconds) for auto-expiry.",
      example: 'SET user:1001 "Alice Vance" EX 120',
      complexity: "O(1)",
    },
    {
      name: "GET",
      tag: "string",
      category: "Strings",
      desc: "Retrieve the string value associated with key. Returns (nil) if absent.",
      example: "GET user:1001",
      complexity: "O(1)",
    },
    {
      name: "INCR",
      tag: "string",
      category: "Strings",
      desc: "Atomically increments number stored at key by 1. Initializes to 0 if missing.",
      example: "INCR metrics:pageviews",
      complexity: "O(1)",
    },
    {
      name: "TTL",
      tag: "key",
      category: "Keys & TTL",
      desc: "Returns remaining seconds until key expiration. -1 if persistent, -2 if missing.",
      example: "TTL user:1001",
      complexity: "O(1)",
    },
    {
      name: "EXPIRE",
      tag: "key",
      category: "Keys & TTL",
      desc: "Sets a timeout on key in seconds. Returns 1 if timeout set, 0 if key not found.",
      example: "EXPIRE metrics:pageviews 60",
      complexity: "O(1)",
    },
    {
      name: "DEL",
      tag: "key",
      category: "Keys & TTL",
      desc: "Removes specified key(s). Returns count of keys removed.",
      example: "DEL user:1001",
      complexity: "O(N)",
    },
    {
      name: "INFO",
      tag: "server",
      category: "Server & Status",
      desc: "Returns keyspace stats, memory usage, connected clients, and uptime metrics.",
      example: "INFO",
      complexity: "O(1)",
    },
    {
      name: "MULTI / EXEC",
      tag: "transaction",
      category: "Transactions",
      desc: "Marks start of atomic transaction block. Queues commands until EXEC.",
      example: "MULTI\nSET acc:1 500\nINCR acc:1\nEXEC",
      complexity: "O(N)",
    },
    {
      name: "DISCARD",
      tag: "transaction",
      category: "Transactions",
      desc: "Flushes all queued commands in a transaction and returns connection to normal state.",
      example: "DISCARD",
      complexity: "O(1)",
    },
    {
      name: "BGREWRITEAOF",
      tag: "persist",
      category: "Storage & Persistence",
      desc: "Asynchronously rewrite Append-Only File (AOF) log for durable crash recovery.",
      example: "BGREWRITEAOF",
      complexity: "O(N)",
    },
    {
      name: "LRU",
      tag: "evict",
      category: "Memory & Eviction",
      desc: "Triggers immediate allkeys-LRU eviction pass based on least recently accessed keys.",
      example: "LRU",
      complexity: "O(1)",
    },
    {
      name: "SLEEP",
      tag: "debug",
      category: "Server & Status",
      desc: "Blocks execution for specified seconds. Ideal for timeout & concurrency testing.",
      example: "SLEEP 1",
      complexity: "O(1)",
    },
  ];

  // =========================================================================
  // 2. In-Browser Simulated Redis Engine (Zero-Friction Fallback)
  // Ensures UI remains 100% interactive even if external proxy is asleep
  // =========================================================================
  class LocalRedisEngine {
    constructor() {
      this.store = new Map(); // key -> { val, expireAt, lastAccess }
      this.inTx = false;
      this.txQueue = [];
      this.totalCommands = 48;
      this.startTime = Date.now();
      this.seedInitialData();
    }

    seedInitialData() {
      const now = Date.now();
      this.store.set("cluster:name", { val: "go-redis-node-01", expireAt: null, lastAccess: now });
      this.store.set("metrics:qps", { val: "14820", expireAt: null, lastAccess: now });
      this.store.set("session:token", { val: "tok_8f9c2e11d0", expireAt: now + 90000, lastAccess: now });
      this.store.set("rate:limit:ip", { val: "42", expireAt: now + 35000, lastAccess: now });
    }

    cleanExpired() {
      const now = Date.now();
      for (const [k, v] of this.store.entries()) {
        if (v.expireAt && v.expireAt <= now) {
          this.store.delete(k);
        }
      }
    }

    async exec(args) {
      this.cleanExpired();
      this.totalCommands++;
      if (!args || args.length === 0) return null;

      const cmd = String(args[0]).toUpperCase();

      if (this.inTx && cmd !== "EXEC" && cmd !== "DISCARD") {
        this.txQueue.push(args);
        return "QUEUED";
      }

      switch (cmd) {
        case "PING":
          return args[1] ? args.slice(1).join(" ") : "PONG";

        case "SET": {
          const key = args[1];
          const val = args[2] !== undefined ? args[2] : "";
          let expireAt = null;
          if (args[3] && args[3].toUpperCase() === "EX" && args[4]) {
            const sec = parseInt(args[4], 10);
            if (!isNaN(sec)) expireAt = Date.now() + sec * 1000;
          }
          this.store.set(key, { val, expireAt, lastAccess: Date.now() });
          return "OK";
        }

        case "GET": {
          const key = args[1];
          const entry = this.store.get(key);
          if (!entry) return null;
          entry.lastAccess = Date.now();
          return entry.val;
        }

        case "INCR": {
          const key = args[1];
          const entry = this.store.get(key);
          let num = 0;
          let exp = null;
          if (entry) {
            num = parseInt(entry.val, 10);
            if (isNaN(num)) throw new Error("ERR value is not an integer or out of range");
            exp = entry.expireAt;
          }
          num += 1;
          this.store.set(key, { val: String(num), expireAt: exp, lastAccess: Date.now() });
          return num;
        }

        case "TTL": {
          const key = args[1];
          const entry = this.store.get(key);
          if (!entry) return -2;
          if (!entry.expireAt) return -1;
          const rem = Math.ceil((entry.expireAt - Date.now()) / 1000);
          return rem > 0 ? rem : -2;
        }

        case "EXPIRE": {
          const key = args[1];
          const sec = parseInt(args[2], 10);
          const entry = this.store.get(key);
          if (!entry || isNaN(sec)) return 0;
          entry.expireAt = Date.now() + sec * 1000;
          return 1;
        }

        case "DEL": {
          let count = 0;
          for (let i = 1; i < args.length; i++) {
            if (this.store.delete(args[i])) count++;
          }
          return count;
        }

        case "MULTI":
          if (this.inTx) throw new Error("ERR MULTI calls can not be nested");
          this.inTx = true;
          this.txQueue = [];
          return "OK";

        case "EXEC": {
          if (!this.inTx) throw new Error("ERR EXEC without MULTI");
          this.inTx = false;
          const queue = this.txQueue;
          this.txQueue = [];
          const results = [];
          for (const item of queue) {
            try {
              results.push(await this.exec(item));
            } catch (err) {
              results.push(err.message);
            }
          }
          return results;
        }

        case "DISCARD":
          if (!this.inTx) throw new Error("ERR DISCARD without MULTI");
          this.inTx = false;
          this.txQueue = [];
          return "OK";

        case "INFO": {
          const uptime = Math.floor((Date.now() - this.startTime) / 1000);
          return [
            "# Server",
            "redis_version:1.2.0-pure-go",
            "os:linux-amd64",
            "arch_bits:64",
            "process_id:418",
            `uptime_in_seconds:${uptime}`,
            `total_commands_processed:${this.totalCommands}`,
            "# Memory",
            "used_memory:4194304",
            "used_memory_human:4.00M",
            "mem_allocator:go_runtime_gc",
            "# Keyspace",
            `db0:keys=${this.store.size},expires=${Array.from(this.store.values()).filter(v => v.expireAt).length}`,
          ].join("\r\n");
        }

        case "BGREWRITEAOF":
          return "Background append only file rewriting started";

        case "LRU": {
          if (this.store.size === 0) return "OK (0 keys evicted)";
          let oldestKey = null;
          let oldestTime = Infinity;
          for (const [k, v] of this.store.entries()) {
            if (v.lastAccess < oldestTime) {
              oldestTime = v.lastAccess;
              oldestKey = k;
            }
          }
          if (oldestKey) this.store.delete(oldestKey);
          return `OK (evicted least recently used key '${oldestKey}')`;
        }

        case "SLEEP": {
          const sec = parseFloat(args[1]) || 1;
          await new Promise((r) => setTimeout(r, Math.min(sec, 5) * 1000));
          return `OK (slept ${sec}s)`;
        }

        default:
          throw new Error(`ERR unknown command '${cmd}'`);
      }
    }

    getKeysSummary() {
      this.cleanExpired();
      const list = [];
      const now = Date.now();
      for (const [key, data] of this.store.entries()) {
        const ttl = data.expireAt ? Math.max(0, Math.ceil((data.expireAt - now) / 1000)) : -1;
        list.push({
          key,
          val: data.val,
          ttl,
          type: "string",
        });
      }
      return list;
    }
  }

  const localEngine = new LocalRedisEngine();

  // =========================================================================
  // 3. Command Line Parser (Supports Quoting & Multiple Lines)
  // =========================================================================
  function parseArgs(str) {
    const tokens = [];
    let cur = "";
    let inQuote = null;
    for (let i = 0; i < str.length; i++) {
      const c = str[i];
      if (inQuote) {
        if (c === inQuote) inQuote = null;
        else cur += c;
      } else if (c === '"' || c === "'") {
        inQuote = c;
      } else if (/\s/.test(c)) {
        if (cur) {
          tokens.push(cur);
          cur = "";
        }
      } else {
        cur += c;
      }
    }
    if (cur) tokens.push(cur);
    return tokens;
  }

  // =========================================================================
  // 4. API Client with Hybrid Bridge/Local Fallback
  // =========================================================================
  let engineMode = "auto"; // "auto" | "bridge" | "local"
  let isBridgeConnected = false;

  async function executeCommand(cmdInput) {
    const startMs = performance.now();

    // Check multi-line input
    const isMultiLine = typeof cmdInput === "string" && cmdInput.trim().includes("\n");

    if (engineMode === "local") {
      return executeLocally(cmdInput, startMs);
    }

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 3500);

      const res = await fetch("/api/command", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ command: cmdInput }),
        signal: controller.signal,
      });
      clearTimeout(timeoutId);

      const data = await res.json();
      const tookMs = (performance.now() - startMs).toFixed(2);

      if (res.ok && !data.error) {
        isBridgeConnected = true;
        updateStatus(true);
        try {
          const args = Array.isArray(cmdInput) ? cmdInput : parseArgs(typeof cmdInput === 'string' ? cmdInput : '');
          if (args.length) {
            const op = args[0].toUpperCase();
            if (op === 'SET') {
              const k = args[1];
              const v = args[2] || '';
              let exp = null;
              if (args[3] && args[3].toUpperCase() === 'EX' && args[4]) {
                exp = Date.now() + parseInt(args[4], 10) * 1000;
              }
              localEngine.store.set(k, { val: v, expireAt: exp, lastAccess: Date.now() });
            } else if (op === 'DEL') {
              for (let i = 1; i < args.length; i++) localEngine.store.delete(args[i]);
            } else if (op === 'INCR') {
              const k = args[1];
              const cur = localEngine.store.get(k);
              const num = cur ? (parseInt(cur.val, 10) || 0) + 1 : 1;
              localEngine.store.set(k, { val: String(num), expireAt: cur ? cur.expireAt : null, lastAccess: Date.now() });
            }
          }
        } catch (_) {}
        return { result: data.result, latency: tookMs, source: "live-tcp" };
      }

      if (engineMode === "auto" && (res.status === 502 || res.status === 504 || data.error)) {
        // Fall back gracefully to local engine for zero downtime
        const localRes = await executeLocally(cmdInput, startMs);
        localRes.warning = data.error || "TCP bridge unreachable; simulated locally";
        return localRes;
      }

      return { error: data.error || "Execution failed", latency: tookMs, source: "bridge" };
    } catch (err) {
      if (engineMode === "auto") {
        const localRes = await executeLocally(cmdInput, startMs);
        localRes.warning = "Live bridge offline; executed via in-browser engine";
        return localRes;
      }
      return { error: err.message, latency: (performance.now() - startMs).toFixed(2), source: "error" };
    }
  }

  async function executeLocally(cmdInput, startMs) {
    const raw = typeof cmdInput === "string" ? cmdInput.trim() : "";
    if (raw.includes("\n")) {
      const lines = raw.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
      const results = [];
      for (const line of lines) {
        const args = parseArgs(line);
        if (args.length) {
          try {
            results.push(await localEngine.exec(args));
          } catch (e) {
            results.push(`ERR: ${e.message}`);
          }
        }
      }
      return {
        result: results,
        latency: (performance.now() - startMs).toFixed(2),
        source: "local-engine",
      };
    }

    const args = Array.isArray(cmdInput) ? cmdInput : parseArgs(cmdInput);
    if (!args.length) throw new Error("Empty command");
    try {
      const val = await localEngine.exec(args);
      return {
        result: val,
        latency: (performance.now() - startMs).toFixed(2),
        source: "local-engine",
      };
    } catch (e) {
      return {
        error: e.message,
        latency: (performance.now() - startMs).toFixed(2),
        source: "local-engine",
      };
    }
  }

  // =========================================================================
  // 5. RESP Result Formatter
  // =========================================================================
  function formatResp(val) {
    if (val === null || val === undefined) {
      return { text: "(nil)", type: "nil" };
    }
    if (typeof val === "number") {
      return { text: `(integer) ${val}`, type: "num" };
    }
    if (typeof val === "boolean") {
      return { text: val ? "(integer) 1" : "(integer) 0", type: "num" };
    }
    if (Array.isArray(val)) {
      if (val.length === 0) return { text: "(empty array)", type: "nil" };
      const lines = val.map((item, idx) => {
        if (item === null || item === undefined) return `${idx + 1}) (nil)`;
        if (Array.isArray(item)) return `${idx + 1}) [array of ${item.length} items]`;
        return `${idx + 1}) "${item}"`;
      });
      return { text: lines.join("\n"), type: "ok" };
    }
    const str = String(val);
    if (str.startsWith("ERR") || str.startsWith("WRONGTYPE")) {
      return { text: str, type: "err" };
    }
    return { text: str, type: "ok" };
  }

  // =========================================================================
  // 6. Terminal REPL & Studio Controller
  // =========================================================================
  const state = {
    history: [],
    historyIdx: -1,
  };

  const el = {
    consoleOutput: document.getElementById("consoleOutput"),
    consoleInput: document.getElementById("consoleInput"),
    consoleRunBtn: document.getElementById("consoleRunBtn"),
    customTextarea: document.getElementById("customTextarea"),
    runCustomBtn: document.getElementById("runCustomBtn"),
    statusDot: document.getElementById("statusDot"),
    statusText: document.getElementById("statusText"),
    engineModeBadge: document.getElementById("engineModeBadge"),
    sidebarTree: document.getElementById("sidebarTree"),
    sidebarSearch: document.getElementById("sidebarSearch"),
    keysList: document.getElementById("keysList"),
    keysCount: document.getElementById("keysCount"),
    commandsGrid: document.getElementById("commandsGrid"),
    searchModal: document.getElementById("searchModal"),
    searchInput: document.getElementById("searchInput"),
    searchResults: document.getElementById("searchResults"),
    themeToggle: document.getElementById("themeToggle"),
    scrollProgress: document.querySelector(".nav-progress"),
  };

  function appendReplEntry(cmd, response) {
    if (!el.consoleOutput) return;

    const entry = document.createElement("div");
    entry.className = "repl-entry";

    const promptLine = document.createElement("div");
    promptLine.className = "repl-prompt-line";
    promptLine.innerHTML = `<span class="prompt-sign">127.0.0.1:6379&gt;</span> <span class="repl-prompt-cmd">${escapeHtml(cmd)}</span>`;

    const resultLine = document.createElement("div");
    if (response.error) {
      resultLine.className = "repl-result-line err";
      resultLine.textContent = `(error) ${response.error}`;
    } else {
      const f = formatResp(response.result);
      resultLine.className = `repl-result-line ${f.type}`;
      resultLine.textContent = f.text;
    }

    const metaLine = document.createElement("div");
    metaLine.className = "repl-meta";
    metaLine.textContent = `took ${response.latency || "0.2"}ms · ${response.source || "engine"}`;
    if (response.warning) {
      metaLine.textContent += ` [${response.warning}]`;
    }

    entry.appendChild(promptLine);
    entry.appendChild(resultLine);
    entry.appendChild(metaLine);

    el.consoleOutput.appendChild(entry);
    el.consoleOutput.scrollTop = el.consoleOutput.scrollHeight;

    // Refresh keys view
    refreshKeyspace();
  }

  async function handleConsoleSubmit() {
    if (!el.consoleInput) return;
    const cmd = el.consoleInput.value.trim();
    if (!cmd) return;

    el.consoleRunBtn.disabled = true;
    state.history.push(cmd);
    state.historyIdx = state.history.length;

    try {
      const response = await executeCommand(cmd);
      appendReplEntry(cmd, response);
      el.consoleInput.value = "";
    } finally {
      el.consoleRunBtn.disabled = false;
      el.consoleInput.focus();
    }
  }

  // =========================================================================
  // 7. Command Sidebar Tree
  // =========================================================================
  function renderSidebarTree(filter = "") {
    if (!el.sidebarTree) return;
    el.sidebarTree.innerHTML = "";

    const query = filter.toLowerCase().trim();
    const categories = {};

    COMMANDS.forEach((c) => {
      if (query && !c.name.toLowerCase().includes(query) && !c.desc.toLowerCase().includes(query)) {
        return;
      }
      if (!categories[c.category]) categories[c.category] = [];
      categories[c.category].push(c);
    });

    Object.keys(categories).forEach((cat) => {
      const groupTitle = document.createElement("li");
      groupTitle.className = "tree-group-title";
      groupTitle.textContent = cat;
      el.sidebarTree.appendChild(groupTitle);

      categories[cat].forEach((c) => {
        const li = document.createElement("li");
        li.className = "tree-cmd-item";
        li.innerHTML = `
          <span class="tree-cmd-name">${c.name}</span>
          <span class="tree-cmd-badge">${c.tag}</span>
        `;
        li.addEventListener("click", () => {
          document.querySelectorAll(".tree-cmd-item").forEach((item) => item.classList.remove("active"));
          li.classList.add("active");
          loadIntoConsole(c.example);
        });
        el.sidebarTree.appendChild(li);
      });
    });
  }

  function loadIntoConsole(cmd) {
    if (el.consoleInput) {
      el.consoleInput.value = cmd;
      el.consoleInput.focus();
      // Switch to REPL tab if on multiline
      switchCenterTab("repl");
    }
  }

  // =========================================================================
  // 8. Keyspace Inspector
  // =========================================================================
  function refreshKeyspace() {
    if (!el.keysList) return;
    const keys = localEngine.getKeysSummary();
    if (el.keysCount) el.keysCount.textContent = keys.length;

    el.keysList.innerHTML = "";
    if (keys.length === 0) {
      el.keysList.innerHTML = `<li class="empty-keys-note">// No keys in DB 0<br><span style="color:var(--amber)">Run SET key val</span></li>`;
      return;
    }

    keys.forEach((k) => {
      const li = document.createElement("li");
      li.className = "key-card";
      li.innerHTML = `
        <div class="key-card-head">
          <span class="key-name">${escapeHtml(k.key)}</span>
          <span class="key-ttl">${k.ttl === -1 ? "persistent" : k.ttl + "s"}</span>
        </div>
        <div class="key-val-preview">${escapeHtml(String(k.val))}</div>
      `;
      li.addEventListener("click", () => {
        loadIntoConsole(`GET ${k.key}`);
      });
      el.keysList.appendChild(li);
    });
  }

  // =========================================================================
  // 9. Commands Matrix (Section 01)
  // =========================================================================
  function renderCommandsGrid(tagFilter = "all") {
    if (!el.commandsGrid) return;
    el.commandsGrid.innerHTML = "";

    const list = tagFilter === "all" ? COMMANDS : COMMANDS.filter((c) => c.tag === tagFilter);

    list.forEach((c) => {
      const card = document.createElement("article");
      card.className = "cmd-card";
      card.innerHTML = `
        <div class="cmd-card-top">
          <span class="cmd-card-name">${c.name}</span>
          <span class="cmd-card-tag">${c.tag} · ${c.complexity}</span>
        </div>
        <p class="cmd-card-desc">${c.desc}</p>
        <div class="cmd-card-example" title="Click to load into console">${escapeHtml(c.example)}</div>
        <div class="cmd-card-actions">
          <button class="cmd-run-btn" type="button">Run in Console</button>
        </div>
        <div class="cmd-card-res"></div>
      `;

      const ex = card.querySelector(".cmd-card-example");
      ex.addEventListener("click", () => {
        loadIntoConsole(c.example);
        window.location.hash = "stage";
      });

      const runBtn = card.querySelector(".cmd-run-btn");
      const resBox = card.querySelector(".cmd-card-res");

      runBtn.addEventListener("click", async () => {
        runBtn.disabled = true;
        runBtn.innerHTML = '<span class="spinner"></span> Running...';
        resBox.className = "cmd-card-res";

        try {
          const resp = await executeCommand(c.example);
          resBox.classList.add("show");
          if (resp.error) {
            resBox.classList.add("err");
            resBox.textContent = `(error) ${resp.error}`;
          } else {
            const f = formatResp(resp.result);
            resBox.classList.add(f.type);
            resBox.textContent = f.text;
          }
        } catch (err) {
          resBox.classList.add("show", "err");
          resBox.textContent = err.message;
        } finally {
          runBtn.disabled = false;
          runBtn.textContent = "Run in Console";
        }
      });

      el.commandsGrid.appendChild(card);
    });
  }

  // =========================================================================
  // 10. Command Palette Modal (Ctrl+K / Cmd+K)
  // =========================================================================
  let selectedPaletteIndex = 0;
  let paletteItems = [];

  function openSearchModal() {
    if (!el.searchModal) return;
    el.searchModal.hidden = false;
    if (el.searchInput) {
      el.searchInput.value = "";
      el.searchInput.focus();
    }
    renderPaletteResults("");
  }

  function closeSearchModal() {
    if (!el.searchModal) return;
    el.searchModal.hidden = true;
  }

  function renderPaletteResults(query) {
    if (!el.searchResults) return;
    const q = query.toLowerCase().trim();

    paletteItems = [];

    // Commands
    COMMANDS.forEach((c) => {
      if (!q || c.name.toLowerCase().includes(q) || c.desc.toLowerCase().includes(q)) {
        paletteItems.push({
          title: c.name,
          desc: c.desc,
          badge: c.tag,
          action: () => {
            loadIntoConsole(c.example);
            window.location.hash = "stage";
          },
        });
      }
    });

    // Quick Actions
    const quickActions = [
      {
        title: "FLUSHDB (Clear Keys)",
        desc: "Clear in-memory keys and reset keyspace",
        badge: "action",
        action: () => {
          localEngine.store.clear();
          refreshKeyspace();
          appendReplEntry("FLUSHDB", { result: "OK", latency: "0.1", source: "local" });
        },
      },
      {
        title: "Seed Demo Dataset",
        desc: "Populate cluster with sample users, session tokens and metrics",
        badge: "action",
        action: () => {
          localEngine.seedInitialData();
          refreshKeyspace();
          appendReplEntry("SEED", { result: "OK (seeded 4 keys)", latency: "0.1", source: "local" });
        },
      },
      {
        title: "Toggle Light / Dark Theme",
        desc: "Switch between void dark and clean light contrast modes",
        badge: "theme",
        action: toggleTheme,
      },
      {
        title: "Clear REPL Console",
        desc: "Empty terminal scrollback buffer",
        badge: "console",
        action: clearConsole,
      },
    ];

    quickActions.forEach((a) => {
      if (!q || a.title.toLowerCase().includes(q) || a.desc.toLowerCase().includes(q)) {
        paletteItems.push(a);
      }
    });

    selectedPaletteIndex = 0;
    el.searchResults.innerHTML = "";

    paletteItems.forEach((item, idx) => {
      const li = document.createElement("li");
      li.className = `search-item ${idx === 0 ? "selected" : ""}`;
      li.innerHTML = `
        <div class="search-item-left">
          <span class="search-item-cmd">${escapeHtml(item.title)}</span>
          <span class="search-item-desc">${escapeHtml(item.desc)}</span>
        </div>
        <span class="search-item-tag">${item.badge}</span>
      `;
      li.addEventListener("click", () => {
        closeSearchModal();
        item.action();
      });
      el.searchResults.appendChild(li);
    });
  }

  function handlePaletteKeyDown(e) {
    if (el.searchModal.hidden) return;
    const items = el.searchResults.querySelectorAll(".search-item");
    if (!items.length) return;

    if (e.key === "ArrowDown") {
      e.preventDefault();
      items[selectedPaletteIndex]?.classList.remove("selected");
      selectedPaletteIndex = (selectedPaletteIndex + 1) % items.length;
      items[selectedPaletteIndex]?.classList.add("selected");
      items[selectedPaletteIndex]?.scrollIntoView({ block: "nearest" });
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      items[selectedPaletteIndex]?.classList.remove("selected");
      selectedPaletteIndex = (selectedPaletteIndex - 1 + items.length) % items.length;
      items[selectedPaletteIndex]?.classList.add("selected");
      items[selectedPaletteIndex]?.scrollIntoView({ block: "nearest" });
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (paletteItems[selectedPaletteIndex]) {
        closeSearchModal();
        paletteItems[selectedPaletteIndex].action();
      }
    } else if (e.key === "Escape") {
      closeSearchModal();
    }
  }

  // =========================================================================
  // 11. Theme & UI Helpers
  // =========================================================================
  function initTheme() {
    const saved = localStorage.getItem("go-redis-theme") || "dark";
    document.documentElement.setAttribute("data-theme", saved);
  }

  function toggleTheme() {
    const current = document.documentElement.getAttribute("data-theme") || "dark";
    const next = current === "dark" ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", next);
    localStorage.setItem("go-redis-theme", next);
  }

  function updateStatus(isOk) {
    if (!el.statusDot || !el.statusText) return;
    if (isOk) {
      el.statusDot.className = "status-dot ok";
      el.statusText.textContent = "CONNECTED (TCP)";
    } else {
      el.statusDot.className = "status-dot";
      el.statusText.textContent = "SIMULATOR (LOCAL)";
    }
  }

  function clearConsole() {
    if (!el.consoleOutput) return;
    el.consoleOutput.innerHTML = `
      <div class="console-welcome">
        <span class="app-title">go-redis</span> <span class="app-acc">v1.2.0</span> · console cleared<br>
        <span class="app-dim">Type any Redis command (e.g. SET, GET, INCR, MULTI) and press Enter</span>
      </div>
    `;
  }

  function switchCenterTab(tabName) {
    document.querySelectorAll(".center-tab").forEach((t) => t.classList.remove("active"));
    const replBox = document.getElementById("replView");
    const multilineBox = document.getElementById("multilineView");

    if (tabName === "repl") {
      document.getElementById("tabRepl")?.classList.add("active");
      if (replBox) replBox.style.display = "flex";
      if (multilineBox) multilineBox.classList.remove("active");
    } else {
      document.getElementById("tabMultiline")?.classList.add("active");
      if (replBox) replBox.style.display = "none";
      if (multilineBox) multilineBox.classList.add("active");
    }
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  // Check backend server health
  async function checkHealth() {
    try {
      const res = await fetch("/api/health");
      const data = await res.json();
      if (data.status === "ok") {
        isBridgeConnected = true;
        updateStatus(true);
      } else {
        isBridgeConnected = false;
        updateStatus(false);
      }
    } catch {
      isBridgeConnected = false;
      updateStatus(false);
    }
  }

  // =========================================================================
  // 12. Initialization & Event Binding
  // =========================================================================
  function init() {
    initTheme();

    // Check health
    checkHealth();
    setInterval(checkHealth, 15000);

    // Initial renders
    renderSidebarTree();
    refreshKeyspace();
    renderCommandsGrid("all");

    // Scroll progress listener
    window.addEventListener(
      "scroll",
      () => {
        const total = document.documentElement.scrollHeight - window.innerHeight;
        if (el.scrollProgress && total > 0) {
          const ratio = Math.min(window.scrollY / total, 1);
          el.scrollProgress.style.transform = `scaleX(${ratio})`;
        }
      },
      { passive: true }
    );

    // Console Input key handling
    el.consoleInput?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        handleConsoleSubmit();
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        if (state.history.length && state.historyIdx > 0) {
          state.historyIdx--;
          el.consoleInput.value = state.history[state.historyIdx] || "";
        }
      } else if (e.key === "ArrowDown") {
        e.preventDefault();
        if (state.historyIdx < state.history.length - 1) {
          state.historyIdx++;
          el.consoleInput.value = state.history[state.historyIdx] || "";
        } else {
          state.historyIdx = state.history.length;
          el.consoleInput.value = "";
        }
      }
    });

    el.consoleRunBtn?.addEventListener("click", handleConsoleSubmit);

    // Multi-line runner
    el.runCustomBtn?.addEventListener("click", async () => {
      const text = el.customTextarea?.value.trim();
      if (!text) return;
      el.runCustomBtn.disabled = true;
      try {
        const resp = await executeCommand(text);
        appendReplEntry(text, resp);
        switchCenterTab("repl");
      } finally {
        el.runCustomBtn.disabled = false;
      }
    });

    // Sidebar search input
    el.sidebarSearch?.addEventListener("input", (e) => {
      renderSidebarTree(e.target.value);
    });

    // Center tab switches
    document.getElementById("tabRepl")?.addEventListener("click", () => switchCenterTab("repl"));
    document.getElementById("tabMultiline")?.addEventListener("click", () => switchCenterTab("multiline"));

    // Filter pills in Section 01
    document.querySelectorAll(".cmd-filter-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        document.querySelectorAll(".cmd-filter-btn").forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        renderCommandsGrid(btn.dataset.tag || "all");
      });
    });

    // Theme toggle
    el.themeToggle?.addEventListener("click", toggleTheme);

    // Command palette triggers
    document.querySelectorAll("[data-open-palette]").forEach((b) => {
      b.addEventListener("click", openSearchModal);
    });

    document.getElementById("searchCloseBtn")?.addEventListener("click", closeSearchModal);
    document.getElementById("searchBackdrop")?.addEventListener("click", closeSearchModal);

    el.searchInput?.addEventListener("input", (e) => {
      renderPaletteResults(e.target.value);
    });

    window.addEventListener("keydown", (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        if (el.searchModal.hidden) openSearchModal();
        else closeSearchModal();
      } else if (!el.searchModal.hidden) {
        handlePaletteKeyDown(e);
      }
    });

    // Copy line button
    const copyBtn = document.querySelector(".copyline");
    copyBtn?.addEventListener("click", () => {
      const text = copyBtn.dataset.copy || "docker run -d -p 6379:6379 ishanksharma16/go-redis";
      navigator.clipboard.writeText(text).then(() => {
        copyBtn.classList.add("copied");
        setTimeout(() => copyBtn.classList.remove("copied"), 2200);
      });
    });

    // Replay/Clear button
    document.getElementById("stageReplayBtn")?.addEventListener("click", () => {
      clearConsole();
      appendReplEntry("INFO", {
        result: localEngine.exec(["INFO"]),
        latency: "0.08",
        source: "engine",
      });
    });

    // Inspector refresh button
    document.getElementById("refreshKeysBtn")?.addEventListener("click", refreshKeyspace);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
