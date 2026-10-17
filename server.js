const express = require("express");
const cors = require("cors");
const net = require("net");
const path = require("path");

const HOST = process.env.REDIS_HOST || "iriguchi.proxy.rlwy.net";
const PORT = parseInt(process.env.REDIS_PORT || "55254", 10);
const HTTP_PORT = parseInt(process.env.HTTP_PORT || "8080", 10);
const TIMEOUT_MS = parseInt(process.env.REDIS_TIMEOUT || "3000", 10);

// ---------- RESP encoder ----------

function encodeCommand(args) {
  let buf = `*${args.length}\r\n`;
  for (const a of args) {
    const s = String(a);
    buf += `$${Buffer.byteLength(s, "utf8")}\r\n${s}\r\n`;
  }
  return Buffer.from(buf, "utf8");
}

// ---------- RESP parser (incremental) ----------

class RespParser {
  constructor() {
    this.buf = Buffer.alloc(0);
  }

  feed(chunk) {
    this.buf = Buffer.concat([this.buf, chunk]);
  }

  parse() {
    const r = this._parseValue(this.buf, 0);
    if (r === null) return { done: false };
    this.buf = this.buf.slice(r.pos);
    return { done: true, value: r.value, error: r.error };
  }

  _parseValue(buf, start) {
    if (start >= buf.length) return null;
    const type = String.fromCharCode(buf[start]);
    const lineEnd = buf.indexOf("\r\n", start);
    if (lineEnd === -1) return null;

    switch (type) {
      case "+":
        return { value: buf.toString("utf8", start + 1, lineEnd), pos: lineEnd + 2 };
      case "-":
        return { value: buf.toString("utf8", start + 1, lineEnd), error: true, pos: lineEnd + 2 };
      case ":":
        return { value: parseInt(buf.toString("utf8", start + 1, lineEnd), 10), pos: lineEnd + 2 };
      case "$": {
        const len = parseInt(buf.toString("utf8", start + 1, lineEnd), 10);
        if (len === -1) return { value: null, pos: lineEnd + 2 };
        const dataStart = lineEnd + 2;
        if (buf.length < dataStart + len + 2) return null;
        return { value: buf.toString("utf8", dataStart, dataStart + len), pos: dataStart + len + 2 };
      }
      case "*": {
        const count = parseInt(buf.toString("utf8", start + 1, lineEnd), 10);
        if (count === -1) return { value: null, pos: lineEnd + 2 };
        let pos = lineEnd + 2;
        const arr = [];
        for (let i = 0; i < count; i++) {
          const r = this._parseValue(buf, pos);
          if (r === null) return null;
          if (r.error) return { value: r.value, error: true, pos: r.pos };
          arr.push(r.value);
          pos = r.pos;
        }
        return { value: arr, pos };
      }
      default:
        throw new Error(`Unknown RESP type: ${type}`);
    }
  }
}

// ---------- TCP client ----------

function sendToRedis(args) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: HOST, port: PORT });
    const parser = new RespParser();
    let settled = false;

    const done = (fn, arg) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      fn(arg);
    };

    const timer = setTimeout(() => done(reject, new Error("Redis request timed out")), TIMEOUT_MS);

    socket.on("connect", () => socket.write(encodeCommand(args)));

    socket.on("data", (chunk) => {
      parser.feed(chunk);
      try {
        const r = parser.parse();
        if (r.done) {
          if (r.error) done(reject, new Error(r.value));
          else done(resolve, r.value);
        }
      } catch (e) {
        done(reject, e);
      }
    });

    socket.on("error", (err) => done(reject, err));
    socket.on("close", () => done(reject, new Error("Connection closed before reply")));
  });
}

function sendCommandsBatch(commands) {
  // commands: array of string[] args
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: HOST, port: PORT });
    const parser = new RespParser();
    let settled = false;
    const results = [];
    const expected = commands.length;

    const done = (fn, arg) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      fn(arg);
    };

    const timer = setTimeout(() => done(reject, new Error("Redis request timed out")), TIMEOUT_MS);

    socket.on("connect", () => {
      let buf = Buffer.alloc(0);
      for (const args of commands) {
        buf = Buffer.concat([buf, encodeCommand(args.map(String))]);
      }
      socket.write(buf);
    });

    socket.on("data", (chunk) => {
      parser.feed(chunk);
      try {
        while (true) {
          const r = parser.parse();
          if (!r.done) break;
          if (r.error) {
            done(reject, new Error(r.value));
            return;
          }
          results.push(r.value);
          if (results.length >= expected) {
            done(resolve, results);
            return;
          }
        }
      } catch (e) {
        done(reject, e);
      }
    });

    socket.on("error", (err) => done(reject, err));
    socket.on("close", () => done(reject, new Error("Connection closed before reply")));
  });
}

// ---------- command string parsing ----------

function parseCommandString(str) {
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
    } else if (c === " ") {
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

// ---------- express app ----------

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

app.get("/api/health", async (req, res) => {
  try {
    const pong = await sendToRedis(["PING"]);
    res.json({ status: "ok", redis: pong, host: HOST, port: PORT });
  } catch (e) {
    res.status(502).json({ status: "error", message: e.message, host: HOST, port: PORT });
  }
});

app.post("/api/command", async (req, res) => {
  const { command } = req.body || {};
  if (!command || (typeof command !== "string" && !Array.isArray(command))) {
    return res
      .status(400)
      .json({ error: 'Body must be {"command": "PING"} or {"command": ["SET","foo","bar"]}' });
  }

  // Parse commands - if string, check for newlines to support multi-line transactions
  if (typeof command === "string") {
    const trimmed = command.trim();
    if (trimmed.includes("\n")) {
      const lines = trimmed.split(/\r?\n/).map(l => l.trim()).filter(l => l);
      if (lines.length === 0) return res.status(400).json({ error: "Empty command" });
      const cmds = lines.map(line => parseCommandString(line));
      // Detect transaction pattern
      const first = cmds[0][0] ? cmds[0][0].toUpperCase() : "";
      const last = cmds[cmds.length - 1][0] ? cmds[cmds.length - 1][0].toUpperCase() : "";
      if (first === "MULTI" && last === "EXEC" && cmds.length >= 3) {
        // Execute all as a batch - server handles transaction
        try {
          const result = await sendCommandsBatch(cmds);
          return res.json({ command: cmds, result });
        } catch (e) {
          return res.status(502).json({ command: cmds, error: e.message });
        }
      } else {
        try {
          const result = await sendCommandsBatch(cmds);
          return res.json({ command: cmds, result });
        } catch (e) {
          return res.status(502).json({ command: cmds, error: e.message });
        }
      }
    }
  }

  const args = Array.isArray(command) ? command.map(String) : parseCommandString(command);
  if (args.length === 0) return res.status(400).json({ error: "Empty command" });

  try {
    const result = await sendToRedis(args);
    res.json({ command: args, result });
  } catch (e) {
    res.status(502).json({ command: args, error: e.message });
  }
});

app.listen(HTTP_PORT, () => {
  console.log(`redis-ui  ->  http://localhost:${HTTP_PORT}`);
  console.log(`redis tcp ->  ${HOST}:${PORT}`);
});
