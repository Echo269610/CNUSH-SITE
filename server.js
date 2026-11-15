/* CNUHS Ad Site (public web version) - tiny zero-dependency Node server
 * Serves index.html + logo, stores uploaded videos on disk, pushes live
 * updates via SSE. Designed for single-port hosting (PORT env variable).
 */
"use strict";

var http = require("http");
var fs = require("fs");
var path = require("path");
var crypto = require("crypto");

/* ---------------- configuration ---------------- */
var PORT = parseInt(process.env.PORT || "3000", 10);
var MAX_UPLOAD = 200 * 1024 * 1024;      // 200 MB
var MAX_INTRO_WORDS = 100;
var APP_DIR = __dirname;
var DATA_DIR = path.join(APP_DIR, "data");
var VIDEO_DIR = path.join(DATA_DIR, "videos");
var DB_FILE = path.join(DATA_DIR, "db.json");

try { fs.mkdirSync(VIDEO_DIR, { recursive: true }); } catch (e) {}
if (!fs.existsSync(DB_FILE)) { try { fs.writeFileSync(DB_FILE, "[]"); } catch (e) {} }

/* ---------------- never die ---------------- */
process.on("uncaughtException", function (err) {
  console.error("[uncaught]", err && err.message);
});
process.on("unhandledRejection", function (err) {
  console.error("[unhandled]", err && (err.message || err));
});

/* ---------------- tiny JSON db ---------------- */
function readDB() {
  try {
    var arr = JSON.parse(fs.readFileSync(DB_FILE, "utf8"));
    return Array.isArray(arr) ? arr : [];
  } catch (e) { return []; }
}
function writeDB(arr) {
  try { fs.writeFileSync(DB_FILE, JSON.stringify(arr, null, 1)); }
  catch (e) { console.error("db write failed:", e.message); }
}

/* ---------------- SSE ---------------- */
var sseClients = [];
function sseBroadcast(msg) {
  var dead = [];
  sseClients.forEach(function (res) {
    try { res.write("data: " + msg + "\n\n"); } catch (e) { dead.push(res); }
  });
  dead.forEach(function (res) {
    var i = sseClients.indexOf(res);
    if (i !== -1) sseClients.splice(i, 1);
  });
}

/* ---------------- helpers ---------------- */
function sendJSON(res, code, obj) {
  res.writeHead(code, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  res.end(JSON.stringify(obj));
}

function readBody(req, cap, cb) {
  var chunks = [];
  var size = 0;
  var aborted = false;
  req.on("data", function (c) {
    if (aborted) return;
    size += c.length;
    if (size > cap) {
      aborted = true;
      cb(new Error("TOO_LARGE"), null);
      req.destroy();
      return;
    }
    chunks.push(c);
  });
  req.on("end", function () {
    if (!aborted) cb(null, Buffer.concat(chunks));
  });
  req.on("error", function () { if (!aborted) { aborted = true; cb(new Error("READ_ERROR"), null); } });
}

function wordCount(s) {
  var t = String(s || "").trim();
  return t ? t.split(/\s+/).length : 0;
}

function parseMultipart(buf, contentType) {
  var m = /boundary=(?:"([^"]+)"|([^;]+))/i.exec(contentType);
  if (!m) return null;
  var boundary = "--" + (m[1] || m[2]).trim();
  var bBuf = Buffer.from(boundary);
  var fields = {};
  var file = null;
  var idx = buf.indexOf(bBuf);
  while (idx !== -1) {
    var next = buf.indexOf(bBuf, idx + bBuf.length);
    if (next === -1) break;
    var start = idx + bBuf.length;
    if (buf[start] === 13 && buf[start + 1] === 10) start += 2;
    var end = next - 2;
    if (end <= start) { idx = next; continue; }
    var part = buf.slice(start, end);
    var headEnd = part.indexOf("\r\n\r\n");
    if (headEnd !== -1) {
      var head = part.slice(0, headEnd).toString("utf8");
      var body = part.slice(headEnd + 4);
      var nameMatch = /name="([^"]*)"/i.exec(head);
      var fileMatch = /filename="([^"]*)"/i.exec(head);
      var typeMatch = /Content-Type:\s*([^\r\n]+)/i.exec(head);
      if (nameMatch) {
        if (fileMatch && fileMatch[1] !== "") {
          var ext = path.extname(fileMatch[1]).toLowerCase();
          if (ext === "" || ext.charAt(0) !== ".") ext = ".mp4";
          file = { ext: ext, type: (typeMatch ? typeMatch[1].trim() : "video/mp4"), data: body };
        } else {
          fields[nameMatch[1]] = body.toString("utf8").trim();
        }
      }
    }
    idx = next;
  }
  return { fields: fields, file: file };
}

var VIDEO_TYPES = {
  ".mp4": "video/mp4", ".m4v": "video/mp4", ".mov": "video/quicktime",
  ".webm": "video/webm", ".mkv": "video/x-matroska", ".3gp": "video/3gpp"
};

function serveVideoFile(req, res, fileName) {
  var base = path.basename(decodeURIComponent(fileName));
  var full = path.join(VIDEO_DIR, base);
  if (!fs.existsSync(full)) { res.writeHead(404); res.end("Not found"); return; }
  var stat = fs.statSync(full);
  var ctype = VIDEO_TYPES[path.extname(base).toLowerCase()] || "application/octet-stream";
  var range = req.headers.range;
  if (range) {
    var m = /bytes=(\d*)-(\d*)/.exec(range);
    var start = m && m[1] !== "" ? parseInt(m[1], 10) : 0;
    var end = m && m[2] !== "" ? parseInt(m[2], 10) : stat.size - 1;
    if (isNaN(start) || start > end || end >= stat.size) {
      res.writeHead(416, { "Content-Range": "bytes */" + stat.size });
      res.end(); return;
    }
    res.writeHead(206, {
      "Content-Type": ctype,
      "Content-Range": "bytes " + start + "-" + end + "/" + stat.size,
      "Accept-Ranges": "bytes",
      "Content-Length": end - start + 1
    });
    fs.createReadStream(full, { start: start, end: end }).pipe(res);
  } else {
    res.writeHead(200, {
      "Content-Type": ctype,
      "Content-Length": stat.size,
      "Accept-Ranges": "bytes"
    });
    fs.createReadStream(full).pipe(res);
  }
}

function serveStatic(res, filePath, ctype) {
  fs.readFile(filePath, function (err, data) {
    if (err) { res.writeHead(404); res.end("Not found"); return; }
    res.writeHead(200, { "Content-Type": ctype });
    res.end(data);
  });
}

/* ---------------- router ---------------- */
var server = http.createServer(function (req, res) {
  var urlPath = (req.url || "/").split("?")[0];
  res.setHeader("X-Content-Type-Options", "nosniff");

  if (req.method === "GET") {
    if (urlPath === "/" || urlPath === "/index.html") {
      return serveStatic(res, path.join(APP_DIR, "index.html"), "text/html; charset=utf-8");
    }
    if (urlPath === "/logo.jpg") {
      return serveStatic(res, path.join(APP_DIR, "logo.jpg"), "image/jpeg");
    }
    if (urlPath === "/api/config") {
      return sendJSON(res, 200, { mode: "server", public: true });
    }
    if (urlPath === "/api/videos") {
      var cat = "";
      try { cat = decodeURIComponent((req.url.split("?")[1] || "").replace(/^cat=/, "")); } catch (e) {}
      var db = readDB();
      var list = db.filter(function (v) { return !cat || v.cat === cat; });
      list.sort(function (a, b) { return b.time - a.time; });
      return sendJSON(res, 200, { videos: list });
    }
    if (urlPath === "/api/events") {
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-store",
        "Connection": "keep-alive"
      });
      res.write(": ok\n\n");
      sseClients.push(res);
      var ping = setInterval(function () {
        try { res.write(": ping\n\n"); } catch (e) {}
      }, 15000);
      req.on("close", function () {
        clearInterval(ping);
        var i = sseClients.indexOf(res);
        if (i !== -1) sseClients.splice(i, 1);
      });
      return;
    }
    if (urlPath.indexOf("/videos/") === 0) {
      return serveVideoFile(req, res, urlPath.slice("/videos/".length));
    }
    res.writeHead(404); res.end("Not found");
    return;
  }

  if (req.method === "POST" && urlPath === "/api/upload") {
    var ct = req.headers["content-type"] || "";
    if (ct.indexOf("multipart/form-data") === -1) {
      return sendJSON(res, 400, { error: "Bad request" });
    }
    return readBody(req, MAX_UPLOAD, function (err, buf) {
      if (err) {
        if (err.message === "TOO_LARGE") return sendJSON(res, 413, { error: "Video is too large. Max 200 MB." });
        return sendJSON(res, 400, { error: "Upload interrupted. Please try again." });
      }
      var parsed = parseMultipart(buf, ct);
      if (!parsed || !parsed.file || !parsed.fields.cat) {
        return sendJSON(res, 400, { error: "Please choose a video file and fill in all required fields." });
      }
      var f = parsed.fields;
      if (!f.group || !f.title || !f.intro) {
        return sendJSON(res, 400, { error: "Group number, video title and introduction are all required." });
      }
      if (wordCount(f.intro) > MAX_INTRO_WORDS) {
        return sendJSON(res, 400, { error: "The introduction must be no more than 100 words." });
      }
      var id = crypto.randomBytes(6).toString("hex") + "-" + Date.now();
      var ext = [".mp4", ".m4v", ".mov", ".webm", ".mkv", ".3gp"].indexOf(parsed.file.ext.toLowerCase()) !== -1
        ? parsed.file.ext.toLowerCase() : ".mp4";
      var fileName = id.replace(/[^a-z0-9-]/gi, "") + ext;
      try {
        fs.writeFileSync(path.join(VIDEO_DIR, fileName), parsed.file.data);
      } catch (e) {
        return sendJSON(res, 500, { error: "Could not save the video on the server." });
      }
      var db = readDB();
      db.push({
        id: id,
        cat: String(f.cat).slice(0, 30),
        group: String(f.group).slice(0, 20),
        title: String(f.title).slice(0, 80),
        intro: String(f.intro).slice(0, 1200),
        file: fileName,
        size: parsed.file.data.length,
        time: Date.now()
      });
      writeDB(db);
      sseBroadcast("new");
      return sendJSON(res, 200, { ok: true, id: id });
    });
  }

  res.writeHead(405); res.end("Method not allowed");
});

server.listen(PORT, "0.0.0.0", function () {
  console.log("CNUHS Ad Site (public) running on port " + PORT);
});
