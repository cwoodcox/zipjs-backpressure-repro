/**
 * Minimum reproduction for the @zip.js/zip.js getData() backpressure issue.
 *
 * getData() inflates as fast as the compressed source allows, with no byte-based
 * bound on its internal transform chain. With a slow destination (e.g. R2 at
 * ~50 MB/s) the entire decompressed entry accumulates in memory. On the 128 MB
 * Cloudflare Workers hard cap this causes OOM for entries above ~2.7 GB.
 *
 * Three panels:
 *   /run/getdata  — current getData(), no backpressure on sink     (bug)
 *   /run/fixed    — patched getData() with real backpressured sink (fix)
 *   /run/direct   — native DecompressionStream, pull-paced         (workaround)
 *
 * Setup:
 *   node generate-zip.mjs           # build large.zip (do once)
 *   npx wrangler r2 object put zipjs-repro-data/large.zip --file large.zip --remote
 *   npx wrangler dev                 # local dev
 */

// npm version — current released @zip.js/zip.js (exhibits the bug)
import { BlobReader, ZipReader, configure } from "@zip.js/zip.js";
// local patched fork — ByteLengthQueuingStrategy on internal transforms (fix)
import { BlobReader as BlobReaderFixed, ZipReader as ZipReaderFixed, configure as configureFixed } from "zip-js-fixed";

configure({ useWebWorkers: false });
configureFixed({ useWebWorkers: false });

const PART_SIZE = 5 * 1024 * 1024; // R2 multipart minimum part size (last part may be smaller)

// ── fetch ZIP blob from R2 ────────────────────────────────────────────────────

async function getZipBlob(env) {
	const obj = await env.DATA.get("large.zip");
	if (!obj) throw new Error("large.zip not found in R2 — run generate-zip.mjs and upload first");
	const buf = await obj.arrayBuffer();
	return new Blob([buf]);
}

// ── local file header: compute data offset only ───────────────────────────────
//
// zip.js writes entries with a data descriptor (GP bit 3 set), so the
// compressed-size field in the LFH is always 0. We get the real compressedSize
// from the ZipEntry returned by getEntries() (which reads the central directory).
// We still need to read the LFH to get fnLen + exLen so we know where data begins.

async function localDataOffset(blob, lfhOffset) {
	const buf = await blob.slice(lfhOffset, lfhOffset + 30).arrayBuffer();
	const dv  = new DataView(buf);
	if (dv.getUint32(0, true) !== 0x04034b50) throw new Error("not a ZIP local file header");
	const method = dv.getUint16(8, true);
	const fnLen  = dv.getUint16(26, true);
	const exLen  = dv.getUint16(28, true); // LOCAL extra length — may differ from CD
	return { method, dataOffset: lfhOffset + 30 + fnLen + exLen };
}

// ── SSE helper ────────────────────────────────────────────────────────────────

function makeSSEResponse(handler) {
	const enc = new TextEncoder();
	let ctrl;
	const readable = new ReadableStream({
		start(c) { ctrl = c; },
	});
	const emit = (obj) => ctrl.enqueue(enc.encode(`data: ${JSON.stringify(obj)}\n\n`));
	handler(emit)
		.catch((err) => emit({ error: String(err) }))
		.finally(() => ctrl.close());
	return new Response(readable, {
		headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" },
	});
}

// ── R2 multipart sink ─────────────────────────────────────────────────────────
//
// Buffers chunks to PART_SIZE then uploads one R2 multipart part.
// backpressured=true  → write() returns the upload Promise (getData waits)
// backpressured=false → write() returns undefined (getData never waits)

function makeR2Sink(mpu, { backpressured, onProduced, onConsumed, onProgress }) {
	const parts   = [];
	const pending = []; // in-flight upload promises (non-backpressured mode only)
	let buf = [], bufSize = 0, partNum = 1;

	function startPart(chunks, size, num) {
		const p = mpu.uploadPart(num, new Blob(chunks)).then(part => {
			parts[num - 1] = part;
			onConsumed(size);
			if (onProgress) onProgress();
		});
		return p;
	}

	return new WritableStream({
		write(chunk) {
			onProduced(chunk.byteLength);
			buf.push(chunk);
			bufSize += chunk.byteLength;
			if (bufSize >= PART_SIZE) {
				const p = startPart(buf, bufSize, partNum++);
				buf = []; bufSize = 0;
				if (backpressured) return p;
				else pending.push(p);
			}
		},
		async flush() {
			if (buf.length) await startPart(buf, bufSize, partNum++);
			if (pending.length) await Promise.all(pending);
			await mpu.complete(parts.filter(Boolean));
		},
		async abort() { await mpu.abort().catch(() => {}); },
	});
}

// ── /run/getdata ──────────────────────────────────────────────────────────────
//
// Uses current npm getData() with a non-backpressuring sink.
// getData inflates the entire entry synchronously before any R2 write begins.
// On a 128 MB Workers isolate, entries above ~2.7 GB OOM here.
// Also exceeds the 30 ms CPU time limit on deployed edge — run locally.

async function runGetData(emit, env) {
	const start = Date.now();
	const elapsed = () => (Date.now() - start) / 1000;

	const blob      = await getZipBlob(env);
	const zipReader = new ZipReader(new BlobReader(blob));
	const [entry]   = await zipReader.getEntries();

	let produced = 0, consumed = 0;

	// write() returns undefined → getData inflates the full entry synchronously.
	// All decompressed chunks accumulate in `chunks` before any R2 write starts.
	const chunks = [];
	await entry.getData(new WritableStream({
		write(chunk) { produced += chunk.byteLength; chunks.push(chunk); },
	}));

	// Emit once to show the full produced spike before any R2 writes begin.
	emit({ t: elapsed(), produced: produced / 1e6, consumed: 0 });

	// Drain to R2 serially — emit after each part so consumed rises visibly.
	const outKey = `output/getdata-${Math.random().toString(36).slice(2, 8)}.bin`;
	const mpu = await env.DATA.createMultipartUpload(outKey);
	try {
		let buf = [], bufSize = 0, partNum = 1;
		const parts = [];
		for (const chunk of chunks) {
			buf.push(chunk); bufSize += chunk.byteLength;
			if (bufSize >= PART_SIZE) {
				parts.push(await mpu.uploadPart(partNum++, new Blob(buf)));
				consumed += bufSize;
				buf = []; bufSize = 0;
				emit({ t: elapsed(), produced: produced / 1e6, consumed: consumed / 1e6 });
			}
		}
		if (bufSize > 0) {
			parts.push(await mpu.uploadPart(partNum++, new Blob(buf)));
			consumed += bufSize;
		}
		await mpu.complete(parts);
	} catch (err) {
		await mpu.abort().catch(() => {});
		throw err;
	} finally {
		await env.DATA.delete(outKey).catch(() => {});
	}

	emit({ t: elapsed(), produced: produced / 1e6, consumed: consumed / 1e6, done: true });
	await zipReader.close();
}

// ── /run/fixed ────────────────────────────────────────────────────────────────
//
// Uses the patched getData() (ByteLengthQueuingStrategy on internal transforms)
// with a REAL backpressured sink: write() returns a setTimeout Promise so the
// drain rate is enforced. Because the fix makes getData honour backpressure,
// it advances only as fast as the sink drains — produced tracks consumed.

async function runFixed(emit, env) {
	const start = Date.now();
	const elapsed = () => (Date.now() - start) / 1000;

	const blob      = await getZipBlob(env);
	const zipReader = new ZipReaderFixed(new BlobReaderFixed(blob));
	const [entry]   = await zipReader.getEntries();

	let produced = 0, consumed = 0;

	const outKey = `output/fixed-${Math.random().toString(36).slice(2, 8)}.bin`;
	const mpu = await env.DATA.createMultipartUpload(outKey);
	try {
		await entry.getData(makeR2Sink(mpu, {
			backpressured: true,
			onProduced: (n) => { produced += n; },
			onConsumed: (n) => { consumed += n; },
			onProgress: () => emit({ t: elapsed(), produced: produced / 1e6, consumed: consumed / 1e6 }),
		}));
	} catch (err) {
		await mpu.abort().catch(() => {});
		throw err;
	} finally {
		await env.DATA.delete(outKey).catch(() => {});
	}

	emit({ t: elapsed(), produced: produced / 1e6, consumed: consumed / 1e6, done: true });
	await zipReader.close();
}

// ── /run/direct ───────────────────────────────────────────────────────────────
//
// Bypasses getData entirely. Pull-based: native DecompressionStream driven by
// the sink. Produced and consumed track each other throughout — no backlog.

async function runDirect(emit, env) {
	const start = Date.now();
	const elapsed = () => (Date.now() - start) / 1000;

	const blob = await getZipBlob(env);

	const zipReader          = new ZipReader(new BlobReader(blob));
	const [entry]            = await zipReader.getEntries();
	await zipReader.close();
	const { compressedSize } = entry;
	const { method, dataOffset } = await localDataOffset(blob, entry.offset);
	if (method !== 8)
		throw new Error(`unexpected compression method ${method} (expected 8 = deflate)`);

	let produced = 0, consumed = 0;

	const CHUNK = 64 * 1024;
	const compressedData = blob.slice(dataOffset, dataOffset + compressedSize);
	let offset = 0;

	const outKey = `output/direct-${Math.random().toString(36).slice(2, 8)}.bin`;
	const mpu = await env.DATA.createMultipartUpload(outKey);
	try {
		await new ReadableStream({
			async pull(controller) {
				if (offset >= compressedSize) { controller.close(); return; }
				const n  = Math.min(CHUNK, compressedSize - offset);
				const ab = await compressedData.slice(offset, offset + n).arrayBuffer();
				controller.enqueue(new Uint8Array(ab));
				offset += n;
			},
		})
			.pipeThrough(new DecompressionStream("deflate-raw"))
			.pipeTo(makeR2Sink(mpu, {
				backpressured: true,
				onProduced: (n) => { produced += n; },
				onConsumed: (n) => { consumed += n; },
				onProgress: () => emit({ t: elapsed(), produced: produced / 1e6, consumed: consumed / 1e6 }),
			}));
	} catch (err) {
		await mpu.abort().catch(() => {});
		throw err;
	} finally {
		await env.DATA.delete(outKey).catch(() => {});
	}

	emit({ t: elapsed(), produced: produced / 1e6, consumed: consumed / 1e6, done: true });
}

// ── entry metadata ────────────────────────────────────────────────────────────

let _meta = null;
async function getEntryMeta(env) {
	if (_meta) return _meta;
	const blob      = await getZipBlob(env);
	const zipReader = new ZipReader(new BlobReader(blob));
	const [entry]   = await zipReader.getEntries();
	await zipReader.close();
	_meta = { entryMb: Math.round(entry.uncompressedSize / 1024 / 1024) };
	return _meta;
}

// ── HTML ──────────────────────────────────────────────────────────────────────

function buildHtml(entryMb) {
	return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>zip.js getData() backpressure demo</title>
<style>
  *, *::before, *::after { box-sizing: border-box; }
  body { font-family: ui-monospace, monospace; background: #0d1117; color: #e6edf3;
         margin: 0; padding: 32px 24px; max-width: 1200px; }
  h1   { font-size: 1rem; color: #58a6ff; margin: 0 0 6px; }
  .sub { font-size: .8rem; color: #8b949e; margin: 0 0 28px; line-height: 1.7; }
  .sub strong { color: #e6edf3; }
  .grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
  .card-title { font-size: .8rem; margin: 0 0 10px; display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
  .badge { font-size: .6rem; padding: 2px 7px; border-radius: 3px; font-weight: bold; white-space: nowrap; }
  .badge-bug  { background: #da3633; }
  .badge-fix  { background: #238636; }
  .badge-alt  { background: #6e40c9; }
  canvas { width: 100%; height: 170px; display: block; background: #0d1117; border-radius: 4px; }
  .legend { display: flex; gap: 12px; margin: 7px 0 4px; font-size: .7rem; color: #8b949e; flex-wrap: wrap; }
  .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
         margin-right: 3px; vertical-align: middle; }
  .stat { font-size: .7rem; color: #8b949e; min-height: 1.4em; margin-bottom: 10px; }
  button { background: #21262d; border: 1px solid #30363d; color: #e6edf3;
           padding: 6px 14px; border-radius: 6px; cursor: pointer;
           font-family: inherit; font-size: .78rem; }
  button:hover:not(:disabled) { background: #30363d; }
  button:disabled { opacity: .45; cursor: default; }
  .footer { margin-top: 28px; font-size: .7rem; color: #8b949e; }
  .footer a { color: #58a6ff; text-decoration: none; }
  .footer a:hover { text-decoration: underline; }
</style>
</head>
<body>
<h1>zip.js <code>entry.getData()</code> — backpressure demo</h1>
<p class="sub">
  All three runs extract a <strong>${entryMb} MB</strong> entry from the same ZIP
  and write the decompressed bytes to R2 (multipart upload).<br>
  The gap between
  <span style="color:#388bfd">produced</span> and
  <span style="color:#3fb950">consumed</span>
  is live memory held in the process.<br>
  <em>Left panel: <code>getData()</code> inflates synchronously — the full entry
  accumulates in JS heap before any R2 write begins. On a deployed Workers isolate
  (128 MB hard limit) this OOMs for entries above ~2.7 GB; it also exceeds the 30 ms
  CPU time limit, so the left panel works best in local <code>wrangler dev</code>.
  The middle and right panels yield the event loop every R2 part and work when deployed.</em>
</p>

<div class="grid">
  <div class="card">
    <p class="card-title">
      <code>getData(writable)</code>
      <span class="badge badge-bug">current — unbounded</span>
    </p>
    <canvas id="c-getdata"></canvas>
    <div class="legend">
      <span><span class="dot" style="background:#388bfd"></span>produced</span>
      <span><span class="dot" style="background:#3fb950"></span>consumed</span>
      <span><span class="dot" style="background:#da3633"></span>backlog</span>
    </div>
    <div class="stat" id="stat-getdata">—</div>
    <button id="btn-getdata" onclick="run('getdata')">Run current</button>
  </div>

  <div class="card">
    <p class="card-title">
      <code>getData(writable)</code>
      <span class="badge badge-fix">patched — ByteLengthQS</span>
    </p>
    <canvas id="c-fixed"></canvas>
    <div class="legend">
      <span><span class="dot" style="background:#3fb950"></span>consumed (= produced)</span>
    </div>
    <div class="stat" id="stat-fixed">—</div>
    <button id="btn-fixed" onclick="run('fixed')">Run patched</button>
  </div>

  <div class="card">
    <p class="card-title">
      <code>DecompressionStream</code>
      <span class="badge badge-alt">workaround — bypass</span>
    </p>
    <canvas id="c-direct"></canvas>
    <div class="legend">
      <span><span class="dot" style="background:#3fb950"></span>consumed (= produced)</span>
    </div>
    <div class="stat" id="stat-direct">—</div>
    <button id="btn-direct" onclick="run('direct')">Run direct</button>
  </div>
</div>

<div class="footer">
  <a href="https://github.com/cwoodcox/zipjs-backpressure-repro">cwoodcox/zipjs-backpressure-repro</a>
  ·
  <a href="https://github.com/gildas-lormeau/zip.js">gildas-lormeau/zip.js</a>
</div>

<script>
const ENTRY_MB = ${entryMb};

function run(mode) {
  const canvas = document.getElementById('c-' + mode);
  const stat   = document.getElementById('stat-' + mode);
  const btn    = document.getElementById('btn-' + mode);
  btn.disabled = true;
  stat.textContent = 'running…';

  const dpr = devicePixelRatio || 1;
  canvas.width  = canvas.offsetWidth  * dpr;
  canvas.height = canvas.offsetHeight * dpr;
  const ctx = canvas.getContext('2d');
  const W = canvas.width, H = canvas.height;
  const pts = [];

  const es = new EventSource('/run/' + mode);

  es.onmessage = ({ data }) => {
    const d = JSON.parse(data);
    if (d.error) {
      stat.textContent = 'error: ' + d.error;
      es.close(); btn.disabled = false; return;
    }
    pts.push(d);
    draw();
    const backlog = Math.max(0, d.produced - d.consumed).toFixed(0);
    stat.textContent = d.done
      ? 'produced ' + d.produced.toFixed(0) + ' MB \xb7 consumed ' + d.consumed.toFixed(0) + ' MB \xb7 done ' + d.t.toFixed(1) + 's'
      : 'produced ' + d.produced.toFixed(0) + ' MB \xb7 consumed ' + d.consumed.toFixed(0) + ' MB \xb7 backlog ' + backlog + ' MB';
    if (d.done) { es.close(); btn.disabled = false; }
  };

  es.onerror = () => {
    stat.textContent = 'connection lost — Worker may have exceeded the 128 MB memory limit';
    es.close(); btn.disabled = false;
  };

  function draw() {
    ctx.clearRect(0, 0, W, H);
    if (pts.length < 2) return;
    const latest = pts[pts.length - 1];
    const tMax  = Math.max(latest.t * 1.15, 5);
    // 15% headroom so the produced line (often at ENTRY_MB) stays visible
    const Y_MAX = ENTRY_MB / 0.85;
    const x = (t)  => (t  / tMax)  * W;
    const y = (mb) => H - (mb / Y_MAX) * H;

    // grid lines at 25/50/75/100% of ENTRY_MB; 100% line in blue-tint
    ctx.lineWidth = 1;
    for (let i = 1; i <= 4; i++) {
      const mb = ENTRY_MB * i / 4;
      const yy = y(mb);
      ctx.strokeStyle = i === 4 ? 'rgba(56,139,253,.25)' : '#21262d';
      ctx.beginPath(); ctx.moveTo(0, yy); ctx.lineTo(W, yy); ctx.stroke();
      ctx.fillStyle = i === 4 ? 'rgba(56,139,253,.6)' : '#484f58';
      ctx.font = (9 * dpr) + 'px ui-monospace,monospace';
      ctx.fillText(mb.toFixed(0) + ' MB', 4, yy - 3);
    }

    // backlog fill (produced - consumed) — red
    ctx.beginPath();
    ctx.moveTo(x(pts[0].t), y(pts[0].produced));
    for (const p of pts) ctx.lineTo(x(p.t), y(p.produced));
    for (let i = pts.length - 1; i >= 0; i--) ctx.lineTo(x(pts[i].t), y(pts[i].consumed));
    ctx.closePath();
    ctx.fillStyle = 'rgba(218,54,51,.35)';
    ctx.fill();

    // consumed area — green
    ctx.beginPath();
    ctx.moveTo(x(pts[0].t), H);
    for (const p of pts) ctx.lineTo(x(p.t), y(p.consumed));
    ctx.lineTo(x(latest.t), H);
    ctx.closePath();
    ctx.fillStyle = 'rgba(63,185,80,.25)';
    ctx.fill();

    // produced line — blue (drawn after fills so it's on top)
    ctx.beginPath();
    ctx.moveTo(x(pts[0].t), y(pts[0].produced));
    for (const p of pts) ctx.lineTo(x(p.t), y(p.produced));
    ctx.strokeStyle = '#388bfd'; ctx.lineWidth = 2; ctx.stroke();

    // consumed line — green
    ctx.beginPath();
    ctx.moveTo(x(pts[0].t), y(pts[0].consumed));
    for (const p of pts) ctx.lineTo(x(p.t), y(p.consumed));
    ctx.strokeStyle = '#3fb950'; ctx.lineWidth = 2; ctx.stroke();
  }
}
</script>
</body>
</html>`;
}

// ── router ────────────────────────────────────────────────────────────────────

export default {
	async fetch(request, env) {
		const { pathname } = new URL(request.url);
		if (pathname === "/") {
			const { entryMb } = await getEntryMeta(env);
			return new Response(buildHtml(entryMb), { headers: { "Content-Type": "text/html" } });
		}
		if (pathname === "/run/getdata") return makeSSEResponse((emit) => runGetData(emit, env));
		if (pathname === "/run/fixed")   return makeSSEResponse((emit) => runFixed(emit, env));
		if (pathname === "/run/direct")  return makeSSEResponse((emit) => runDirect(emit, env));
return new Response("Not found", { status: 404 });
	},
};
