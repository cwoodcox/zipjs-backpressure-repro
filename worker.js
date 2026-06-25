/**
 * Minimum reproduction for the @zip.js/zip.js getData() backpressure issue.
 *
 * Three panels run the same ZIP entry through three pipelines with the same
 * real backpressured R2 multipart sink.  Panels differ only in the zip layer:
 *   /run/getdata  — current npm getData()
 *   /run/fixed    — patched getData() with ByteLengthQueuingStrategy
 *   /run/direct   — native DecompressionStream (no zip.js wrapper)
 *
 * Setup:
 *   node generate-zip.mjs [entry_mb [noise_period [dest_key]]]
 *   npx wrangler dev
 */

// npm version — current released @zip.js/zip.js
import { BlobReader, ZipReader, configure } from "@zip.js/zip.js";
// patched fork — ByteLengthQueuingStrategy on internal transforms
import { BlobReader as BlobReaderFixed, ZipReader as ZipReaderFixed, configure as configureFixed } from "zip-js-fixed";

configure({ useWebWorkers: false });
configureFixed({ useWebWorkers: false });

const PART_SIZE = 5 * 1024 * 1024; // R2 multipart minimum (last part may be smaller)

// ── R2 helpers ────────────────────────────────────────────────────────────────

async function getZipBlob(env, key) {
	const obj = await env.DATA.get(key);
	if (!obj) throw new Error(`${key} not found in R2 — run generate-zip.mjs first`);
	return new Blob([await obj.arrayBuffer()]);
}

async function listZipFiles(env) {
	const { objects } = await env.DATA.list();
	return objects
		.filter(o => o.key.endsWith(".zip"))
		.map(o => ({ key: o.key, size: o.size }))
		.sort((a, b) => a.key.localeCompare(b.key));
}

// ── local file header: compute data offset ────────────────────────────────────
//
// zip.js sets GP bit 3 so the compressed-size in the LFH is 0; we get the
// real value from getEntries().  We still need the LFH to find fnLen + exLen.

async function localDataOffset(blob, lfhOffset) {
	const buf = await blob.slice(lfhOffset, lfhOffset + 30).arrayBuffer();
	const dv  = new DataView(buf);
	if (dv.getUint32(0, true) !== 0x04034b50) throw new Error("not a ZIP local file header");
	const method = dv.getUint16(8, true);
	const fnLen  = dv.getUint16(26, true);
	const exLen  = dv.getUint16(28, true);
	return { method, dataOffset: lfhOffset + 30 + fnLen + exLen };
}

// ── SSE helper ────────────────────────────────────────────────────────────────

function makeSSEResponse(handler) {
	const enc = new TextEncoder();
	let ctrl;
	const readable = new ReadableStream({ start(c) { ctrl = c; } });
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
// backpressured=true  → write() returns the upload Promise (caller must await)
// backpressured=false → write() returns undefined (fire-and-forget)

function makeR2Sink(mpu, { backpressured, onProduced, onConsumed, onProgress }) {
	const parts   = [];
	const pending = [];
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

// ── run handlers ──────────────────────────────────────────────────────────────

async function runGetData(emit, env, key) {
	const start = Date.now();
	const elapsed = () => (Date.now() - start) / 1000;
	emit({ t: 0, produced: 0, consumed: 0 });

	const blob      = await getZipBlob(env, key);
	const zipReader = new ZipReader(new BlobReader(blob));
	const [entry]   = await zipReader.getEntries();
	let produced = 0, consumed = 0;

	const outKey = `output/getdata-${Math.random().toString(36).slice(2, 8)}.bin`;
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

async function runFixed(emit, env, key) {
	const start = Date.now();
	const elapsed = () => (Date.now() - start) / 1000;
	emit({ t: 0, produced: 0, consumed: 0 });

	const blob      = await getZipBlob(env, key);
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

async function runDirect(emit, env, key) {
	const start = Date.now();
	const elapsed = () => (Date.now() - start) / 1000;
	emit({ t: 0, produced: 0, consumed: 0 });

	const blob                   = await getZipBlob(env, key);
	const zipReader              = new ZipReader(new BlobReader(blob));
	const [entry]                = await zipReader.getEntries();
	await zipReader.close();
	const { compressedSize }     = entry;
	const { method, dataOffset } = await localDataOffset(blob, entry.offset);
	if (method !== 8) throw new Error(`unexpected compression method ${method} (expected deflate=8)`);

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

// ── HTML ──────────────────────────────────────────────────────────────────────

function buildHtml() {
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
  .sub { font-size: .8rem; color: #8b949e; margin: 0 0 16px; line-height: 1.7; }
  .sub strong { color: #e6edf3; }
  .file-row { display: flex; align-items: center; gap: 10px; margin-bottom: 20px;
              font-size: .78rem; flex-wrap: wrap; }
  .file-row label { color: #8b949e; }
  .file-row select { background: #21262d; border: 1px solid #30363d; color: #e6edf3;
                     padding: 4px 10px; border-radius: 4px; font-family: inherit;
                     font-size: .78rem; cursor: pointer; min-width: 220px; }
  .file-row select:disabled { opacity: .5; cursor: default; }
  .file-size { color: #8b949e; font-size: .72rem; }
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
  All three runs extract the same entry and write decompressed bytes to the
  same real backpressured R2 multipart sink.  The gap between
  <span style="color:#388bfd">produced</span> and
  <span style="color:#3fb950">consumed</span> is live memory held in the process.<br>
  <em>Left = current npm getData(), Middle = ByteLengthQS patch, Right = native DecompressionStream.</em>
</p>

<div class="file-row">
  <label for="archive-sel">Archive</label>
  <select id="archive-sel" disabled><option value="">loading…</option></select>
  <span class="file-size" id="archive-info"></span>
</div>

<div class="grid">
  <div class="card">
    <p class="card-title">
      <code>getData(writable)</code>
      <span class="badge badge-bug">current — npm</span>
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
      <span><span class="dot" style="background:#388bfd"></span>produced</span>
      <span><span class="dot" style="background:#3fb950"></span>consumed</span>
      <span><span class="dot" style="background:#da3633"></span>backlog</span>
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
      <span><span class="dot" style="background:#388bfd"></span>produced</span>
      <span><span class="dot" style="background:#3fb950"></span>consumed</span>
      <span><span class="dot" style="background:#da3633"></span>backlog</span>
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
(async function loadFiles() {
  const sel  = document.getElementById('archive-sel');
  const info = document.getElementById('archive-info');
  try {
    const files = await fetch('/files').then(r => r.json());
    sel.innerHTML = '';
    if (!files.length) {
      sel.innerHTML = '<option value="">no .zip files in R2</option>';
      return;
    }
    for (const f of files) {
      const opt = document.createElement('option');
      opt.value = f.key;
      opt.textContent = f.key + '  (' + (f.size / 1024 / 1024).toFixed(1) + ' MB compressed)';
      sel.appendChild(opt);
    }
    info.textContent = files.length + ' file' + (files.length === 1 ? '' : 's');
  } catch (e) {
    sel.innerHTML = '<option value="">error loading files</option>';
    info.textContent = String(e);
  } finally {
    sel.disabled = false;
  }
})();

function run(mode) {
  const key = document.getElementById('archive-sel').value;
  if (!key) return;

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

  const es = new EventSource('/run/' + mode + '?key=' + encodeURIComponent(key));

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
    const latest  = pts[pts.length - 1];
    const tMax    = Math.max(latest.t * 1.15, 5);
    const peakMb  = pts.reduce((m, p) => Math.max(m, p.produced, p.consumed), 1);
    const Y_MAX   = Math.max(peakMb / 0.85, 200); // always show 128 MB limit line
    const x = (t)  => (t  / tMax)  * W;
    const y = (mb) => H - (mb / Y_MAX) * H;

    // grid lines at 25/50/75/100% of peak
    ctx.lineWidth = 1;
    for (let i = 1; i <= 4; i++) {
      const mb = peakMb * i / 4;
      const yy = y(mb);
      ctx.strokeStyle = i === 4 ? 'rgba(56,139,253,.25)' : '#21262d';
      ctx.beginPath(); ctx.moveTo(0, yy); ctx.lineTo(W, yy); ctx.stroke();
      ctx.fillStyle = i === 4 ? 'rgba(56,139,253,.6)' : '#484f58';
      ctx.font = (9 * dpr) + 'px ui-monospace,monospace';
      ctx.fillText(mb.toFixed(0) + ' MB', 4, yy - 3);
    }

    // Workers 128 MB memory limit
    const limitY = y(128);
    ctx.save();
    ctx.setLineDash([5 * dpr, 4 * dpr]);
    ctx.strokeStyle = 'rgba(218,54,51,.7)';
    ctx.lineWidth = 1.5;
    ctx.beginPath(); ctx.moveTo(0, limitY); ctx.lineTo(W, limitY); ctx.stroke();
    ctx.restore();
    ctx.fillStyle = 'rgba(218,54,51,.9)';
    ctx.font = (9 * dpr) + 'px ui-monospace,monospace';
    ctx.fillText('128 MB workers limit', 4, limitY - 3);

    // backlog fill
    ctx.beginPath();
    ctx.moveTo(x(pts[0].t), y(pts[0].produced));
    for (const p of pts) ctx.lineTo(x(p.t), y(p.produced));
    for (let i = pts.length - 1; i >= 0; i--) ctx.lineTo(x(pts[i].t), y(pts[i].consumed));
    ctx.closePath();
    ctx.fillStyle = 'rgba(218,54,51,.35)';
    ctx.fill();

    // consumed area
    ctx.beginPath();
    ctx.moveTo(x(pts[0].t), H);
    for (const p of pts) ctx.lineTo(x(p.t), y(p.consumed));
    ctx.lineTo(x(latest.t), H);
    ctx.closePath();
    ctx.fillStyle = 'rgba(63,185,80,.25)';
    ctx.fill();

    // produced line
    ctx.beginPath();
    ctx.moveTo(x(pts[0].t), y(pts[0].produced));
    for (const p of pts) ctx.lineTo(x(p.t), y(p.produced));
    ctx.strokeStyle = '#388bfd'; ctx.lineWidth = 2; ctx.stroke();

    // consumed line
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
		const url      = new URL(request.url);
		const { pathname } = url;

		if (pathname === "/") {
			return new Response(buildHtml(), { headers: { "Content-Type": "text/html" } });
		}

		if (pathname === "/files") {
			const files = await listZipFiles(env);
			return new Response(JSON.stringify(files), {
				headers: { "Content-Type": "application/json", "Cache-Control": "no-cache" },
			});
		}

		const key = url.searchParams.get("key") || "large.zip";
		if (pathname === "/run/getdata") return makeSSEResponse((emit) => runGetData(emit, env, key));
		if (pathname === "/run/fixed")   return makeSSEResponse((emit) => runFixed(emit, env, key));
		if (pathname === "/run/direct")  return makeSSEResponse((emit) => runDirect(emit, env, key));

		return new Response("Not found", { status: 404 });
	},
};
