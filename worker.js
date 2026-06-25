/**
 * Minimum reproduction for the @zip.js/zip.js getData() backpressure issue.
 *
 * getData() inflates as fast as the compressed source allows, with no byte-based
 * bound on its internal transform chain. With a slow destination (e.g. R2 at
 * ~50 MB/s) the entire decompressed entry accumulates in memory. On the 128 MB
 * Cloudflare Workers hard cap this causes OOM for entries above ~2.7 GB.
 *
 * This Worker loads a pre-built ZIP from R2 and lets you watch the effect live:
 *
 *   GET /             → demo UI (two side-by-side live charts)
 *   GET /run/getdata  → SSE, getData() path — produced spikes immediately
 *   GET /run/direct   → SSE, plain DecompressionStream — produced tracks consumed
 *
 * Setup:
 *   node generate-zip.mjs           # build large.zip (do once)
 *   npx wrangler r2 object put zipjs-repro-data/large.zip --file large.zip --remote
 *   npx wrangler dev                 # local dev against remote R2
 */

import { BlobReader, ZipReader, configure } from "@zip.js/zip.js";

configure({ useWebWorkers: false });

const DRAIN_RATE_MBPS = 50; // simulated destination write speed (MB/s)

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
// We still need to read the LFH to get fnLen + exLen (the LOCAL extra-field
// length can differ from the central-directory value) so we know where data begins.

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
	const { readable, writable } = new TransformStream();
	const writer  = writable.getWriter();
	const enc     = new TextEncoder();
	const emit    = (obj) => writer.write(enc.encode(`data: ${JSON.stringify(obj)}\n\n`));
	handler(emit)
		.catch((err) => emit({ error: String(err) }))
		.finally(() => writer.close());
	return new Response(readable, {
		headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" },
	});
}

// ── /run/getdata ──────────────────────────────────────────────────────────────
//
// Uses entry.getData(writable). The sink accepts every chunk with no delay
// (no backpressure) so getData inflates as fast as it can. Meanwhile a
// separate drain loop consumes at DRAIN_RATE_MBPS.
//
// Because getData is not backpressured, it runs at inflate speed (~80+ MB/s
// in workerd). The drain runs at 50 MB/s. The gap between produced and
// consumed is memory held live in the Worker at that moment. On a 128 MB
// isolate, entries above ~2.7 GB cause OOM before the sink drains anything.

async function runGetData(emit, env) {
	const blob      = await getZipBlob(env);
	const zipReader = new ZipReader(new BlobReader(blob));
	const [entry]   = await zipReader.getEntries();

	let produced = 0, consumed = 0;
	const start = Date.now();
	const elapsed = () => (Date.now() - start) / 1000;

	const interval = setInterval(
		() => emit({ t: elapsed(), produced: produced / 1e6, consumed: consumed / 1e6 }),
		100,
	);

	// Fire getData with a non-backpressuring sink: write() returns undefined so
	// getData never has to wait. It inflates at full speed and increments
	// `produced`. The Promise resolves once all chunks are written.
	const getDataDone = entry.getData(new WritableStream({
		write(chunk) { produced += chunk.byteLength; /* no return → no backpressure */ },
	}));

	let inflateFinished = false;
	getDataDone.finally(() => { inflateFinished = true; });

	// Drain loop — simulates a 50 MB/s destination (e.g. R2 multipart upload).
	// Runs concurrently with getData: while inflate fills `produced`, this loop
	// advances `consumed` at the simulated rate.
	const DRAIN_CHUNK = 1 * 1024 * 1024;
	while (!inflateFinished || consumed < produced) {
		if (consumed >= produced) {
			await new Promise((r) => setTimeout(r, 10));
			continue;
		}
		const n = Math.min(DRAIN_CHUNK, produced - consumed);
		await new Promise((r) => setTimeout(r, n / (DRAIN_RATE_MBPS * 1e6) * 1000));
		consumed += n;
	}

	await getDataDone;
	clearInterval(interval);
	emit({ t: elapsed(), produced: produced / 1e6, consumed: consumed / 1e6, done: true });
	await zipReader.close();
}

// ── /run/direct ───────────────────────────────────────────────────────────────
//
// Bypasses getData entirely. Pulls compressed bytes from R2 in 64 KB chunks,
// pipes through native DecompressionStream('deflate-raw'). Pull-based:
// the inflate only runs as fast as the sink consumes output. Produced and
// consumed track each other throughout — no in-flight backlog.

async function runDirect(emit, env) {
	const blob = await getZipBlob(env);

	const zipReader          = new ZipReader(new BlobReader(blob));
	const [entry]            = await zipReader.getEntries();
	await zipReader.close();
	const { compressedSize } = entry;
	const { method, dataOffset } = await localDataOffset(blob, entry.offset);
	if (method !== 8)
		throw new Error(`unexpected compression method ${method} (expected 8 = deflate)`);

	let produced = 0, consumed = 0;
	const start = Date.now();
	const elapsed = () => (Date.now() - start) / 1000;

	const interval = setInterval(
		() => emit({ t: elapsed(), produced: produced / 1e6, consumed: consumed / 1e6 }),
		100,
	);

	const CHUNK = 64 * 1024;
	const compressedData = blob.slice(dataOffset, dataOffset + compressedSize);
	let offset = 0;

	const compressedStream = new ReadableStream({
		async pull(controller) {
			if (offset >= compressedSize) { controller.close(); return; }
			const n  = Math.min(CHUNK, compressedSize - offset);
			const ab = await compressedData.slice(offset, offset + n).arrayBuffer();
			controller.enqueue(new Uint8Array(ab));
			offset += n;
		},
	});

	await compressedStream
		.pipeThrough(new DecompressionStream("deflate-raw"))
		.pipeTo(new WritableStream({
			write(chunk) {
				produced += chunk.byteLength;
				consumed += chunk.byteLength;
				return new Promise((r) =>
					setTimeout(r, chunk.byteLength / (DRAIN_RATE_MBPS * 1e6) * 1000)
				);
			},
		}));

	clearInterval(interval);
	emit({ t: elapsed(), produced: produced / 1e6, consumed: consumed / 1e6, done: true });
}

// ── entry metadata ────────────────────────────────────────────────────────────
// Read once at startup so the HTML can embed accurate numbers.
// Cached across requests within an isolate lifetime.

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
         margin: 0; padding: 32px 24px; max-width: 960px; }
  h1   { font-size: 1rem; color: #58a6ff; margin: 0 0 6px; }
  .sub { font-size: .8rem; color: #8b949e; margin: 0 0 28px; line-height: 1.7; }
  .sub strong { color: #e6edf3; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 18px; }
  .card-title { font-size: .85rem; margin: 0 0 12px; display: flex; align-items: center; gap: 8px; }
  .badge { font-size: .65rem; padding: 2px 7px; border-radius: 3px; font-weight: bold; }
  .badge-bug { background: #da3633; }
  .badge-fix { background: #238636; }
  canvas { width: 100%; height: 190px; display: block; background: #0d1117; border-radius: 4px; }
  .legend { display: flex; gap: 16px; margin: 8px 0 4px; font-size: .72rem; color: #8b949e; }
  .dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%;
         margin-right: 4px; vertical-align: middle; }
  .stat { font-size: .73rem; color: #8b949e; min-height: 1.5em; margin-bottom: 12px; }
  button { background: #21262d; border: 1px solid #30363d; color: #e6edf3;
           padding: 7px 18px; border-radius: 6px; cursor: pointer;
           font-family: inherit; font-size: .8rem; }
  button:hover:not(:disabled) { background: #30363d; }
  button:disabled { opacity: .45; cursor: default; }
  .footer { margin-top: 32px; font-size: .72rem; color: #8b949e; }
  .footer a { color: #58a6ff; text-decoration: none; }
  .footer a:hover { text-decoration: underline; }
</style>
</head>
<body>
<h1>zip.js <code>entry.getData()</code> — backpressure demo</h1>
<p class="sub">
  Both runs extract a <strong>${entryMb} MB</strong> entry from the same ZIP,
  draining to a simulated <strong>${DRAIN_RATE_MBPS} MB/s</strong> destination
  (e.g. an R2 write stream).<br>
  The gap between
  <span style="color:#388bfd">produced</span> and
  <span style="color:#3fb950">consumed</span>
  is memory held in the pipeline.
  With <code>getData()</code> on a 128 MB Workers isolate, entries above ~2.7 GB
  cause OOM before the sink drains anything.
</p>

<div class="grid">
  <div class="card">
    <p class="card-title">
      <code>entry.getData(writable)</code>
      <span class="badge badge-bug">current behaviour</span>
    </p>
    <canvas id="c-getdata"></canvas>
    <div class="legend">
      <span><span class="dot" style="background:#388bfd"></span>produced</span>
      <span><span class="dot" style="background:#3fb950"></span>consumed</span>
      <span><span class="dot" style="background:#da3633"></span>backlog</span>
    </div>
    <div class="stat" id="stat-getdata">—</div>
    <button id="btn-getdata" onclick="run('getdata')">Run getData()</button>
  </div>

  <div class="card">
    <p class="card-title">
      <code>DecompressionStream</code> (direct)
      <span class="badge badge-fix">bounded</span>
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
  &nbsp;·&nbsp;
  <a href="https://github.com/gildas-lormeau/zip.js">gildas-lormeau/zip.js</a>
</div>

<script>
const ENTRY_MB      = ${entryMb};
const DRAIN_RATE    = ${DRAIN_RATE_MBPS};
const EXPECTED_SECS = ENTRY_MB / DRAIN_RATE;

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
      ? \`produced \${d.produced.toFixed(0)} MB \xb7 consumed \${d.consumed.toFixed(0)} MB \xb7 done \${d.t.toFixed(1)}s\`
      : \`produced \${d.produced.toFixed(0)} MB \xb7 consumed \${d.consumed.toFixed(0)} MB \xb7 backlog \${backlog} MB\`;
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
    const tMax = Math.max(latest.t, EXPECTED_SECS * 1.05, 0.1);
    const x = (t)  => (t  / tMax)    * W;
    const y = (mb) => H - (mb / ENTRY_MB) * H;

    ctx.lineWidth = 1;
    for (let i = 1; i <= 3; i++) {
      const yy = H * (1 - i / 4);
      ctx.strokeStyle = '#21262d';
      ctx.beginPath(); ctx.moveTo(0, yy); ctx.lineTo(W, yy); ctx.stroke();
      const label = (ENTRY_MB * i / 4).toFixed(0) + ' MB';
      ctx.fillStyle = '#484f58';
      ctx.font = (9 * dpr) + 'px ui-monospace,monospace';
      ctx.fillText(label, 4, yy - 3);
    }

    ctx.beginPath();
    ctx.moveTo(x(pts[0].t), y(pts[0].produced));
    for (const p of pts) ctx.lineTo(x(p.t), y(p.produced));
    for (let i = pts.length - 1; i >= 0; i--) ctx.lineTo(x(pts[i].t), y(pts[i].consumed));
    ctx.closePath();
    ctx.fillStyle = 'rgba(218,54,51,.35)';
    ctx.fill();

    ctx.beginPath();
    ctx.moveTo(x(pts[0].t), H);
    for (const p of pts) ctx.lineTo(x(p.t), y(p.consumed));
    ctx.lineTo(x(latest.t), H);
    ctx.closePath();
    ctx.fillStyle = 'rgba(63,185,80,.25)';
    ctx.fill();

    ctx.beginPath();
    for (const p of pts) ctx.lineTo(x(p.t), y(p.produced));
    ctx.strokeStyle = '#388bfd'; ctx.lineWidth = 2; ctx.stroke();

    ctx.beginPath();
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
		if (pathname === "/run/direct")  return makeSSEResponse((emit) => runDirect(emit, env));
		return new Response("Not found", { status: 404 });
	},
};
