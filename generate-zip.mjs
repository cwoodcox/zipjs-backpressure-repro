#!/usr/bin/env node
// Generates the test ZIP and writes large.zip for upload to R2.
// Run once: node generate-zip.mjs
// Then upload: npx wrangler r2 object put zipjs-repro-data/large.zip --file large.zip --remote
//
// The ZIP has a single entry ("large.bin") of ENTRY_MB megabytes.
// Data is every NOISE_PERIOD-th byte random (XorShift32), the rest zeros.
// At NOISE_PERIOD=8 this gives ~8:1 compression — the .zip is ~65 MB rather
// than the ~522 KB produced by all-zeros.  A less extreme compression ratio
// better reflects real-world archives and avoids inflating from a trivially
// small source (zeros let getData() run to completion almost instantly, which
// is worse than any real file would be).

import { BlobWriter, ZipWriter } from "@zip.js/zip.js";
import { writeFileSync } from "fs";

const ENTRY_MB     = 512;
const NOISE_PERIOD = 8; // 1 in NOISE_PERIOD bytes is pseudo-random → ~8:1 ratio

console.log(`generating ${ENTRY_MB} MB ZIP (~${Math.round(ENTRY_MB / NOISE_PERIOD)} MB compressed)…`);

// XorShift32 — deterministic, no external dependency
let xstate = 0xDEADBEEF;
function xs32() {
	xstate ^= xstate << 13;
	xstate ^= xstate >>> 17;
	xstate ^= xstate << 5;
	return xstate >>> 0;
}

const buf = new Uint8Array(64 * 1024);
let remaining  = ENTRY_MB * 1024 * 1024;
let byteIndex  = 0;

const source = new ReadableStream({
	pull(controller) {
		if (remaining <= 0) { controller.close(); return; }
		const n = Math.min(remaining, buf.length);
		for (let i = 0; i < n; i++) {
			buf[i] = (byteIndex % NOISE_PERIOD === 0) ? (xs32() & 0xff) : 0;
			byteIndex++;
		}
		controller.enqueue(buf.slice(0, n)); // slice copies — buf is reused each pull
		remaining -= n;
	},
});

const blobWriter = new BlobWriter("application/zip");
const zipWriter  = new ZipWriter(blobWriter);
await zipWriter.add("large.bin", source);
await zipWriter.close();
const blob  = await blobWriter.getData();
const bytes = new Uint8Array(await blob.arrayBuffer());

writeFileSync("large.zip", bytes);
console.log(`done: ${bytes.length} bytes (${(bytes.length / 1024 / 1024).toFixed(1)} MB) → large.zip`);
console.log(`upload: npx wrangler r2 object put zipjs-repro-data/large.zip --file large.zip --remote`);
