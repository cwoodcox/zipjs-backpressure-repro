#!/usr/bin/env node
// Usage: node generate-zip.mjs [entry_mb [noise_period [dest_key]]]
//
//   entry_mb     decompressed entry size in MB          (default 1024)
//   noise_period 1 in N bytes is random, rest zeros     (default 5  → ~5:1 compression)
//   dest_key     R2 object key in zipjs-repro-data       (default large.zip)
//
// Generates the ZIP in memory then uploads it directly to R2 via wrangler.

import { BlobWriter, ZipWriter } from "@zip.js/zip.js";
import { spawnSync } from "child_process";

const [, , argMb, argNoise, argKey] = process.argv;
const ENTRY_MB     = argMb    ? parseInt(argMb,    10) : 1024;
const NOISE_PERIOD = argNoise ? parseInt(argNoise, 10) : 5;
const DEST_KEY     = argKey  ?? "large.zip";
const BUCKET       = "zipjs-repro-data";

if (isNaN(ENTRY_MB) || ENTRY_MB <= 0) { console.error("entry_mb must be a positive integer"); process.exit(1); }
if (isNaN(NOISE_PERIOD) || NOISE_PERIOD <= 0) { console.error("noise_period must be a positive integer"); process.exit(1); }

console.log(`generating ${ENTRY_MB} MB entry, noise_period=${NOISE_PERIOD} (~${Math.round(ENTRY_MB / NOISE_PERIOD)} MB compressed), key=${DEST_KEY}…`);

// XorShift32 — deterministic, no external dependency
let xstate = 0xDEADBEEF;
function xs32() {
	xstate ^= xstate << 13;
	xstate ^= xstate >>> 17;
	xstate ^= xstate << 5;
	return xstate >>> 0;
}

const buf = new Uint8Array(64 * 1024);
let remaining = ENTRY_MB * 1024 * 1024;
let byteIndex = 0;

const source = new ReadableStream({
	pull(controller) {
		if (remaining <= 0) { controller.close(); return; }
		const n = Math.min(remaining, buf.length);
		for (let i = 0; i < n; i++) {
			buf[i] = (byteIndex % NOISE_PERIOD === 0) ? (xs32() & 0xff) : 0;
			byteIndex++;
		}
		controller.enqueue(buf.slice(0, n));
		remaining -= n;
	},
});

const blobWriter = new BlobWriter("application/zip");
const zipWriter  = new ZipWriter(blobWriter);
await zipWriter.add("large.bin", source);
await zipWriter.close();
const blob  = await blobWriter.getData();
const bytes = Buffer.from(await blob.arrayBuffer());

console.log(`compressed: ${(bytes.length / 1024 / 1024).toFixed(1)} MB — uploading to ${BUCKET}/${DEST_KEY}…`);

const result = spawnSync(
	"npx", ["wrangler", "r2", "object", "put", `${BUCKET}/${DEST_KEY}`,
	        "--pipe", "--content-type", "application/zip", "--remote"],
	{ input: bytes, stdio: ["pipe", "inherit", "inherit"] },
);

if (result.status !== 0) {
	console.error(`upload failed (exit ${result.status})`);
	process.exit(result.status ?? 1);
}

console.log(`done: ${BUCKET}/${DEST_KEY}`);
