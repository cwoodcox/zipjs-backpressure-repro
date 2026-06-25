#!/usr/bin/env node
// Generates the test ZIP and writes large.zip for upload to R2.
// Run once: node generate-zip.mjs
// Then upload: npx wrangler r2 object put zipjs-repro-data/large.zip --file large.zip --remote
//
// The ZIP has a single entry ("large.bin") containing ENTRY_MB megabytes of
// zeros. Zeros compress ~1000:1 with deflate, so large.zip is tiny even for
// a multi-gigabyte decompressed entry.

import { BlobWriter, ZipWriter } from "@zip.js/zip.js";
import { writeFileSync } from "fs";

const ENTRY_MB = 512;

console.log(`generating ${ENTRY_MB} MB ZIP…`);

const zero = new Uint8Array(64 * 1024);
let remaining = ENTRY_MB * 1024 * 1024;
const source = new ReadableStream({
	pull(controller) {
		if (remaining <= 0) { controller.close(); return; }
		const n = Math.min(remaining, zero.length);
		controller.enqueue(zero.subarray(0, n));
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
console.log(`done: ${bytes.length} bytes → large.zip`);
console.log(`upload: npx wrangler r2 object put zipjs-repro-data/large.zip --file large.zip --remote`);
