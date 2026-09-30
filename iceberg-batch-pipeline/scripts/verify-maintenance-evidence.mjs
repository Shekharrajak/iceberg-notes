import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const evidence = JSON.parse(await fs.readFile(path.join(root, 'assets/maintenance/iceberg-maintenance-jvm-vs-comet.json'), 'utf8'));
const chapter = await fs.readFile(path.join(root, '18-iceberg-tests-and-benchmark-evidence.md'), 'utf8');
const results = evidence.results;
const median = samples => [...samples].sort((a, b) => a - b)[Math.floor(samples.length / 2)];
for (const arm of ['jvm', 'comet']) {
  const samples = results[`${arm}_seconds`];
  assert.equal(samples.length, 3);
  assert.ok(samples.every(value => Number.isFinite(value) && value > 0));
  assert.equal(median(samples), results[`${arm}_median_seconds`]);
  for (const sample of samples) assert.ok(chapter.includes(sample.toFixed(6)));
}
const ratio = results.jvm_median_seconds / results.comet_median_seconds;
const pairedMean = Math.exp(results.jvm_seconds.reduce((sum, value, index) => sum + Math.log(value / results.comet_seconds[index]), 0) / 3);
assert.equal(Number(ratio.toFixed(3)), results.median_speedup);
assert.equal(Number(pairedMean.toFixed(3)), results.paired_geometric_mean_speedup);
assert.ok(chapter.includes(`${ratio.toFixed(2)}x`));
assert.ok(chapter.includes(`${pairedMean.toFixed(3)}x`));
assert.equal(evidence.input.visible_rows, evidence.input.physical_rows * (1 - evidence.input.delete_percent / 100));
assert.equal(evidence.correctness.output_physical_rows, evidence.input.visible_rows);
assert.equal(evidence.correctness.row_count, evidence.input.visible_rows);
assert.equal(evidence.correctness.remaining_delete_files, 72);
assert.equal(evidence.correctness.rewritten_data_files, evidence.input.data_files);
assert.ok(chapter.includes('72 remaining delete files'));
console.log(`PASS: six saved timing samples; median ratio ${ratio.toFixed(3)}x, paired geometric mean ${pairedMean.toFixed(3)}x and recorded row/file arithmetic.`);
console.log('This checks summary consistency, not engine correctness, raw metric provenance or benchmark reproduction.');
