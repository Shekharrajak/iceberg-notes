import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = name => fs.readFile(path.join(root, name), 'utf8');
const evidence = JSON.parse(await read('assets/tpch/historic-sf1-transcription.json'));
const results = await read('12-tpch-spark-versus-comet.md');
const schema = await read('11-tpch-dataset-and-schema.md');
assert.equal(evidence.kind, 'transcribed_presentation_values_not_raw_benchmark_results');
assert.deepEqual(evidence.queries.map(row => row.query), Array.from({ length: 22 }, (_, i) => i + 1));
const timingRows = [...results.matchAll(/^\| Q(\d+) \| ([\d.]+) \| ([\d.]+) \| ([\d.]+)x \| ([\d.]+) \|$/gm)];
assert.equal(timingRows.length, 22);
for (const [i, row] of timingRows.entries()) {
  const expected = evidence.queries[i];
  assert.equal(Number(row[1]), expected.query);
  assert.equal(Number(row[2]), expected.spark_seconds);
  assert.equal(Number(row[3]), expected.comet_seconds);
  assert.equal(row[4], (expected.spark_seconds / expected.comet_seconds).toFixed(2));
  assert.equal(row[5], (expected.spark_seconds - expected.comet_seconds).toFixed(2));
}
const spark = evidence.queries.reduce((sum, row) => sum + Math.round(row.spark_seconds * 100), 0) / 100;
const comet = evidence.queries.reduce((sum, row) => sum + Math.round(row.comet_seconds * 100), 0) / 100;
const geometricMean = Math.exp(evidence.queries.reduce((sum, row) => sum + Math.log(row.spark_seconds / row.comet_seconds), 0) / 22);
assert.equal(spark.toFixed(2), '29.33');
assert.equal(comet.toFixed(2), '14.59');
for (const number of [(spark / comet).toFixed(4), (spark - comet).toFixed(2), geometricMean.toFixed(4), ((1 - comet / spark) * 100).toFixed(2)]) {
  assert.ok(results.includes(number), `Summary missing calculated value ${number}`);
}
assert.equal(evidence.reported_summary.comet_seconds, 14.56);
const columns = [...schema.matchAll(/^\| `([a-z]+_[a-z]+)` \| (BIGINT|INT|STRING|DATE|DECIMAL\(12,2\)) \|/gm)];
assert.equal(columns.length, 61);
assert.equal(new Set(columns.map(row => row[1])).size, 61);
const expectedColumns = { r: 3, n: 4, s: 7, c: 8, p: 9, ps: 5, o: 9, l: 16 };
for (const [prefix, count] of Object.entries(expectedColumns)) {
  assert.equal(columns.filter(row => row[1].startsWith(`${prefix}_`)).length, count, prefix);
}
assert.equal([...schema.matchAll(/^\| Q\d+ \|/gm)].length, 22);
const sparkPlan = await read('assets/tpch/tpch-q6-spark-iceberg.plan.txt');
const cometPlan = await read('assets/tpch/tpch-q6-comet-iceberg.plan.txt');
for (const name of ['BatchScan', 'ColumnarToRow', 'HashAggregate', 'SinglePartition']) assert.ok(sparkPlan.includes(name), name);
for (const name of ['CometIcebergNativeScan', 'CometFilter', 'CometProject', 'CometHashAggregate', 'CometNativeShuffle', 'CometColumnarToRow']) assert.ok(cometPlan.includes(name), name);
console.log(`PASS: 22 transcribed timing rows, ratios and savings; totals ${spark.toFixed(2)}/${comet.toFixed(2)} s; geometric mean ${geometricMean.toFixed(4)}x; 61 columns, 22 query descriptions and saved Q6 operator names.`);
console.log('This checks documentation consistency, not benchmark correctness or historical provenance.');
