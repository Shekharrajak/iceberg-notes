import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const memory = await fs.readFile(path.join(root, '13-arrow-memory-and-kernels.md'), 'utf8');
const hardware = await fs.readFile(path.join(root, '14-vectorization-and-hardware.md'), 'utf8');
const packBits = bits => bits.reduce((mask, bit, i) => mask | (Number(bit) << i), 0);
assert.equal(packBits([1, 0, 1, 1]), 0b00001101);
assert.ok(memory.includes('0b00001101'));

const values = [10, 20, null, 40, 5, 60, 7, 80];
const selected = values.flatMap((value, i) => value !== null && value > 25 ? [i] : []);
assert.deepEqual(selected, [3, 5, 7]);
assert.deepEqual(selected.map(i => values[i]), [40, 60, 80]);
assert.equal(packBits(values.map(value => value !== null && value > 25)), 0b10101000);
assert.ok(hardware.includes('0b10101000'));
assert.ok(hardware.includes('[3, 5, 7] -> selected values = [40, 60, 80]'));

const offsets = [0, 2, 2, 5, 5];
const validity = [true, false, true, true];
const strings = validity.map((valid, i) => valid ? 'abxyz'.slice(offsets[i], offsets[i + 1]) : null);
assert.deepEqual(strings, ['ab', null, 'xyz', '']);
assert.ok(memory.includes('offsets `[0, 2, 2, 5, 5]`'));

const rows = 8192;
const bitmapBytes = Math.ceil(rows / 8);
const int64Bytes = rows * 8 + bitmapBytes;
assert.equal(int64Bytes, 66560);
assert.equal(int64Bytes / 1024, 65);
assert.equal(3 * int64Bytes / 1024, 195);
const decimalDateBytes = rows * (16 + 16 + 16 + 4) + 4 * bitmapBytes;
assert.equal(decimalDateBytes, 430080);
assert.equal(decimalDateBytes / 1024, 420);
for (const value of ['66560', '65 KiB', '195 KiB', '430080', '420 KiB']) assert.ok(memory.includes(value), value);

assert.equal(10_000_000 * (8 + 8 + 8) / 40_000_000_000, 0.006);
assert.ok(hardware.includes('240000000 / 40000000000 = 0.006 seconds'));
assert.equal((1 / (0.70 + 0.30 / 2)).toFixed(3), '1.176');
assert.ok(hardware.includes('1 / (0.70 + 0.30 / 2) = 1.176x'));
console.log('PASS: illustrative validity/selection bitmaps, string offsets, batch memory, bandwidth and serial-fraction arithmetic.');
console.log('Documentation consistency only; no Arrow kernel, SIMD instruction, hardware profile or benchmark was executed.');
