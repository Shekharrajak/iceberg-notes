import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
const skipSourcePaths = args.includes('--skip-source-paths');
const parentAt = args.indexOf('--notes-parent');
const notesParent = parentAt < 0 ? undefined : path.resolve(args[parentAt + 1]);
const failures = [];
const files = (await fs.readdir(root)).filter(name => name.endsWith('.md')).sort();
const documents = new Map(await Promise.all(files.map(async name => [name, await fs.readFile(path.join(root, name), 'utf8')])));
const anchor = value => value.toLowerCase().replace(/[^\w\s-]/g, '').replace(/\s/g, '-');
const exists = async target => { try { await fs.access(target); return true; } catch { return false; } };
let linkCount = 0;
let diagramCount = 0;
for (const [name, text] of documents) {
  if (/[\u2013\u2014\u2192]/.test(text)) failures.push(`${name}: non-ASCII prose dash or arrow`);
  for (const match of text.matchAll(/\[[^\]]*\]\(([^)]+)\)/g)) {
    const href = match[1];
    if (/^https?:/.test(href)) continue;
    linkCount++;
    if (href.startsWith('/')) {
      if (skipSourcePaths) continue;
      const [, target, line] = href.match(/^(.*?)(?::(\d+))?$/);
      if (!(await exists(target))) failures.push(`${name}: missing source ${target}`);
      else if (line && Number(line) > (await fs.readFile(target, 'utf8')).split('\n').length) {
        failures.push(`${name}: line outside source ${href}`);
      }
      continue;
    }
    const [file, fragment] = href.split('#');
    let target = path.resolve(root, file || name);
    if (file.startsWith('../') && notesParent) target = path.resolve(notesParent, file.slice(3));
    if (!(await exists(target))) {
      failures.push(`${name}: missing link ${href}`);
      continue;
    }
    if (fragment && target.endsWith('.md')) {
      const body = await fs.readFile(target, 'utf8');
      const headings = [...body.matchAll(/^#{1,6} (.+)$/gm)].map(item => anchor(item[1]));
      if (!headings.includes(fragment)) failures.push(`${name}: missing anchor ${href}`);
    }
  }
  for (const match of text.matchAll(/```mermaid\n([\s\S]*?)\n```/g)) {
    diagramCount++;
    const source = match[1];
    if (!source.startsWith('%%{init:') || !source.includes('Ubuntu, Arial, sans-serif')) failures.push(`${name}: missing diagram theme/font`);
    if (source.includes('flowchart LR') && !source.includes('"curve":"basis"')) failures.push(`${name}: missing curved flowchart setting`);
    if (source.includes('sequenceDiagram') && !source.includes('box rgb(')) failures.push(`${name}: ungrouped sequence`);
  }
}
const manifest = JSON.parse(await fs.readFile(path.join(root, 'diagrams', 'manifest.json'), 'utf8'));
if (manifest.diagrams.length !== diagramCount) failures.push('Markdown and rendered diagram counts differ');
for (const item of manifest.diagrams) {
  const body = documents.get(item.chapter);
  const source = await fs.readFile(path.join(root, 'diagrams', item.source), 'utf8');
  if (!body?.includes(`\x60\x60\x60mermaid\n${source.trim()}\n\x60\x60\x60`)) failures.push(`${item.id}: generated Mermaid differs from Markdown`);
  const svg = await fs.readFile(path.join(root, 'diagrams', item.svg), 'utf8');
  if (!svg.includes('<svg') || !svg.includes('data:font/woff2;base64,') || !item.fontLoaded) failures.push(`${item.id}: invalid SVG or missing embedded font`);
}
if (failures.length) {
  console.error(failures.join('\n'));
  process.exitCode = 1;
} else {
  console.log(`PASS: ${documents.size} Markdown files, ${linkCount} local links, ${diagramCount} themed/rendered diagrams, generated-source parity and embedded fonts.`);
}
