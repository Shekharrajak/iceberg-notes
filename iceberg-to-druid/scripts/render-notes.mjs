import fs from 'node:fs/promises';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
const at = args.indexOf('--modules');
if (at < 0 || !args[at + 1]) throw new Error('Provide --modules /absolute/path/to/node_modules');
const require = createRequire(path.join(path.resolve(args[at + 1]), '__notes__.cjs'));
const { marked, Renderer } = require('marked');
if (!require('marked/package.json').version.startsWith('4.')) throw new Error('Requires marked 4.x');
const escape = value => String(value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
const markdown = await fs.readFile(path.join(root, 'README.md'), 'utf8');
const manifest = JSON.parse(await fs.readFile(path.join(root, 'diagram-manifest.json'), 'utf8'));
const sources = await Promise.all(manifest.map(item => fs.readFile(path.join(root, 'diagrams', `${item.id}.mmd`), 'utf8')));
const headings = [];
const renderer = new Renderer();
const heading = renderer.heading.bind(renderer);
const code = renderer.code.bind(renderer);
let diagramIndex = 0;
renderer.heading = (text, level, raw, slugger) => {
  const html = heading(text, level, raw, slugger);
  if (level === 2) headings.push({ id: html.match(/id="([^"]+)"/)[1], text });
  return html;
};
renderer.table = (header, body) => `<div class="table-scroll" tabindex="0" role="region" aria-label="Scrollable comparison table"><table><thead>${header}</thead><tbody>${body}</tbody></table></div>\n`;
renderer.code = (value, info, escaped) => {
  if (info !== 'mermaid') return code(value, info, escaped);
  const item = manifest[diagramIndex];
  if (!item || value.trim() !== sources[diagramIndex].trim()) throw new Error('Diagram sources differ; regenerate diagrams first');
  diagramIndex++;
  const title = escape(item.title);
  const stem = `diagrams/${item.id}`;
  return `<figure class="diagram" id="diagram-${item.id}"><figcaption><span>${title}</span><span class="diagram-tools"><button type="button" class="diagram-zoom" aria-pressed="false" aria-controls="view-${item.id}" hidden>Actual size</button><a href="${stem}.svg" target="_blank" rel="noopener">Open SVG</a><a href="${stem}.mmd">Mermaid</a></span></figcaption><div class="diagram-view" id="view-${item.id}" tabindex="0" role="region" aria-label="${title}"><img src="${stem}.svg" width="${item.width}" height="${item.height}" alt="${title}"></div></figure>\n`;
};
const content = marked.parse(markdown, { renderer, gfm: true, mangle: false });
if (diagramIndex !== manifest.length) throw new Error('Diagram count differs');
const nav = headings.map((item, index) => `<li><a href="#${item.id}"><span class="chapter-number">${String(index + 1).padStart(2, '0')}</span>${item.text.replace(/^\d+\.\s*/, '')}</a></li>`).join('\n');
const toc = `<details class="page-toc"><summary>On this page</summary><nav aria-label="On this page"><ul>${headings.map(item => `<li><a href="#${item.id}">${item.text}</a></li>`).join('\n')}</ul></nav></details>`;
const html = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Iceberg to Druid | Ingestion and serving notebook</title><link rel="stylesheet" href="assets/notes.css"><script defer src="assets/notes.js"></script></head>
<body><a class="skip-link" href="#main">Skip to content</a><div class="layout"><aside class="sidebar"><a class="brand" href="index.html">Iceberg to Druid<br><span>ingestion notebook</span></a><p class="sidebar-caption">Iceberg · Parquet · Arrow<br>Indexing · Segments · Queries</p><details class="chapter-nav" open><summary>Sections</summary><nav aria-label="Sections"><ol>${nav}</ol></nav></details><a class="gallery-link" href="../iceberg-batch-pipeline/index.html">Iceberg batch pipeline notebook</a></aside><main id="main"><header class="page-tools"><span>Source-backed technical notes</span><a class="source-link" href="README.md">Markdown source</a></header><article>${content.replace('</h1>', `</h1>\n${toc}`)}</article><nav class="pager" aria-label="Notebook navigation"><a href="../iceberg-batch-pipeline/index.html"><small>Related notebook</small>Iceberg batch pipeline</a><a href="#main"><small>Back to top</small>Iceberg to Druid</a></nav><footer>Rendered from the local Markdown source. Diagrams work offline. Use Actual size or Open SVG to inspect wide flows.</footer></main></div></body></html>\n`;
for (const asset of ['notes.css', 'notes.js']) {
  await fs.copyFile(path.join(root, '..', 'iceberg-batch-pipeline', 'assets', asset), path.join(root, 'assets', asset));
}
await fs.writeFile(path.join(root, 'index.html'), html);
console.log(`Rendered ${headings.length} sections and ${diagramIndex} diagrams with the batch notebook theme.`);
