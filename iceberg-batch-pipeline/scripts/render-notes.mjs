import fs from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
const at = args.indexOf('--modules');
if (at < 0 || !args[at + 1]) throw new Error('Provide --modules /absolute/path/to/node_modules');
const requireModules = createRequire(path.join(path.resolve(args[at + 1]), '__notes__.cjs'));
const { marked, Renderer } = requireModules('marked');
const version = requireModules('marked/package.json').version;
if (!version.startsWith('4.')) throw new Error(`This renderer requires marked 4.x; found ${version}`);
const diagramManifest = JSON.parse(await fs.readFile(path.join(root, 'diagrams/manifest.json'), 'utf8'));
const files = ['README.md', ...(await fs.readdir(root)).filter(name => /^\d\d-.*\.md$/.test(name)).sort(), 'VALIDATION.md', 'scripts/README.md'];
const documents = new Map(await Promise.all(files.map(async file => [file, await fs.readFile(path.join(root, file), 'utf8')])));
const outputName = file => file === 'README.md' ? 'index.html' : file.replace(/\.md$/, '.html');
const escape = value => String(value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
const slug = value => value.toLowerCase().replace(/[^\w\s-]/g, '').replace(/\s/g, '-');
const titles = new Map([...documents].map(([file, text]) => [file, text.match(/^# (.+)$/m)?.[1] ?? file]));
const labels = ['Overview', 'System architecture', 'Iceberg scans', 'Parquet read and write', 'Runtime and serialization', 'Distributed execution', 'Writes and commits', 'Fault tolerance', 'Capabilities and gaps', 'Source ledger', 'DataFusion and connector', 'Validation', 'Rendering guide'];
const pages = [];

for (const [file, markdown] of documents) {
  const output = outputName(file);
  const relative = target => path.relative(path.dirname(output), target).split(path.sep).join('/');
  const chapterDiagrams = diagramManifest.diagrams.filter(item => item.chapter === file);
  const headings = [];
  const usedIds = new Map();
  let diagramIndex = 0;
  const renderer = new Renderer();
  renderer.heading = (text, level, raw) => {
    const base = slug(raw);
    const occurrence = usedIds.get(base) ?? 0;
    usedIds.set(base, occurrence + 1);
    const id = occurrence ? `${base}-${occurrence}` : base;
    headings.push({ id, level, text });
    return `<h${level} id="${escape(id)}">${text}</h${level}>\n`;
  };
  renderer.code = (code, info = '') => {
    if (info.trim() !== 'mermaid') return `<pre><code>${escape(code)}</code></pre>\n`;
    const item = chapterDiagrams[diagramIndex++];
    if (!item) throw new Error(`${file}: missing rendered diagram`);
    const source = diagramSources.get(item.id);
    if (code.trim() !== source.trim()) throw new Error(`${item.id}: regenerate diagrams before rendering notes`);
    const svg = relative(`diagrams/${item.svg}`);
    return `<figure class="diagram" id="diagram-${item.id}"><figcaption><span>${escape(item.title)}</span><span class="diagram-tools"><button type="button" class="diagram-zoom" aria-pressed="false" aria-controls="view-${item.id}" hidden>Actual size</button><a href="${svg}" target="_blank" rel="noopener">Open SVG</a><a class="source-link" href="${relative(`diagrams/${item.source}`)}">Mermaid</a></span></figcaption><div class="diagram-view" id="view-${item.id}" tabindex="0" role="region" aria-label="${escape(item.title)}"><img src="${svg}" width="${item.width}" height="${item.height}" alt="${escape(item.title)}"></div></figure>\n`;
  };
  renderer.table = (header, body) => `<div class="table-scroll" tabindex="0" role="region" aria-label="Scrollable comparison table"><table><thead>${header}</thead><tbody>${body}</tbody></table></div>\n`;
  renderer.html = html => escape(html);
  renderer.link = (href, title, text) => {
    let source = false;
    if (href.startsWith('/')) {
      const [, target, line] = href.match(/^(.*?)(?::(\d+))?$/);
      title = line ? `${target}, line ${line}; opens the local source file` : target;
      href = pathToFileURL(target).href;
      source = true;
    } else if (!/^[a-z][a-z0-9+.-]*:/i.test(href) && !href.startsWith('#')) {
      const [target, fragment] = href.split('#');
      const resolved = path.posix.normalize(path.posix.join(path.posix.dirname(file), target));
      if (documents.has(resolved)) href = `${relative(outputName(resolved))}${fragment ? `#${fragment}` : ''}`;
      else if (target.endsWith('.md')) source = true;
    } else if (/^(javascript|data):/i.test(href)) throw new Error(`Unsupported link: ${href}`);
    return `<a href="${escape(href)}"${title ? ` title="${escape(title)}"` : ''}${source ? ' class="source-link"' : ''}>${text}</a>`;
  };
  const diagramSources = new Map(await Promise.all(chapterDiagrams.map(async item => [item.id, await fs.readFile(path.join(root, 'diagrams', item.source), 'utf8')])));
  const content = marked.parse(markdown, { renderer, gfm: true, mangle: false, headerIds: false });
  if (diagramIndex !== chapterDiagrams.length) throw new Error(`${file}: rendered diagram count differs`);
  const chapterNav = files.map((name, index) => `<li><a href="${relative(outputName(name))}"${name === file ? ' aria-current="page"' : ''}><span class="chapter-number">${/^\d\d/.test(name) ? name.slice(0, 2) : ''}</span>${escape(labels[index])}</a></li>`).join('\n');
  const toc = headings.filter(item => item.level === 2).map(item => `<li><a href="#${escape(item.id)}">${item.text}</a></li>`).join('\n');
  const index = files.indexOf(file);
  const pager = [[index - 1, 'Previous'], [index + 1, 'Next']].map(([position, label]) => files[position] ? `<a href="${relative(outputName(files[position]))}"><small>${label}</small>${escape(labels[position])}</a>` : '<span></span>').join('');
  const html = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escape(titles.get(file))} | Iceberg batch notebook</title><link rel="stylesheet" href="${relative('assets/notes.css')}"><script defer src="${relative('assets/notes.js')}"></script></head>
<body><a class="skip-link" href="#main">Skip to content</a><div class="layout"><aside class="sidebar"><a class="brand" href="${relative('index.html')}">Iceberg batch<br><span>pipeline notebook</span></a><p class="sidebar-caption">Spark · Iceberg · Comet<br>DataFusion · Arrow</p><details class="chapter-nav" open><summary>Chapters</summary><nav aria-label="Chapters"><ol>${chapterNav}</ol></nav></details><a class="gallery-link" href="${relative('diagrams/index.html')}">Browse all 29 diagrams</a></aside><main id="main"><header class="page-tools"><span>Source-backed technical notes</span><a class="source-link" href="${relative(file)}">Markdown source</a></header><article>${content.replace(/(<\/h1>)/, `$1\n${toc ? `<details class="page-toc"><summary>On this page</summary><nav aria-label="On this page"><ul>${toc}</ul></nav></details>` : ''}`)}</article><nav class="pager" aria-label="Chapter pagination">${pager}</nav><footer>Rendered from the local Markdown sources. Diagrams work offline. Use Actual size or Open SVG to inspect wide flows.</footer></main></div></body></html>\n`;
  await fs.writeFile(path.join(root, output), html);
  pages.push({ source: file, html: output, title: titles.get(file), headings: headings.length, diagrams: chapterDiagrams.length, sha256: createHash('sha256').update(markdown).digest('hex') });
}
const galleryPath = path.join(root, 'diagrams/index.html');
const gallery = await fs.readFile(galleryPath, 'utf8');
await fs.writeFile(galleryPath, gallery.replace(/href="\.\.\/([^"#]+\.md)(#[^"]*)?"/g, (match, file, fragment = '') => documents.has(file) ? `href="../${outputName(file)}${fragment}"` : match));
await fs.writeFile(path.join(root, 'notes-manifest.json'), `${JSON.stringify({ markedVersion: version, pages }, null, 2)}\n`);
console.log(`Rendered ${pages.length} HTML pages with ${diagramManifest.diagrams.length} inline diagrams; gallery links use HTML.`);
