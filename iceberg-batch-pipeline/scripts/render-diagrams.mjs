import fs from 'node:fs/promises';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(scriptDir, '..');
const args = process.argv.slice(2);
const option = name => {
  const at = args.indexOf(name);
  return at < 0 ? undefined : args[at + 1];
};
const modules = option('--modules');
const chrome = option('--chrome');
if (!modules || !chrome) {
  throw new Error('Provide --modules /absolute/node_modules and --chrome /absolute/chrome');
}
const requireFromModules = createRequire(path.join(path.resolve(modules), '__renderer__.cjs'));
const { default: puppeteer } = await import(pathToFileURL(requireFromModules.resolve('puppeteer')));
const mermaidPath = path.join(path.resolve(modules), 'mermaid', 'dist', 'mermaid.min.js');
const mermaidPackage = JSON.parse(await fs.readFile(path.join(modules, 'mermaid', 'package.json'), 'utf8'));
const fontFaces = await Promise.all([
  ['Ubuntu-Regular.woff2', 400],
  ['Ubuntu-Medium.woff2', 500],
  ['Ubuntu-Medium.woff2', 600],
  ['Ubuntu-Medium.woff2', 700]
].map(async ([file, weight]) => {
  const bytes = await fs.readFile(path.join(root, 'assets', file));
  return `@font-face{font-family:Ubuntu;font-style:normal;font-weight:${weight};src:url(data:font/woff2;base64,${bytes.toString('base64')}) format('woff2');}`;
}));
const fontCSS = `${fontFaces.join('\n')}\nbody,svg,text,tspan,.label{font-family:Ubuntu,Arial,sans-serif!important}`;
const slug = value => value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
const escape = value => value.replace(/[&<>"']/g, char => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
}[char]));
const chapters = (await fs.readdir(root)).filter(name => /^\d\d-.*\.md$/.test(name)).sort();
const diagrams = [];
for (const chapter of chapters) {
  const markdown = await fs.readFile(path.join(root, chapter), 'utf8');
  for (const match of markdown.matchAll(/```mermaid\n([\s\S]*?)\n```/g)) {
    const headings = [...markdown.slice(0, match.index).matchAll(/^#{1,6} (.+)$/gm)];
    const title = headings.at(-1)?.[1] ?? chapter;
    diagrams.push({
      chapter,
      title,
      id: `${chapter.slice(0, 2)}-${slug(title)}`,
      source: `${match[1].trim()}\n`,
      type: match[1].includes('sequenceDiagram') ? 'sequence' : match[1].includes('erDiagram') ? 'ER' : 'flowchart'
    });
  }
}
if (new Set(diagrams.map(item => item.id)).size !== diagrams.length) {
  throw new Error('Duplicate generated diagram IDs');
}
const output = path.join(root, 'diagrams');
await fs.mkdir(output, { recursive: true });
const previewDir = option('--preview-dir');
if (previewDir) await fs.mkdir(previewDir, { recursive: true });
const browser = await puppeteer.launch({
  executablePath: chrome,
  headless: true,
  args: ['--no-sandbox', '--disable-setuid-sandbox']
});
const manifest = [];
try {
  const page = await browser.newPage();
  await page.setViewport({ width: 1800, height: 1200, deviceScaleFactor: 1 });
  await page.setContent('<html><head></head><body style="margin:0;background:white"><div id="stage"></div></body></html>');
  await page.addStyleTag({ content: fontCSS });
  await page.addScriptTag({ path: mermaidPath });
  const fontCheck = await page.evaluate(async () => {
    await document.fonts.load('400 15px Ubuntu');
    await document.fonts.load('500 15px Ubuntu');
    await document.fonts.ready;
    return document.fonts.check('400 15px Ubuntu') && document.fonts.check('500 15px Ubuntu');
  });
  if (!fontCheck) throw new Error('Ubuntu font did not load');
  for (const [index, diagram] of diagrams.entries()) {
    const rendered = await page.evaluate(async ({ source, index }) => {
      const stage = document.getElementById('stage');
      stage.innerHTML = '';
      mermaid.initialize({
        startOnLoad: false,
        securityLevel: 'strict',
        theme: 'base',
        htmlLabels: false,
        fontFamily: 'Ubuntu, Arial, sans-serif',
        flowchart: { htmlLabels: false, curve: 'basis' },
        sequence: { useMaxWidth: false }
      });
      await mermaid.parse(source);
      const result = await mermaid.render(`diagram-${index}`, source);
      stage.innerHTML = result.svg;
      await document.fonts.ready;
      const svg = stage.querySelector('svg');
      const box = svg.viewBox.baseVal;
      const width = Math.ceil(box.width || svg.getBoundingClientRect().width);
      const height = Math.ceil(box.height || svg.getBoundingClientRect().height);
      if (width < 1 || height < 1) throw new Error('Invalid SVG dimensions');
      svg.style.maxWidth = 'none';
      svg.setAttribute('width', String(width));
      svg.setAttribute('height', String(height));
      const labels = [...svg.querySelectorAll('text')];
      if (!labels.length) throw new Error('Rendered diagram has no SVG text');
      const families = [...new Set(labels.map(node => getComputedStyle(node).fontFamily))];
      if (families.some(value => !value.includes('Ubuntu'))) {
        throw new Error(`Unexpected diagram font: ${families.join(', ')}`);
      }
      return { svg: new XMLSerializer().serializeToString(svg), width, height, labels: labels.length, families };
    }, { source: diagram.source, index });
    const svg = rendered.svg.replace(/(<svg\b[^>]*>)/, `$1<style>${fontCSS}</style>`);
    await fs.writeFile(path.join(output, `${diagram.id}.mmd`), diagram.source);
    await fs.writeFile(path.join(output, `${diagram.id}.svg`), svg);
    if (previewDir) {
      await page.setViewport({
        width: Math.min(5000, Math.max(800, rendered.width)),
        height: Math.min(5000, Math.max(600, rendered.height)),
        deviceScaleFactor: 1
      });
      await (await page.$('#stage svg')).screenshot({ path: path.join(previewDir, `${diagram.id}.png`) });
    }
    manifest.push({
      id: diagram.id, title: diagram.title, chapter: diagram.chapter, type: diagram.type,
      source: `${diagram.id}.mmd`, svg: `${diagram.id}.svg`,
      width: rendered.width, height: rendered.height, textElements: rendered.labels,
      fonts: rendered.families, fontLoaded: fontCheck
    });
    console.log(`${diagram.id}: ${rendered.width}x${rendered.height}, ${rendered.labels} labels`);
  }
} finally {
  await browser.close();
}
const navigation = manifest.map(item => `<li><a href="#${item.id}">${escape(item.title)}</a> <small>${escape(item.chapter.slice(0, 2))} / ${item.type}</small></li>`).join('\n');
const figures = manifest.map(item => `<section id="${item.id}"><h2>${escape(item.title)}</h2><p><a href="../${item.chapter}">Chapter notes</a> · <a href="${item.source}">Mermaid source</a> · <a href="${item.svg}">Open SVG</a></p><div class="viewport"><img src="${item.svg}" width="${item.width}" height="${item.height}" alt="${escape(item.title)}"></div></section>`).join('\n');
const gallery = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Iceberg batch pipeline diagrams</title><style>${fontCSS}
body{font-family:Ubuntu,Arial,sans-serif;margin:0;background:#f8fafc;color:#0f172a;line-height:1.5}main{max-width:1600px;margin:auto;padding:28px}h1{font-weight:500}h2{font-size:22px;font-weight:500}a{color:#075985}nav ul{columns:2;column-gap:48px;padding-left:20px}li{break-inside:avoid;margin:5px 0}small{color:#475569}section{margin:36px 0}section p{margin-top:0}.viewport{overflow:auto;background:white;border:1px solid #cbd5e1;border-radius:6px;padding:16px}.viewport img{display:block;max-width:none}@media(max-width:760px){main{padding:14px}nav ul{columns:1}}@media print{nav{display:none}.viewport{overflow:visible;border:0}.viewport img{max-width:100%;height:auto}section{break-inside:avoid}}</style></head>
<body><main><h1>Iceberg batch pipeline diagrams</h1><p><a href="../README.md">Notebook index</a>. ${manifest.length} source-backed diagrams. Panels retain natural size; scroll or open an SVG to inspect it. SVGs include their fonts and work offline.</p><nav aria-label="Diagram index"><ul>${navigation}</ul></nav>${figures}</main></body></html>`;
const linkedGallery = gallery.replace(/href="\.\.\/README\.md"/g, 'href="../index.html"')
  .replace(/href="\.\.\/(\d\d-[^"]+)\.md"/g, 'href="../$1.html"');
await fs.writeFile(path.join(output, 'index.html'), linkedGallery);
await fs.writeFile(path.join(output, 'manifest.json'), `${JSON.stringify({ mermaidVersion: mermaidPackage.version, diagrams: manifest }, null, 2)}\n`);
console.log(`Rendered ${manifest.length} diagrams with Mermaid ${mermaidPackage.version}; Ubuntu verified.`);
