import fs from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';

const args = process.argv.slice(2);
const option = name => args.includes(name) ? args[args.indexOf(name) + 1] : undefined;
if (!option('--modules') || !option('--chrome') || !option('--screenshots')) {
  throw new Error('Provide --modules, --chrome and --screenshots paths');
}
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const requireModules = createRequire(path.join(path.resolve(option('--modules')), '__verify__.cjs'));
const { default: puppeteer } = await import(pathToFileURL(requireModules.resolve('puppeteer')));
const manifest = JSON.parse(await fs.readFile(path.join(root, 'notes-manifest.json'), 'utf8'));
const screenshots = path.resolve(option('--screenshots'));
await fs.mkdir(screenshots, { recursive: true });
for (const item of manifest.pages) {
  const markdown = await fs.readFile(path.join(root, item.source), 'utf8');
  if (createHash('sha256').update(markdown).digest('hex') !== item.sha256) throw new Error(`${item.source}: HTML is stale`);
}
const browser = await puppeteer.launch({ executablePath: option('--chrome'), headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] });
const checkedLinks = new Set();
const htmlCache = new Map();
try {
  const page = await browser.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  page.on('requestfailed', request => errors.push(`Asset failed: ${request.url()}`));
  await page.setRequestInterception(true);
  page.on('request', request => {
    if (/^https?:/.test(request.url())) {
      errors.push(`Unexpected network request: ${request.url()}`);
      request.abort();
    } else request.continue();
  });
  for (const [size, width, height] of [['desktop', 1440, 1100], ['mobile', 390, 844]]) {
    let diagrams = 0;
    for (const item of manifest.pages) {
      await page.setViewport({ width, height, deviceScaleFactor: 1 });
      await page.goto(pathToFileURL(path.join(root, item.html)).href, { waitUntil: 'load' });
      const result = await page.evaluate(async () => {
        await document.fonts.load('400 16px Ubuntu');
        await document.fonts.load('500 16px Ubuntu');
        await document.fonts.ready;
        const images = [...document.querySelectorAll('article img')];
        await Promise.all(images.map(image => image.decode()));
        return {
          headings: document.querySelectorAll('article h1,article h2,article h3,article h4,article h5,article h6').length,
          diagrams: document.querySelectorAll('.diagram img').length,
          brokenImages: images.filter(image => !image.naturalWidth).length,
          font: document.fonts.check('400 16px Ubuntu') && document.fonts.check('500 16px Ubuntu'),
          overflow: document.documentElement.scrollWidth > innerWidth + 1,
          rawMermaid: [...document.querySelectorAll('article pre code')].some(node => node.textContent.includes('%%{init:')),
          links: [...document.querySelectorAll('a[href],link[href],img[src],script[src]')].map(node => ({ url: node.href || node.src, source: node.classList.contains('source-link') })),
          navigationOpen: document.querySelector('.chapter-nav').open
        };
      });
      if (result.headings !== item.headings || result.diagrams !== item.diagrams || result.brokenImages || !result.font || result.overflow || result.rawMermaid || errors.length) {
        throw new Error(JSON.stringify({ page: item.html, size, result, errors }));
      }
      if (result.navigationOpen !== (width > 900)) throw new Error(`${item.html}: incorrect responsive navigation`);
      for (const link of result.links) {
        if (checkedLinks.has(link.url) || !link.url.startsWith('file:')) continue;
        checkedLinks.add(link.url);
        const url = new URL(link.url);
        let target = fileURLToPath(url);
        if (option('--notes-parent') && target.startsWith(`${path.dirname(root)}${path.sep}`) && !target.startsWith(`${root}${path.sep}`)) {
          target = path.resolve(option('--notes-parent'), path.relative(path.dirname(root), target));
        }
        await fs.access(target);
        if (target.endsWith('.md') && !link.source) throw new Error(`Reading link points to raw Markdown: ${link.url}`);
        if (url.hash && target.endsWith('.html')) {
          if (!htmlCache.has(target)) htmlCache.set(target, await fs.readFile(target, 'utf8'));
          const id = decodeURIComponent(url.hash.slice(1));
          if (!htmlCache.get(target).includes(`id="${id}"`)) throw new Error(`Missing HTML anchor: ${link.url}`);
        }
      }
      if (item.diagrams) {
        await page.evaluate(() => {
          for (const button of document.querySelectorAll('.diagram-zoom')) button.click();
        });
        const enlarged = await page.evaluate(() => ({
          count: document.querySelectorAll('.diagram.actual-size .diagram-zoom[aria-pressed="true"]').length,
          overflow: document.documentElement.scrollWidth > innerWidth + 1
        }));
        if (enlarged.count !== item.diagrams || enlarged.overflow) throw new Error(`${item.html}: zoom failed at ${size}`);
        await page.evaluate(() => {
          for (const button of document.querySelectorAll('.diagram-zoom')) button.click();
        });
      }
      if (['index.html', '02-iceberg-scan.html', '08-capabilities-and-debugging.html', '11-tpch-dataset-and-schema.html', '12-tpch-spark-versus-comet.html', '13-arrow-memory-and-kernels.html', '14-vectorization-and-hardware.html', '15-arrow-in-iceberg-scan-and-rewrite.html'].includes(item.html)) {
        await page.screenshot({ path: path.join(screenshots, `${path.basename(item.html, '.html')}-${size}.png`) });
      }
      if (item.html === '02-iceberg-scan.html') {
        await page.evaluate(() => document.getElementById('metadata-planning-flow').scrollIntoView({ behavior: 'instant' }));
        await page.screenshot({ path: path.join(screenshots, `scan-inline-${size}.png`) });
        await page.evaluate(() => document.getElementById('runtime-pruning-sequence').scrollIntoView({ behavior: 'instant' }));
        await page.screenshot({ path: path.join(screenshots, `scan-sequence-${size}.png`) });
      }
      const evidenceSections = {
        '11-tpch-dataset-and-schema.html': ['entity-relationships'],
        '12-tpch-spark-versus-comet.html': ['all-22-historic-timings', 'paired-q6-execution-flow', 'merge-on-read-evidence'],
        '13-arrow-memory-and-kernels.html': ['batch-and-buffer-ownership', 'c-interface-and-comet-handoff'],
        '14-vectorization-and-hardware.html': ['from-a-predicate-to-selected-rows', 'the-memory-hierarchy'],
        '15-arrow-in-iceberg-scan-and-rewrite.html': ['scan-work-reduction-ladder', 'rewrite-coordination-sequence', 'native-write-data-path']
      };
      for (const id of evidenceSections[item.html] ?? []) {
        await page.evaluate(id => document.getElementById(id).scrollIntoView({ behavior: 'instant' }), id);
        await page.screenshot({ path: path.join(screenshots, `${id}-${size}.png`) });
      }
      diagrams += item.diagrams;
    }
    console.log(`${size}: ${manifest.pages.length} pages, ${diagrams} diagrams, headings, fonts, links, zoom controls and no document overflow passed.`);
  }
  console.log(`PASS: ${checkedLinks.size} unique local links/assets; no network requests or browser errors.`);
} finally {
  await browser.close();
}
