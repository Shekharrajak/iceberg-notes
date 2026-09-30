import fs from 'node:fs/promises';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';

const args = process.argv.slice(2);
const option = name => args[args.indexOf(name) + 1];
if (!args.includes('--modules') || !args.includes('--chrome') || !args.includes('--screenshots')) {
  throw new Error('Provide --modules, --chrome and --screenshots paths');
}
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const requireFromModules = createRequire(path.join(path.resolve(option('--modules')), '__verify__.cjs'));
const { default: puppeteer } = await import(pathToFileURL(requireFromModules.resolve('puppeteer')));
const screenshots = path.resolve(option('--screenshots'));
await fs.mkdir(screenshots, { recursive: true });
const browser = await puppeteer.launch({
  executablePath: option('--chrome'), headless: true,
  args: ['--no-sandbox', '--disable-setuid-sandbox']
});
try {
  const page = await browser.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  for (const [name, width, height] of [['desktop', 1440, 1100], ['mobile', 390, 844]]) {
    await page.setViewport({ width, height, deviceScaleFactor: 1 });
    await page.goto(pathToFileURL(path.join(root, 'diagrams', 'index.html')).href, { waitUntil: 'load' });
    const result = await page.evaluate(async () => {
      await document.fonts.ready;
      const images = [...document.images];
      const decoded = await Promise.allSettled(images.map(image => image.decode()));
      return {
        images: images.length,
        allLoaded: images.every(image => image.naturalWidth > 0),
        decodeFailures: decoded.flatMap((result, index) => result.status === 'rejected' ? [images[index].getAttribute('src')] : []),
        fontLoaded: document.fonts.check('400 15px Ubuntu'),
        documentWidth: document.documentElement.scrollWidth,
        viewportWidth: innerWidth,
        scrollablePanels: [...document.querySelectorAll('.viewport')].filter(element => element.scrollWidth > element.clientWidth).length
      };
    });
    if (!result.allLoaded || !result.fontLoaded || result.documentWidth > width + 1 || errors.length) {
      throw new Error(JSON.stringify({ name, result, errors }));
    }
    await page.screenshot({ path: path.join(screenshots, `gallery-${name}.png`) });
    console.log(`${name}: ${JSON.stringify(result)}`);
  }
} finally {
  await browser.close();
}
