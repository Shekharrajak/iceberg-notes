# Diagram rendering and validation

The Markdown chapters are the source of truth. `render-diagrams.mjs` extracts every fenced Mermaid block, parses and renders it with Mermaid, embeds the bundled Ubuntu font into each SVG, and writes an offline gallery plus a machine-readable manifest.

## Render

Use Node.js and an existing installation of Mermaid and Puppeteer. This collection was rendered with Mermaid 11.17.2, Puppeteer 23.11.1, Node.js 25.7.0 and Chrome Headless Shell 131. The renderer does not install packages, contact a CDN or modify the deck.

```bash
node scripts/render-diagrams.mjs \
  --modules /absolute/path/to/node_modules \
  --chrome /absolute/path/to/chrome-headless-shell
```

The modules directory must contain `mermaid` and `puppeteer`. A compatible local Chromium/Chrome executable is required. Optional `--preview-dir /absolute/path` saves verification PNGs outside the notes. The default notes root is resolved from the script's own location, so the command works from any working directory.

Outputs under `diagrams/` are generated: `.mmd`, `.svg`, `manifest.json` and `index.html`. Existing files with the same generated names are replaced when intentionally regenerating. The script does not delete stale diagrams after renaming a chapter; inspect the manifest and remove obsolete outputs deliberately if needed.

The gallery is offline and uses external SVG image files. Open it directly in a browser. Diagrams retain their natural size in scrollable panels and also have an SVG link for zooming. It does not require a web server or Mermaid installed to view.

## Render the complete notebook

```bash
node scripts/render-notes.mjs --modules /absolute/path/to/node_modules
```

This uses `marked` 4.x (verified with 4.3.0) from the supplied modules directory. Run it after rendering changed diagrams. It generates `index.html`, a matching HTML page for every chapter and supporting guide, and `notes-manifest.json`. It also makes the gallery's chapter links open HTML. No packages are installed and the presentation is unchanged.

Open `index.html` directly in a browser to read the prose, tables and diagrams together. Chapter links stay in the HTML notebook. Markdown and Mermaid source links remain available explicitly. Diagrams fit the reading width; use Actual size or Open SVG for detail. Source-code links open local files, with line references retained in their link tooltips; browser navigation does not jump to a code line.

The static pages use the bundled fonts and `assets/notes.css` / `assets/notes.js`. There is no runtime Markdown parser, Mermaid dependency, CDN or web-server requirement. Edit Markdown, not generated HTML, then rerun the renderer.

## Check notes

```bash
node scripts/validate-notes.mjs
```

The validator checks chapter links/anchors, local source paths and line numbers, Mermaid count/theme requirements, generated-source parity and SVG presence. Absolute source links require the recorded local repositories. On another machine, supply `--skip-source-paths` and use the source ledger's repository/revision mapping to locate code.

Use `--notes-parent /absolute/path/to/talk/notes` only when validating a staging copy whose link to the preserved older notebook lives in the final talk repository.

Check the TPC-H transcription and its documented arithmetic separately:

```bash
node scripts/verify-tpch-evidence.mjs
```

This verifies all 22 timing rows, ratios, totals, 61 documented columns, the query-map count and saved Q6 operator names. It does not rerun queries or validate historical correctness claims.

Check the Arrow/hardware teaching examples separately:

```bash
node scripts/verify-arrow-examples.mjs
```

This checks the documented validity/selection bitmaps, string offsets, batch-memory examples, bandwidth floor and serial-fraction arithmetic. It does not execute Arrow kernels or measure hardware performance.

## Check the offline gallery

```bash
node scripts/verify-gallery.mjs \
  --modules /absolute/path/to/node_modules \
  --chrome /absolute/path/to/chrome-headless-shell \
  --screenshots /absolute/path/to/verification-images
```

This opens the local gallery at desktop and mobile viewport sizes, checks image decoding, font availability and document overflow, and saves screenshots. Wide diagrams intentionally scroll inside their panels. Inspect representative flowcharts and sequence diagrams visually as well.

For the complete notebook, run the corresponding checks:

```bash
node scripts/verify-notes.mjs \
  --modules /absolute/path/to/node_modules \
  --chrome /absolute/path/to/chrome-headless-shell \
  --screenshots /absolute/path/to/verification-images
```

The notebook check visits every generated page at desktop and mobile sizes, checks source hashes, links, headings, diagram decoding, fonts, overflow and zoom controls, and captures representative pages. Wide tables scroll within their own panels.

## Font assets

`assets/Ubuntu-Regular.woff2` and `assets/Ubuntu-Medium.woff2` are embedded before layout and in the exported SVGs. Their licence is in `assets/Ubuntu-LICENSE.txt`. The renderer checks `document.fonts.check` after explicitly loading both weights. It records fonts, Mermaid version, diagram type and dimensions in the manifest.

The renderer validates syntax and basic SVG/text presence. It is not a substitute for visual inspection or engine correctness tests. `VALIDATION.md` records what was actually checked for this revision.
