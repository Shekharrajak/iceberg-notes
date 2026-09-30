# Documentation validation

Checked on 2026-09-30 against the local revisions recorded in the source ledger. Engine tests and benchmarks were not run for this notebook. Named tests in the chapters are inspected coverage evidence, not passing results from this session.

| Check | Result |
| --- | --- |
| Root Markdown and source links | 12 Markdown files and 154 local links passed path, anchor and source-line checks |
| Mermaid syntax and rendering | All 29 diagrams parsed and rendered with Mermaid 11.17.2 |
| Generated-source parity | Every exported `.mmd` matches its Markdown source block |
| SVG XML | All 29 standalone SVGs passed `xmllint --noout` |
| Fonts | Ubuntu loaded before layout and is embedded in every SVG |
| Desktop gallery | All 29 images decoded at 1440 x 1100; no document-level horizontal overflow |
| Mobile gallery | All 29 images decoded at 390 x 844; no document-level horizontal overflow |
| Visual review | Inspected desktop/mobile gallery screenshots and representative flow/sequence diagrams, including the native scan and stream-execution paths |
| Scope | Existing notebook and presentation preserved; no engine source changes or dependency installation |

The offline gallery uses scrollable panels for wide diagrams; this is intentional, not a claim that each diagram fits a phone screen. Automated rendering checks do not establish implementation correctness or exhaustive absence of visual overlap.

## Formatted notebook

The browser notebook contains 13 generated HTML pages: the index, ten chapters, validation record and rendering guide. All 29 existing diagrams appear inline with their surrounding prose. Markdown remains the content source.

- Checked every page at 1440 x 1100 and 390 x 844: headings, diagram counts, image decoding, fonts, responsive navigation and diagram zoom controls passed.
- Verified 280 unique local links and assets, including chapter anchors; no network requests or browser errors occurred.
- Checked source hashes against the generated-page manifest to detect stale HTML.
- Inspected desktop and mobile screenshots of the formatted text and inline diagrams. Wide tables and enlarged diagrams scroll inside their panels; neither causes document-level horizontal overflow.
- Gallery chapter links open HTML. Explicit source links still open Markdown, Mermaid or local code files. Source-code line numbers remain in link tooltips; browsers do not jump to those lines.

Rendering uses marked 4.3.0 locally and reuses the verified SVGs. The browser needs no Markdown parser, Mermaid package, web server or internet connection. No technical chapter text, SVG content or presentation files changed for this formatting update.

See the renderer instructions for reproducible commands. Source references are tied to local checkouts and line numbers; rerun the validator after moving the notebook or updating those checkouts.
