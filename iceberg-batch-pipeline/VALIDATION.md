# Documentation validation

Checked on 2026-09-30 against the local revisions recorded in the source ledger. Engine tests and benchmarks were not run for this notebook. Named tests in the chapters are inspected coverage evidence, not passing results from this session.

| Check | Result |
| --- | --- |
| Root Markdown and source links | 17 Markdown files and 274 local links passed path, anchor and source-line checks |
| Mermaid syntax and rendering | All 40 diagrams parsed and rendered with Mermaid 11.17.2 |
| Generated-source parity | Every exported `.mmd` matches its Markdown source block |
| SVG XML | All 40 standalone SVGs passed `xmllint --noout` |
| Fonts | Ubuntu loaded before layout and is embedded in every SVG |
| Desktop gallery | All 40 images decoded at 1440 x 1100; no document-level horizontal overflow |
| Mobile gallery | All 40 images decoded at 390 x 844; no document-level horizontal overflow |
| Visual review | Inspected all seven new Arrow/hardware/scan/rewrite diagrams and representative desktop/mobile pages; corrected ownership-diagram routing; previous TPC-H and scan/stream reviews remain applicable |
| TPC-H evidence consistency | Checked 22 transcribed timings, recalculated ratios/savings/totals, 61 column entries, 22 query descriptions and saved Q6 operator names |
| Source correspondence | All 22 timings match the source deck; all 61 names/types match `TPCH.scala`; four copied charts/plans are byte-identical to the preserved originals |
| Arrow teaching examples | Checked validity/selection masks, string offsets, batch-memory totals, bandwidth floor and serial-fraction arithmetic; these are illustrative calculations, not kernel tests |
| JavaScript syntax | All rendering and validation scripts passed `node --check` |
| Scope | Existing notebook and presentation preserved; no engine source changes or dependency installation |

The offline gallery uses scrollable panels for wide diagrams; this is intentional, not a claim that each diagram fits a phone screen. Automated rendering checks do not establish implementation correctness or exhaustive absence of visual overlap.

## Formatted notebook

The browser notebook contains 18 generated HTML pages: the index, fifteen chapters, validation record and rendering guide. All 40 diagrams and both preserved benchmark charts appear inline with their surrounding prose. Markdown remains the content source.

- Checked every page at 1440 x 1100 and 390 x 844: headings, diagram counts, image decoding, fonts, responsive navigation and diagram zoom controls passed.
- Verified local links and assets, including chapter anchors; no network requests or browser errors occurred.
- Checked source hashes against the generated-page manifest to detect stale HTML.
- Inspected desktop and mobile screenshots of the formatted text and inline diagrams. Wide tables and enlarged diagrams scroll inside their panels; neither causes document-level horizontal overflow.
- Gallery chapter links open HTML. Explicit source links still open Markdown, Mermaid or local code files. Source-code line numbers remain in link tooltips; browsers do not jump to those lines.

Rendering uses marked 4.3.0 locally and reuses the verified SVGs. The browser needs no Markdown parser, Mermaid package, web server or internet connection. The Arrow addendum adds three chapters and seven diagrams, updates the index/source ledger and keeps the earlier TPC-H evidence intact. Existing technical chapter sources 01-08 and 10-12 and presentation files were not edited for the Arrow addendum; generated HTML navigation was refreshed.

## Arrow and hardware evidence limits

Arrow 60.0.0 reference-source inspection is distinguished from Comet's resolved Arrow/Parquet 59.3.0 dependency. Relevant cached 59.3.0 kernels and the pinned Iceberg Rust reader/writer were inspected. Source links require the recorded repositories and, for dependency internals, local Cargo caches.

No Arrow/native engine test suite, assembly inspection, hardware-counter collection or scan/rewrite performance run was executed. Examples and hardware cost models explain mechanisms; they do not establish SIMD attribution for the saved TPC-H gains. Source-inspected tests describe intended correctness coverage, not new passing test results.

## Benchmark evidence limits

The historical SF1 table was transcribed from the existing presentation. Its displayed rows sum to 29.33 s Spark and 14.59 s Comet; the deck separately reports 14.56 s Comet. The notes retain both rather than treating rounded values as raw samples. Only two Q6 physical plans and the two chart images were recovered, not the complete result JSONs, event logs or all-query plans. Historical hash equality is reported by the deck, not independently revalidated here. No engine benchmark or performance profile was run for this update.

See the renderer instructions for reproducible commands. Source references are tied to local checkouts and line numbers; rerun the validator after moving the notebook or updating those checkouts.
