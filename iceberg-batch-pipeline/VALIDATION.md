# Documentation validation

## October 2 Spark UI evidence addition

Chapter 19 preserves 14 SQL executions across seven JVM/Comet query-mode pairs, 20 annotated screenshots, saved SQL/physical plans, curated SQL metrics and a timing/provenance manifest. All seven selected pairs have matching schema, row count and result digest. Tables are calculated from five measured samples per engine, not from screenshot durations. Q18 is explicitly marked as an empty-result teaching example. No new benchmark or engine test suite was run while capturing this evidence. The historical validation counts below describe the earlier 18-chapter revision.

SQL screenshots highlight exchanges, joins/aggregates and row conversions; metric values are unchanged. Q3 DAG and Q9 stage/executor views supplement the operator screenshots. Full configurations and raw event logs are not published. The demo source and build limitations are recorded in chapter 19.

## September 30 notebook validation

Checked on 2026-09-30 against the local revisions recorded in the source ledger. Engine tests and benchmarks were not run for this notebook. Named tests in the chapters are inspected coverage evidence, not passing results from this session.

| Check | Result |
| --- | --- |
| Root Markdown and source links | 20 Markdown files and 394 local links passed path, anchor and source-line checks |
| Mermaid syntax and rendering | All 47 diagrams parsed and rendered with Mermaid 11.17.2 |
| Generated-source parity | Every exported `.mmd` matches its Markdown source block |
| SVG XML | All 47 standalone SVGs passed `xmllint --noout` |
| Fonts | Ubuntu loaded before layout and is embedded in every SVG |
| Desktop gallery | All 47 images decoded at 1440 x 1100; no document-level horizontal overflow |
| Mobile gallery | All 47 images decoded at 390 x 844; no document-level horizontal overflow |
| Visual review | Inspected all seven new metadata/distributed-planning/compaction/test/benchmark diagrams and representative desktop/mobile pages; previous Arrow, TPC-H and scan/stream reviews remain applicable |
| TPC-H evidence consistency | Checked 22 transcribed timings, recalculated ratios/savings/totals, 61 column entries, 22 query descriptions and saved Q6 operator names |
| Source correspondence | All 22 timings match the source deck; all 61 names/types match `TPCH.scala`; four copied charts/plans are byte-identical to the preserved originals |
| Arrow teaching examples | Checked validity/selection masks, string offsets, batch-memory totals, bandwidth floor and serial-fraction arithmetic; these are illustrative calculations, not kernel tests |
| Maintenance summary | Six saved timing samples, median/geometric-mean ratios and recorded row/file arithmetic passed; no engine correctness or provenance verdict |
| Iceberg teaching examples | Checked partial-progress group batching, illustrative descriptor-memory conversion and benchmark fixture row counts; these are arithmetic checks, not engine runs |
| JavaScript syntax | All rendering and validation scripts passed `node --check` |
| Scope | Correctness audit updates notes and presentation; no engine source changes or dependency installation |

The offline gallery uses scrollable panels for wide diagrams; this is intentional, not a claim that each diagram fits a phone screen. Automated rendering checks do not establish implementation correctness or exhaustive absence of visual overlap.

## Formatted notebook

The browser notebook contains 21 generated HTML pages: the index, eighteen chapters, validation record and rendering guide. All 47 diagrams and both preserved benchmark charts appear inline with their surrounding prose. Markdown remains the content source.

- Checked every page at 1440 x 1100 and 390 x 844: headings, diagram counts, image decoding, fonts, responsive navigation and diagram zoom controls passed.
- Verified 535 unique local links and assets from the saved notebook, including chapter anchors; no network requests or browser errors occurred.
- Checked source hashes against the generated-page manifest to detect stale HTML.
- Inspected desktop and mobile screenshots of the formatted text and inline diagrams. Wide tables and enlarged diagrams scroll inside their panels; neither causes document-level horizontal overflow.
- Gallery chapter links open HTML. Explicit source links still open Markdown, Mermaid or local code files. Source-code line numbers remain in link tooltips; browsers do not jump to those lines.

Rendering uses marked 4.3.0 locally and reuses the verified SVGs. The browser needs no Markdown parser, Mermaid package, web server or internet connection. The Arrow addendum adds three chapters and seven diagrams, updates the index/source ledger and keeps the earlier TPC-H evidence intact. Existing technical chapter sources 01-08 and 10-12 and presentation files were not edited for the Arrow addendum; generated HTML navigation was refreshed.

The metadata/distributed-scale addendum adds chapters 16-18 and seven diagrams. It updates the index, source ledger and renderer navigation while preserving historical benchmark evidence. The correctness audit regenerates the diagram exports; generated SVG identifiers can change even when a Mermaid block is unchanged.

## Iceberg scale evidence limits

Read source implementations and test assertions for metadata publication, manifest planning, file rewrites, concurrent deletes, partial progress and native write engagement. Inspected benchmark fixture construction and timed blocks separately: metadata-only replacement/rollback, real Spark compaction and three-arm Comet SQL writing measure different work.

Seven existing local XML reports were summarized, not rerun. One selected Comet native-scan report records cancellation after SparkContext shutdown; the exact binary/source provenance and shutdown cause were not established. Older zero-failure reports are not current-source passing verdicts. No new integration suite, cluster benchmark, fault-injection run or performance profile was executed. Local Spark fixtures do not establish multi-host behavior. Proposed commands and an experiment matrix are explicitly labeled as future validation.

## Arrow and hardware evidence limits

Arrow 60.0.0 reference-source inspection is distinguished from Comet's resolved Arrow/Parquet 59.3.0 dependency. Relevant cached 59.3.0 kernels and the pinned Iceberg Rust reader/writer were inspected. Source links require the recorded repositories and, for dependency internals, local Cargo caches.

No Arrow/native engine test suite, assembly inspection, hardware-counter collection or scan/rewrite performance run was executed. Examples and hardware cost models explain mechanisms; they do not establish SIMD attribution for the saved TPC-H gains. Source-inspected tests describe intended correctness coverage, not new passing test results.

## Benchmark evidence limits

The historical SF1 table was transcribed from the existing presentation. Its displayed rows sum to 29.33 s Spark and 14.59 s Comet; the historical deck separately reported 14.56 s Comet. The corrected deck now uses consistent rounded-row sums and ratios. The notes retain the historical discrepancy rather than treating rounded values as raw samples. Only two Q6 physical plans and the two chart images were recovered, not the complete result JSONs, event logs or all-query plans. Historical hash equality is reported by the deck, not independently revalidated here. No engine benchmark or performance profile was run for this update.

The existing physical compaction JSON is now preserved in this notebook and described in chapter 18. Its six timing samples, median ratio, paired geometric mean and row/file summary arithmetic were checked. This validates the saved summary's internal consistency, not its run provenance or logical correctness. Full plans, exact binary identity, executable fixture and per-trial verification logs were not recovered.

## Correctness audit

The audit distinguishes data and delete manifests, driver scan serialization, executor JVM TaskCommit reconstruction, planning-time native-write eligibility and uncertain commit outcomes. It separates position-delete rewriting from metadata/lifecycle maintenance, local Arrow FFI from serialized shuffle and final rows, and source-inspected single-column Z-order coverage from broader capability claims.

Memory and shuffle diagrams now state off-heap accounting scope, optionally compressed IPC, row versus encoded-byte buffering and shuffle-manager location lookup. Batch overhead is distinguished from Spark task scheduling. The deck labels non-Q6 performance explanations as hypotheses and replaces the scan-only-titled visual with an execution-comparison chart; historical assets remain unchanged.

The source/link, Mermaid parity, arithmetic, SVG XML, gallery and desktop/mobile notebook checks passed after regeneration. The presentation check passed for 21 slides and 21 speaker-note blocks, consistent rounded-row arithmetic and saved maintenance medians. Browser inspection visited every slide with images decoded and no script errors; only the pre-existing decorative cover illustration intentionally extends beyond its slide. The corrected runtime, query-shape and benchmark visuals were inspected at presentation size.

Each speaker-note block has anchor keywords, a short horizontal mental-model chain and a plain-language takeaway. The cue structure is checked automatically; source claims and evidence boundaries still require technical review. These are documentation checks, not engine integration or performance tests.

See the renderer instructions for reproducible commands. Source references are tied to local checkouts and line numbers; rerun the validator after moving the notebook or updating those checkouts.
