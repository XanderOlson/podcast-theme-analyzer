# MVP Build Plan — Step 1: Ingestion

Scope: **Resolver → RSS fetch → Parse → Upsert.** Transcript discovery only from `<podcast:transcript>` tags. No HTML scraping. Idempotent. Conditional GET.

Assumptions: Python, Poetry, DuckDB, Requests, XML parser, CLI via Typer/Argparse. Paths under `podcast-theme-analayzer/ingestion/`.

---

## Phase 0 — Repo + Runtime

### 0.1 Create repo skeleton

* **Start:** Empty repo
* **End:** Folder tree from architecture with empty files
* **Test:** `tree` shows expected structure

### 0.2 Define toolchain config

* **Start:** No env management
* **End:** `pyproject.toml` or `requirements.txt` pinned; optional `.python-version`
* **Test:** `poetry install` or `pip install -r requirements.txt` succeeds

### 0.3 Config files

* **Start:** Missing config
* **End:** `config/defaults.yaml`, `.env.example` placeholders
* **Test:** YAML parses

### 0.4 Config loader

* **Start:** No loader
* **End:** Loader merges YAML + env overrides, returns immutable dict
* **Test:** Env override beats YAML in unit test

---

## Phase 1 — Storage (DuckDB + Layout)

### 1.1 DB opener

* **Start:** No DB file
* **End:** Opens/creates `data/ingestion.duckdb`, ensures dir exists
* **Test:** File created, connection usable

### 1.2 DDL: `shows`

* **End:** `shows(show_id TEXT PK, title, canonical_rss_url UNIQUE, publisher, lang, last_crawl_at TIMESTAMP)`
* **Test:** PRAGMA shows schema

### 1.3 DDL: `episodes`

* **End:** `episodes(episode_id TEXT PK, show_id, guid, title, pubdate, duration_s, audio_url, transcript_url, enclosure_type, explicit, episode_type, season_n, episode_n, first_seen_at, last_seen_at, tombstoned_bool)`
* **Test:** Columns present

### 1.4 DDL: `source_meta`

* **End:** `source_meta(resource_url PK, etag, last_modified, last_status, last_fetch_ts, content_sha256, bytes INTEGER)`
* **Test:** Insert/select works

### 1.5 DDL: `provenance`

* **End:** `provenance(object_type, object_id, source_url, fetched_at, parser_version, notes)`
* **Test:** Insert/select works

### 1.6 Migration bootstrap

* **End:** Idempotent init that runs all DDL once
* **Test:** Second run no-ops

---

## Phase 2 — Contracts + Types

### 2.1 In-memory models

* **End:** Types for `ResolverResponse`, `FeedFetchResponse`, `ParsedFeed`, `EpisodeRecord`
* **Test:** Type-check and serialize to dict

### 2.2 Invariants

* **End:** Validators for RSS URL, episode identity
* **Test:** Valid passes, invalid raises

---

## Phase 3 — Identifier Resolution

### 3.1 Pass-through RSS URL

* **End:** Valid HTTPS feed URL returns as canonical
* **Test:** URL in → same URL out

### 3.2 Apple ID lookup (gated)

* **End:** If `APPLE_LOOKUP_ENABLED=true`, map Apple ID → feed URL; else unsupported
* **Test:** Mock HTTP maps ID → URL

### 3.3 PodcastIndex ID lookup (optional)

* **End:** If keys present, map ID → feed URL; else unsupported
* **Test:** Mock response

### 3.4 Disambiguation guard

* **End:** Plain titles return `AMBIGUOUS_MATCH` in MVP
* **Test:** Title input returns error

---

## Phase 4 — Robots + Rate Limit

### 4.1 Robots fetcher

* **End:** Fetch and cache `/robots.txt` to `cache/robots/<host>.txt` with TTL
* **Test:** First call network, second cache

### 4.2 Robots allow check

* **End:** Allow/deny GET for URL per robots rules
* **Test:** Allow and deny fixtures

### 4.3 Per-host rate limiter

* **End:** Token bucket per host with QPS/burst from config
* **Test:** Rapid calls throttle

---

## Phase 5 — Feed Client (HTTP + Cache)

### 5.1 HTTP GET basic

* **End:** GET with UA, timeouts, gzip
* **Test:** Mock 200 returns body

### 5.2 Conditional GET

* **End:** Send `If-None-Match`/`If-Modified-Since` from `source_meta`
* **Test:** Mock 304 path exercised

### 5.3 Persist `source_meta`

* **End:** Upsert URL, etag, last\_modified, status, fetch\_ts, bytes, content\_sha256
* **Test:** Row matches headers

### 5.4 Body cache

* **End:** Save body to `cache/http/<sha256>`; dedupe by SHA
* **Test:** Re-fetch writes no new blob

### 5.5 Retry policy

* **End:** Retry 5xx with jitter; 429 honors `Retry-After`; no retry 4xx
* **Test:** Mock 500 then 200 retries once

---

## Phase 6 — Feed Parser

### 6.1 XML well-formedness

* **End:** Parse or raise `PARSER_INVALID_XML`
* **Test:** Bad XML raises

### 6.2 Show metadata

* **End:** Title, publisher, language from standard + iTunes tags
* **Test:** Fixture asserts values

### 6.3 Episodes list

* **End:** Extract guid, title, pubDate, duration, enclosure url/type, explicit, episode\_type, season/episode numbers
* **Test:** Fixture with 3 items → 3 records

### 6.4 Normalize dates/duration

* **End:** ISO UTC timestamp, integer seconds
* **Test:** Known strings map correctly

### 6.5 `<podcast:transcript>`

* **End:** Capture candidates `{url,type}`; rank machine-readable first
* **Test:** Multi-candidate ranking works

### 6.6 Build `ParsedFeed`

* **End:** Single structure `show` + `episodes[]`
* **Test:** Contract shape validated

---

## Phase 7 — Episode Normalizer

### 7.1 Episode ID

* **End:** `episode_id = guid` else `sha256(audio_url + title + pubdate_iso)`
* **Test:** Both branches

### 7.2 MIME normalization

* **End:** Map aliases to canonical, e.g., `audio/mp3` → `audio/mpeg`
* **Test:** Mapping holds

### 7.3 Bool/enum normalization

* **End:** `explicit` bool; `episode_type ∈ {full,trailer,bonus}`
* **Test:** Fixtures pass

---

## Phase 8 — Persistence

### 8.1 Upsert show

* **End:** Insert/update by `canonical_rss_url`; set `last_crawl_at`
* **Test:** No duplicates across runs

### 8.2 Upsert episodes

* **End:** Insert new with `first_seen_at`; always set `last_seen_at`; update mutable fields
* **Test:** Idempotency across two runs

### 8.3 Best transcript candidate

* **End:** Choose ranked best and store in `episodes.transcript_url`
* **Test:** Selection logic verified

### 8.4 Provenance rows

* **End:** One per show and per episode with `parser_version`
* **Test:** Counts match processed objects

---

## Phase 9 — Checkpoints + Delta

### 9.1 Save checkpoint

* **End:** `state/checkpoints/rss_cursor.json` with ETag/Last-Modified
* **Test:** File values match headers

### 9.2 Load checkpoint

* **End:** Read and apply conditional headers on next run
* **Test:** Uses prior ETag

### 9.3 Delta short-circuit

* **End:** On 304, exit before parse/upsert; log `NOT_MODIFIED`
* **Test:** DB unchanged in integration test

---

## Phase 10 — Orchestration (CLI)

### 10.1 `crawl_rss`

* **End:** CLI runs resolver → robots → fetch → parse → upsert
* **Test:** Fixture feed populates DB and prints counts

### 10.2 `refresh_delta`

* **End:** CLI reads checkpoint, conditional GET, diff, upsert
* **Test:** Run1 inserts N, run2 with 304 inserts 0

### 10.3 Structured logging

* **End:** JSON logs: trace\_id, rss\_url, episode\_id, lat\_ms, bytes, cache\_hit
* **Test:** Log line includes keys

---

## Phase 11 — Observability + SLO-lite

### 11.1 Counters

* **End:** Print bytes, episodes\_upserted, episodes\_skipped, status tallies
* **Test:** Values match DB and HTTP outcomes

### 11.2 Error taxonomy

* **End:** Map to `ID_NOT_FOUND`, `ROBOTS_DISALLOWED`, `HTTP_4XX`, `HTTP_5XX`, `PARSER_INVALID_XML`, `SCHEMA_VIOLATION`
* **Test:** Mocks trigger each code

---

## Phase 12 — Fixtures + Tests

### 12.1 Fixture: simple RSS

* **End:** `tests/fixtures/feed_simple.xml` with 2 items
* **Test:** File readable

### 12.2 Fixture: ETag + 304

* **End:** Mock or VCR cassette: 200 then 304
* **Test:** Client respects 304

### 12.3 Fixture: transcript tags

* **End:** Items with multiple `<podcast:transcript>` types
* **Test:** Picks machine-readable first

### 12.4 Tests: resolver

* **End:** URL passthrough, Apple ID mock, unsupported title
* **Test:** All pass

### 12.5 Tests: robots

* **End:** Allow and deny cases
* **Test:** Both verified

### 12.6 Tests: feed client

* **End:** 200 path, 304 path, 5xx retry once
* **Test:** Assertions on headers and retries

### 12.7 Tests: parser

* **End:** Show fields, episode fields, duration/date normalization, transcript extraction
* **Test:** Explicit assertions

### 12.8 Tests: normalizer

* **End:** ID, booleans, enums, MIME normalization
* **Test:** Assertions pass

### 12.9 Tests: persistence

* **End:** Upsert idempotency, timestamps, transcript selection
* **Test:** Row counts and timestamps

### 12.10 Integration: end-to-end

* **End:** `crawl_rss` on fixture populates DB; `refresh_delta` no-ops on 304
* **Test:** Script asserts all

---

## Phase 13 — Guardrails + Docs

### 13.1 User-Agent policy

* **End:** Configurable UA string
* **Test:** Request header contains UA

### 13.2 Secrets handling

* **End:** Env-only secrets; `.env.example`; `.gitignore` excludes `.env`
* **Test:** Missing key errors clearly; presence loads

### 13.3 README for MVP

* **End:** Install, init DB, crawl, refresh, test steps
* **Test:** Fresh clone reproduces integration pass

---

## Done Criteria

* Resolves RSS from URL and Apple ID when enabled
* Honors robots and per-host QPS
* Conditional GET with 304 short-circuit
* Parses episodes and `<podcast:transcript>`
* Idempotent upserts with `first_seen_at` and `last_seen_at`
* Provenance recorded
* `crawl_rss` and `refresh_delta` produce stable results on fixtures
