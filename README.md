# Polymarket market data

Finite local collection of Polymarket historical price samples, public displayed
books, free PMXT archives and optional paid historical books/trades. MIT software;
no trading interface, scheduler, hosted service or bundled production data.

`polymarket-events` owns Gamma metadata. This package reads its immutable
`oddsfox.polymarket.metadata.v1` bundles. It runs without the old
`oddsfox-pipeline` checkout and does not read or modify its state.

## Install and run

Use Python 3.12 and `uv sync --locked`. `uv run polymarket-market-data --help`
shows the CLI. Installed commands work outside this checkout; `--root` defaults
to `./data` relative to the operator's current directory. Roots inside a Git
checkout must be ignored. Credentials are supplied only through the environment.

The free workflow refreshes and pins targeted metadata, seals a plan, collects,
builds, verifies, publishes and verifies the whole root in one command. Select
feeds explicitly; `run` accepts only these four free feeds. Repeat `--market-id`
for up to five representative markets during initial acceptance.

```sh
polymarket-market-data --root /absolute/path/market-data run \
  --market-id 667082 \
  --feed clob-history --feed clob-ws --feed pmxt-v1 --feed pmxt-v2 \
  --start 2026-04-14T08:00:00Z --end 2026-04-14T09:00:00Z \
  --duration 60 --events-executable /absolute/path/to/catalogue
```

This single historical window applies to both history and archives. Live
recording starts now. A historical market may have no current live snapshots;
completion does not establish readiness. An active-market run can explicitly
select only `clob-history` and `clob-ws` with a recent UTC window.

`run` defaults to 1,000 attempts, 4 GiB downloaded, 16 GiB whole-root storage,
2 GiB temporary resources, and a 16 MiB individual response limit. Override with
`--max-requests`, `--max-download-bytes`, `--max-storage-bytes`,
`--max-temp-bytes`, and `--max-response-bytes`. The request/download caps span
metadata refreshes, retries, partial responses, WebSocket ingress, and every
resume of that run. JSON reports identify the journal/report, plan, acquisitions,
release, source counts, coverage, limits and recovery command. `requests` and
`downloaded_bytes` measure this invocation; `measured_usage` and `charged_usage`
are lifetime totals. Inspect `accounting_complete` and the charged totals before
claiming exact usage. Coverage includes legitimate empty observations and missing
hours, which cannot satisfy a nonempty acceptance case.

```sh
polymarket-market-data --root /absolute/path/market-data run --resume-run RUN_ID
```

Resume reuses pinned inputs and verified checkpoints. Omitted arguments retain
sealed values; changed selections, settings, executable or limits are rejected.
A completed run verifies its existing result and makes no acquisition requests.
An interrupted live acquisition remains preserved; a fresh session starts only
with remaining allowance. Reservations are written before I/O and refunded only
after proven transport closure or validated producer accounting. Unknown crashes
keep their allowances charged, including the full remaining grant for an
interrupted live session or metadata subprocess. Never delete or reconstruct a
run journal to recover allowance. Exhausted runs fail closed. Earlier valid
publication pointers survive a failed publication.

The producer receives managed paths and explicit budgets. Inherited
`CATALOGUE_*` configuration and the paid API key are removed; execution is bounded
to 120 seconds and 64 KiB of diagnostics.

## Individual operations

First obtain a targeted bundle from `polymarket-events`:

```sh
catalogue metadata refresh --market-id 123 --output /absolute/path/metadata
```

Use the returned manifest SHA256 as an explicit trusted pin. Select market IDs,
feeds and a half-open UTC history window. Every valid native outcome of those
selected markets is included; labels never determine complements.

```sh
polymarket-market-data --root ./data plan \
  --metadata /absolute/path/metadata --metadata-sha256 MANIFEST_SHA256 \
  --market-id 123 --feed clob-history --feed clob-ws --feed pmxt-v2 \
  --start 2025-01-01T00:00:00Z --end 2025-01-01T01:00:00Z \
  --fidelity 1 --duration 60 \
  --max-requests 1000 --max-download-bytes 536870912 \
  --max-storage-bytes 10737418240 --max-response-bytes 16777216 \
  --max-temp-bytes 536870912
```

Those example limits are the finite defaults for the individual `plan` command;
`run` uses the larger workstation profile above. A plan pins the metadata,
requested sources, target identities, window, fidelity, live duration and limits.
Historical bounds must use whole seconds. Live duration is 1–86,400 seconds and
starts when recording begins, independent of the historical window.

## Collect and publish

```sh
polymarket-market-data --root ./data prices backfill --plan PLAN_ID
polymarket-market-data --root ./data archive backfill --plan PLAN_ID --version v2
polymarket-market-data --root ./data books record --plan PLAN_ID \
  --events-executable /absolute/path/to/catalogue \
  --events-root /absolute/path/to/polymarket-events
```

Live recording refreshes selected metadata through the configured events
executable before subscribing. It freezes identities for the session and refuses
changed identities. Lifecycle notifications trigger metadata refresh and
reconnection; reconnection always requires fresh snapshots. One session supports
at most 500 distinct wire IDs. Identical numeric IDs in different ledgers remain
distinct stored assets, but cannot share an ambiguous wire subscription.

Commands return JSON containing acquisition IDs and measured requests/bytes.
Build from explicit completed acquisitions; include every feed in the plan.

```sh
polymarket-market-data --root ./data build --plan PLAN_ID \
  --acquisition PRICE_ACQUISITION --acquisition ARCHIVE_ACQUISITION \
  --acquisition LIVE_ACQUISITION
polymarket-market-data --root ./data verify --release RELEASE_ID
polymarket-market-data --root ./data publish --release RELEASE_ID
polymarket-market-data --root ./data status
```

Publication replaces only that plan's current pointer. Other plans and immutable
releases remain intact. Capture completion means all work is accounted for; it
does not promise uninterrupted book coverage or observations in every bucket.
Missing objects, unobserved intervals, empty responses, empty books and unknown
capabilities are distinct. Malformed data, unfinished acquisitions and saturated
paid leaves cannot promote a release.

Root-level `verify` and `status` also validate every active plan pointer against
its release manifest and reject malformed, misassigned or invalidated pointers.
`verify --release` checks that immutable release's evidence independently.

## Source support and interpretation

| Feed | CTF tokens | Protocol V2 positions |
|---|---|---|
| CLOB price history | Supported request identity | Supported request identity |
| Public market WebSocket | Supported | Supported |
| PMXT archive v1/v2 | Supported schemas | Capability unknown; not interpreted as CTF |
| Paid PMXT books/trades | Supported native targets | Capability unknown; no paid probes |

Support for an identifier does not guarantee historical retention. CLOB responses
are sampled prices, not a trade tape; an empty successful response is not proof
of no trading. Free archive objects are hourly, read in their original physical row order
and retained with their hash before decoding. Sequential row groups use
dictionary-preserving Arrow batches of at most 2,048 rows, with decoding threads
and prefetch disabled. Condition and receive-time filters run before selected
rows become Python values. The 2 GiB profile permits at most 134,217,728 source
rows per object; predecode row-group, dictionary, selected-string and normalized
output guards remain finite. Whole-file uncompressed size alone does not reject
an otherwise bounded object. A missing hour is a gap. Differing archive updates
at the same receive time invalidate that book until a later unambiguous snapshot.
State is not carried across independent archive hours or connections.
Changed archive objects have distinct replay streams. Reingesting identical
objects suppresses identical normalized positions while preserving a deterministic
ingestion receipt.

The output contract is `oddsfox.polymarket.market-data.v1`, containing typed
Parquet relations `instruments`, `price_samples`, `book_updates`, `book_states`,
`trades`, `sampled_ohlc` and `coverage`, plus a checksummed manifest and pinned plan.
Empty relations retain their schemas. `(venue, asset_kind, asset_id)` is the
native identity; outcome ordinals are one-based source ordinals, never chain
index sets. Financial values are exact decimals. Source time, receive time and
ingestion time remain separate. Book depth is displayed liquidity, not guaranteed
execution.

Price facts and sampled OHLC retain acquisition/capture identities. Selecting
several acquisitions preserves revised observations rather than choosing a
latest value silently. OHLC is per source capture and observed minute; missing
minutes are not interpolated. Provider trade IDs are deduplicated per typed asset;
conflicting facts for the same ID fail validation.

## Optional paid acquisition

Create a separate plan containing `pmxt-books`, `pmxt-trades` or both. Set
`PMXT_API_KEY` securely in the environment, then explicitly opt in:

```sh
polymarket-market-data --root ./data books backfill --plan PAID_PLAN \
  --enable-paid --account ACCOUNT_LABEL --period 2025-01 \
  --allowance-id ALLOCATED_ALLOWANCE --allowance-cap 100
polymarket-market-data --root ./data trades backfill --plan PAID_PLAN \
  --enable-paid --account ACCOUNT_LABEL --period 2025-01 \
  --allowance-id ALLOCATED_ALLOWANCE --allowance-cap 100
```

Both commands share one durable SQLite request ledger under this root. Every
actual attempt, including retries and ambiguous failures, reserves allowance
before HTTP. The account-period allocation cannot be replaced to reset usage.
Response saturation splits the window down to whole milliseconds; saturation at
one millisecond is an explicit failure. Books and trades are independent; empty
completed windows are publishable.

The allowance is a request count allocated to this root, not a currency estimate
or an account-wide cap. The old pipeline and other roots/users consume additional
credits. Separate API keys do not establish separate account allowances.

## Recovery and operating limits

- Resume historical or archive work with the same `--plan` and `--resume
  ACQUISITION_ID`. Verified committed requests and units are reused. A new
  acquisition deliberately observes the source again. An unfinished archive hour
  with committed batches replays its pinned original object, even if the remote
  object changes. Cached HTTP responses retain their original receive timestamps.
- Live sessions cannot resume. An interrupted session remains incomplete; start
  a new session and preserve the earlier gap evidence.
- Each invocation counts requests, retries and partially downloaded bytes.
  Temporary space and whole-root storage are bounded. Large archives may require
  a larger explicit plan allowance even for a small selected market set. Archive
  footer row-group sizes and row counts are checked before deserialization;
  decoded batches, source JSON rows and normalized batches also have finite caps.
  Live metadata refresh receives the remaining invocation request/download caps
  and the plan's output/response caps. Initial and reconnect refreshes share the
  recorder's budget; failed refresh attempts and partial responses count too.
  Temporary producer output is removed after pinning the validated copy.
- Builds use one worker, bounded DuckDB operations and a conservative derived-row
  memory allowance. Reduce the plan window if the build limit is exhausted.
- No operation automatically deletes captures, releases or checkpoints. Do not
  reset or reconstruct the paid ledger from captures: failed paid attempts may
  have no successful response.
- Stop all writers before copying the complete data root for backup. Include
  `.state`, pinned inputs, plans, captures, objects and publications. Verify the
  restored root with `verify` and check `status`; source replay cannot reconcile
  a missing financial ledger.

WebSocket download accounting includes connection ingress, partial fragments and control frames; compression is disabled. The message and queue allocation are capped by the sealed temporary-byte limit. Paid accounting also reserves SQLite database and journal headroom before writes.

Production readiness requires bounded real cases for every selected typed asset:
current history, authoritative live snapshots and fresh snapshots after reconnect,
and relevant archive updates with valid reconstructed states for each version. Automated tests are offline; they do not
establish current provider retention or account pricing.

## Development

`scripts/verify-fast` runs lint, formatting checks and tests. `scripts/verify`
also verifies the lock, installs locked dependencies and builds distributions.
Use the repository's Universal Pad engineering workspace for material changes.
See `THIRD_PARTY_NOTICES.md` for reused MIT code and PMXT data attribution.

## Repeatable live smoke test

This is opt-in; ordinary tests and CI stay offline. Run with an installed package
outside either checkout, or with `uv run --locked` in this repository. The events
metadata smoke report must pass with complete accounting; its bundle and digest
are validated. The executable is supplied explicitly.

```sh
python -m polymarket_market_data.smoke \
  --events-report /absolute/path/events/report.json \
  --events-executable /absolute/path/to/catalogue \
  --case active --expected-asset-kind ctf_token --duration 60 \
  --output /absolute/path/fresh-ctf-root

python -m polymarket_market_data.smoke \
  --events-report /absolute/path/v2-events/report.json \
  --events-executable /absolute/path/to/catalogue \
  --case active --expected-asset-kind poly_v2_position --duration 60 \
  --output /absolute/path/fresh-v2-root

python -m polymarket_market_data.smoke \
  --events-report /absolute/path/archived-events/report.json \
  --events-executable /absolute/path/to/catalogue \
  --case archive --expected-asset-kind ctf_token \
  --start 2026-04-14T08:00:00Z --end 2026-04-14T09:00:00Z \
  --output /absolute/path/fresh-archive-root
```

Active cases allow 100 attempts/32 MiB downloaded/256 MiB retained/128 MiB temp;
archive cases allow 100 attempts/4 GiB downloaded/16 GiB retained/2 GiB temp.
The separate eight-second reconnect probe allows five attempts/8 MiB downloaded.
Reports preserve exact receipts, source counts, wall time, OS peak process RSS,
sampled temporary/root storage, capture counts and requests/bytes. RSS is the
largest individual process high-water mark, not a concurrent process-tree sum;
storage samples every 0.5 seconds can miss transient peaks. Failed or uncertain
commands cannot claim complete accounting. Every invocation needs a fresh output
root; its stopped-writer restore is saved beside it with `-restored` appended.

A pass requires nonempty exact price samples and valid authoritative snapshots
for every selected active outcome, including fresh snapshots after forced
reconnect. Archive cases require non-reset updates and reconstructed valid states
for every selected asset in both versions. All cases verify completed resume
without requests, deterministic replay, publication, root integrity, DuckDB
consumer queries and an exact whole-root restore. A missing usable Protocol V2
target is an external acceptance blocker, never a pass.

### Measured acceptance on 10 October 2026

These local reports demonstrate particular selections and windows; provider
retention and current market availability can change. Both active recordings ran
for 60 seconds and required fresh snapshots for every outcome after forced
reconnect. All three cases passed typed DuckDB joins, completed resume with zero
requests, deterministic replay, and stopped-writer whole-root restore. Rebuilding
the restored roots also reproduced their release IDs without network access.

| Case and market IDs | Price samples / valid book states | Captures | Attempts / measured downloaded bytes | Wall seconds | Peak process RSS bytes | Sampled peak temporary bytes |
|---|---:|---:|---:|---:|---:|---:|
| Active CTF: `5234660` | 120 / 32 | 181 | 10 / 763,497 | 122.4 | 117,145,600 | 152,256 |
| Two CTF markets: `5234660`, `5473050` | 236 / 624 | 401 | 11 / 576,589 | 151.0 | 209,403,904 | 152,256 |
| PMXT v1 and v2: `1975743`, 14 April 08:00–09:00 UTC | 0 / 6 | 9 | 4 / 1,269,041,639 | 237.4 | 175,669,248 | 944,913,187 |

Active-case totals include the separate reconnect probe. The two-market case has
incomplete accounting: one uncertain HTTP allowance remains charged. Its main
run measured 9 attempts / 542,185 bytes and charged 10 attempts / 17,319,401 bytes;
measured totals are lower bounds. The other two reports have complete accounting.
The archive case retained both pinned objects, 12 updates, and an authoritative
snapshot and valid states for each outcome in each version. Its peak root storage
was 1,269,158,337 bytes. The v1 object's approximately 16.4 GiB uncompressed size
was processed within the selected 2 GiB temporary profile.

Reports are retained locally under `data/smoke/e2e-ctf-20261010`,
`data/smoke/e2e-multiple-final-20261010`, and
`data/smoke/e2e-archive-accepted-20261010`; each contains `report.json` and has a
verified sibling root ending in `-restored`. Supplemental restored replay evidence
is in `data/acceptance/restored-replays.json`. These ignored artifacts are not
bundled into distributions. Failed exploratory cases remain preserved.

Real Protocol V2 acceptance remains open. A bounded search of 500 active Gamma
markets returned only CTF identities, and no usable V2 target was supplied.
Synthetic position tests establish implementation behavior, not live readiness.
For archived market discovery by condition, Gamma's default open-market filter
can omit closed markets; include `closed=true` when looking up historical targets.
Final-commit CI remains pending separately authorized Git delivery.

## DuckDB consumers

Join on the full typed native key. Price samples have revision grain
`(source, acquisition_id, capture_id, venue, asset_kind, asset_id, source_at)`;
OHLC replaces `source_at` with the observed minute. Select acquisition and capture
explicitly. A capture can contain several minutes.
Book states use `(source, venue, asset_kind, asset_id, stream, sequence, ordinal)`;
select source and stream before consuming its history. Streams do not share state.

```sql
SELECT i.market_id, i.outcome_index, p.source_at, p.price
FROM read_parquet('/absolute/root/published/RELEASE/instruments.parquet') i
JOIN read_parquet('/absolute/root/published/RELEASE/price_samples.parquet') p
  USING (venue, asset_kind, asset_id)
WHERE p.source = 'clob-history'
  AND p.acquisition_id = 'ACQUISITION' AND p.capture_id = 'CAPTURE';

SELECT bucket_start, open, high, low, close, observed_samples
FROM read_parquet('/absolute/root/published/RELEASE/sampled_ohlc.parquet')
WHERE source = 'clob-history' AND acquisition_id = 'ACQUISITION'
  AND capture_id = 'CAPTURE' AND asset_kind = 'ctf_token' AND asset_id = 'ASSET';

SELECT received_at, sequence, ordinal, bids, asks, status
FROM read_parquet('/absolute/root/published/RELEASE/book_states.parquet')
WHERE source = 'pmxt-v2' AND stream = 'STREAM'
  AND asset_kind = 'ctf_token' AND asset_id = 'ASSET'
ORDER BY sequence, ordinal;

SELECT venue, asset_kind, asset_id, start, end, status, observations, gaps
FROM read_parquet('/absolute/root/published/RELEASE/coverage.parquet')
WHERE source = 'pmxt-v2' ORDER BY asset_id, start, end;
```

Coverage rows are non-additive observation receipts: source, typed asset,
half-open requested interval and status. Revisions and acquisitions can have
identical or overlapping receipts. They are not unique intervals, durations or
trade counts; never sum their durations/observations to claim complete coverage.
For acquisition-specific coverage inspect that acquisition's capture manifests,
because the unchanged coverage relation does not carry acquisition/stream IDs.
`capability_unknown`, `missing`, empty success and gaps require separate handling.

The offline installed-wheel CI check runs from a fresh directory with synthetic
producer, HTTP and WebSocket fixtures, all four free feeds and multiple typed
markets. It verifies resume, joins, replay and restore without provider access.
