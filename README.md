# Polymarket market data

Finite local collection of Polymarket historical price samples, public displayed
books, free PMXT archives and optional paid historical books/trades. MIT software;
no trading interface, scheduler, hosted service or bundled production data.

`polymarket-events` owns Gamma metadata. This package reads its immutable
`oddsfox.polymarket.metadata.v1` bundles. It runs without the old
`oddsfox-pipeline` checkout and does not read or modify its state.

## Install and plan

Use Python 3.12 and `uv sync --locked`. `uv run polymarket-market-data --help`
shows the CLI. Installed commands work outside this checkout; `--root` defaults
to `./data` relative to the operator's current directory. Roots inside a Git
checkout must be ignored. Credentials are supplied only through the environment.

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

Those example limits are also the finite defaults. A plan pins the metadata,
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
of no trading. Free archive objects are hourly, read in their original row order
and retained with their hash. A missing hour is a gap. Differing archive updates
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
  footer uncompressed sizes and row counts are checked before deserialization;
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

Production readiness requires operator-run, bounded live source checks: current
history envelopes, a WS reconnect, one archive replay, and explicitly allocated
paid checks if paid sources are used. Automated tests are offline; they do not
establish current provider retention or account pricing.

## Development

`scripts/verify-fast` runs lint, formatting checks and tests. `scripts/verify`
also verifies the lock, installs locked dependencies and builds distributions.
Use the repository's Universal Pad engineering workspace for material changes.
See `THIRD_PARTY_NOTICES.md` for reused MIT code and PMXT data attribution.
