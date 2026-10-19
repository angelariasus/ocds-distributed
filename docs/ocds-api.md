# OCDS portal: measured behaviour

Base URL: `https://contratacionesabiertas.oece.gob.pe/api/v1`. Measured on 2026-10-18.

## Volume

| Metric | Value |
|---|---|
| Records (`ocid`) | 2,763,899 |
| By source | `seace_v2`: 1,821,842 · `seace_v3`: 942,057 |
| Years | 2004–2026 (peak 2008: 313,570; 2016+: 60–80k per year) |
| Buyers / suppliers | 3,317 / 503,415 |
| Awards / contracts | 2,013,950 / 1,327,885 |
| Estimated raw JSON | 30–50 GB (one month ≈ 6,625 records ≈ 71 MB) |

Sources: `recordsYearFilter`, `recordsSourceFilter`, `indexCountData`.

## Bulk files (`/files`, `/file/{source}/{type}/{year}/{month}/`)

- 325 monthly files (165 `seace_v3`, 160 `seace_v2`); `paginateBy` up to at least 1000.
- `json` type returns a zip (~7 MB) with a single JSON member (~71 MB) holding `records`.
- `sha` type returns the SHA-256 of the **inner JSON**, not of the zip.
- Supports `Range` requests (`206 Partial Content`), so large files can be fetched in parallel chunks.
- The current month is regenerated daily (`Last-Modified` moves; `ETag` and SHA change).

## Cursor API (`/recordsAfter`)

- `size` max 100; larger values return 404.
- `searchAfter` is an Elasticsearch sort key: `source,tenderId,dateMs,dateMs,ocidWithDate`.
  Any point in the keyspace can be addressed by constructing the key manually.
- `tenderId` sorts lexicographically (`"50000" > "5000" > "499999"`), so naive range splits are skewed.
- `year` and `source` query parameters are ignored.
- ~1 s per page alone; with 20 concurrent requests every request takes ~9 s and no 429 is
  returned. Throttling is implicit, so client-side rate limiting must be adaptive.

## Access

- Generic user agents (for example curl's default) receive `403 Forbidden` from the WAF.
  Any descriptive user agent works.
- Observed from Peru; other regions were not tested.
