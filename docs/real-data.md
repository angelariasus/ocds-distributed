# Real data: two years of the portal end to end

Every other document measures one mechanism, often against the simulated portal. This one runs the
whole pipeline on real data and asks the question the project exists for: how efficient are
EsSalud's medicine purchases? Run on 2026-10-19.

## Scope

| | |
|---|---|
| Source | `seace_v3` monthly bulk files from the real portal |
| Window | October 2024 to September 2026 (24 files) |
| Downloaded | 280.9 MB compressed, every file on the first attempt, every SHA-256 verified |
| Records | 160,063 compiled OCDS records |
| Silver | 183,492 tender items (2024: 27,626; 2025: 89,767; 2026: 66,099) in 78 s on two workers |
| Gold | 183,492 facts, 2,940 buyers, 62,076 suppliers, 130,553 distinct items |

This is about 6% of the 2.76 million records the portal publishes. The older `seace_v2` files and
the cursor API were not extracted from the real portal: the crawl and every load or chaos
experiment ran against `ocds-mock`, and Silver's scaling table replicates real states
(`AMPLIFY`) instead of reading more months. The platform is built for the full corpus; it has been
validated end to end on these two years. The download was a single planning round, well below the
portal's measured limits, and was not repeated.

## What the data showed

- **No OCID appears in two monthly files.** The 24 files hold 160,063 records and the compiler
  ended with exactly 160,063 states. An update to a process arrives by the portal regenerating
  that process's month, not as a new release in a later file, so cross-file merging is only
  exercised by the API crawl and redeliveries.
- **January is nearly empty**: 1,313 records in January 2025 and 1,012 in January 2026 against
  5,000 to 11,000 in other months, consistent with the start of the fiscal year.
- **Every item has its own value.** OCDS gives each tender item, award item and contract item a
  `totalValue`. The original project ignored it and copied the process value onto every item,
  which multiplies amounts by the number of items when they are summed. Silver now takes the item's
  own line, falls back to the process value only when the process has a single item, and keeps the
  process value apart as `monto_referencial_proceso`. EsSalud's medicine referential total went
  from 12,875.5 to 1,739.2 million soles.
- **Amendments are not published** in this window: zero items carry an `adenda`, so the
  original project's overrun ratio cannot be measured on it.

## Medicines and the Petitorio

An item is a medicine when its CUBSO classification is in segment 51. Its description is matched
against EsSalud's Petitorio Farmacológico 2026 (Resolución N° 063-2026) by `gold/pharma.py`, which
records how each match was made:

| Method | Meaning | All buyers | EsSalud |
|---|---|---|---|
| `dci+concentracion` | active ingredient and strength match | 50.4% | 76.2% |
| `dci` | active ingredient, strength not confirmed | 15.5% | 5.4% |
| `fuzzy` | close spelling | 2.8% | 1.4% |
| `dci-parcial` | a combination matched only one of its ingredients | 3.0% | 0.6% |
| `none` | not in the Petitorio | 28.4% | 16.4% |

Percentages are over distinct medicine items for all buyers and over EsSalud's 701 purchased
medicine items.

Validation:

- 26 EsSalud items say in their own description that the drug is outside the Petitorio. The
  matcher marks 21 of them `none`. The other 5 (trastuzumab, dapagliflozina) were bought in
  2024–2025 and added to the formulary in 2026: the reference is a single edition, so purchases
  are judged against the Petitorio in force today, not the one in force when they were made.
- A manual review of 15 `none` items found about 8 genuine out-of-formulary drugs (tenofovir
  alone, darolutamida, ibrutinib, olaparib, turoctocog, eltrombopag, pembrolizumab), 4 generic or
  non-drug lines, and 2 matcher misses (sulfadiazina de plata, sulfato ferroso).
- Some misses are naming, not matching: the Petitorio lists "agua destilada" where purchases say
  "agua para inyección", and plasma-derived factor VIII where EsSalud buys a recombinant one
  (simoctocog alfa).

## EsSalud medicine purchases

From `analysis/essalud_medicines.sql`, run against the Gold replicas. Amounts in millions of soles.

| | Items | Processes | Referential | Awarded |
|---|---|---|---|---|
| EsSalud medicines | 701 | 259 | 1,739.2 | 1,047.9 (474 items) |
| Medicines, rest of the State | 3,893 | | 2,704.8 | |

| Petitorio status | Items | Referential | Share | Direct contracting |
|---|---|---|---|---|
| In the Petitorio | 582 | 1,578.9 | 90.8% | 4.3% |
| Outside | 115 | 154.8 | 8.9% | 53.0% |
| Partial combination | 4 | 5.6 | 0.3% | 0% |

- **Out-of-formulary drugs are bought differently.** More than half go through direct
  contracting, against 4% of formulary drugs; reverse auctions (`subasta inversa`) carry 1,081
  of the 1,739 million and only 12 out-of-formulary items. The largest out-of-formulary lines are
  high-cost specialty drugs: imiglucerasa, elexacaftor/tezacaftor/ivacaftor, simoctocog alfa,
  ribociclib, eltrombopag.
- **Awards come in well under the reference.** For items with both amounts, the median awarded
  value is 58% of the referential one for formulary drugs and 100% for out-of-formulary drugs:
  competition in reverse auctions lowers prices, a single-supplier direct contract does not.
  The referential value is an estimated ceiling, and an award may cover a different quantity, so
  this is a price-pressure indicator, not a measured saving.
- **Supplier concentration is low**: 103 suppliers, Herfindahl-Hirschman index 478, the largest
  supplier (Laboratorios AC Farma) holds 11.5% of the awarded amount.
- **Lead times** (call to signed contract, only signed items): median 112 days for formulary drugs,
  39 for out-of-formulary ones. Direct contracting is faster by design; recent calls that have not
  signed yet are missing, so these medians lean low.

## Lead-time model

The first model was trained on every item with a lead time. Profiling by call month showed the
bias: the monthly median stays between 32 and 56 days for most of the window and falls to 28 and
22 days in the last two months, because only the fastest recent processes have signed. The training job now keeps
only calls at least 180 days older than the newest call in Gold, predicts lead times up to 180
days, and evaluates on the newest 20% of that cohort instead of a random split.

| | |
|---|---|
| Cohort | calls up to 2026-04-19; 64.8% of its items signed within 180 days |
| Training rows | 89,908 |
| Test | 17,982 items called from 2025-12-04 on |
| MAE | 17.9 days |
| Baseline (always the median) | 23.2 days |

The model beats the trivial baseline by about a quarter, a modest gain from five coarse features.
It predicts the lead time of a process *that signs within six months*; it says nothing about
whether a process will sign. Served through the API:

```
direct, 250,000 soles, EsSalud, Lima -> 25.0 days   model 20261019T075738Z  served_by ec66b41539f2
open,   250,000 soles, EsSalud, Lima -> 63.7 days   model 20261019T075738Z  served_by 168ee6467e5b
```

## Re-emission of stored files

The first backfill attempt stored the files but emitted no records: Bronze objects from earlier
runs carried the current checksum, so the idempotent skip path never parsed them again. The
extractor now records in the catalog which object and checksum it has emitted to
`bronze.records`, and re-emits a stored file whose current checksum was never emitted
(`status=reemitted`, attested by the ledger like a fresh download).
