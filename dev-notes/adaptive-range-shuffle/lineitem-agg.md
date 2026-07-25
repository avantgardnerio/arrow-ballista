# Slice 1 — lineitem partial-agg → FinalPartitioned-agg shuffle

The first subplan we rewrite. Chosen because it has the simplest
topology: one upstream, one downstream, no join coupling on the other
side of anything.

Baseline data for this exchange lives in [`../baselines/q20-sf10.md`],
[`../baselines/q20-sf100.md`], [`../baselines/q20-sf1000.md`] — this
exchange is the only spiller at SF10 and one of three spillers at
SF100+.

## Current plan (subtree we're rewriting)

```
AggregateExec FinalPartitioned gby=(l_partkey, l_suppkey) sum(l_quantity)   [stage N+1]
  ExchangeExec Hash([l_partkey, l_suppkey], 16)                             [stage boundary]
    AggregateExec Partial gby=(l_partkey, l_suppkey) sum(l_quantity)        [stage N]
      FilterExec l_shipdate in [1994-01-01, 1995-01-01)
        DataSourceExec lineitem
```

- **Stage N** (P input partitions, P=16 today): scan → filter → partial
  agg → hash-repartition into K output partitions (K=16 today) → sort-
  shuffle writer holds all input batches until spill.
- **Stage N+1** (K tasks, one per hash bucket): reads its one hash-
  partitioned output stream, runs FinalPartitioned agg.
- **Group key** is `(l_partkey, l_suppkey)`. Currently hash-partitioned
  on both columns.

## Target plan

```
AggregateExec FinalPartitioned gby=(l_partkey, l_suppkey) sum(l_quantity)   [stage N+1]
  FilterExec l_partkey ∈ [cut[k-1], cut[k])          ← scheduler-injected per-task-k
    <read partition_slice = [0..P] of stage-N output via MPT>
      Passthrough writer                                                    [stage boundary]
        BufferExec mode=Dam                                                 ← new placement
          RuntimeStatsExec routing_col=l_partkey (KLL)                      [stage N]
            AggregateExec Partial gby=(l_partkey, l_suppkey) sum(l_quantity)
              FilterExec l_shipdate in [1994-01-01, 1995-01-01)
                DataSourceExec lineitem
```

Ops that need to appear where they aren't today:

| Op | Where | New? |
|---|---|---|
| `RuntimeStatsExec(routing_col=l_partkey, sketch=KLL)` | between Partial Agg and Dam at stage N | wiring only — op merged |
| `BufferExec mode=Dam` | between `RuntimeStatsExec` and Passthrough writer at stage N | wiring only — op merged (`buffer.rs`) |
| Passthrough shuffle writer instead of sort shuffle | stage N boundary | wiring only — writer merged |
| `FilterExec l_partkey ∈ [cut[k-1], cut[k])` | inserted per-task-k in stage N+1's plan tree | wiring — literal `FilterExec` with per-task bounds injected by scheduler at task-emit time |
| Scheduler-side sketch merge + cut computation | between stage N completion and stage N+1 task emission | new — sits alongside the #2175 transport-receive path |

### Why the dam is required

`RuntimeStatsExec` is a streaming tap — it observes rows as they pass
through but doesn't force them to stay in memory. Without a `Dam`
downstream, rows exit stage N (to the Passthrough writer, to disk) as
soon as they're produced. That's fine for the sketch's own accuracy
(the sketch sees every row on its way past), but it means the query
has no elastic point where "enough of the input" has been observed
before commitments are made downstream.

`BufferExec::Dam` gives us that elastic point: it holds rows in a
`MemoryReservation` against the runtime pool, and *breaks* — passing
buffered rows plus the remainder straight through — the moment the
pool refuses a growth (from this operator or anywhere else in the
query). Under normal load the dam holds until end-of-stream; under
memory pressure it degrades to passthrough without OOMing.

Net effect: accurate stats without unbounded materialization. The pair
`RuntimeStatsExec → BufferExec::Dam` is the exact composition
`UnorderedRangeRepartitionExec` already uses for its intra-stage
routing (see `unordered_range_repartition.rs` module doc — same
pattern, different downstream consumer).

## Why routing on `l_partkey` alone is correctness-safe

FinalPartitioned agg only requires *"same group key → same partition."*
Group key is `(l_partkey, l_suppkey)`. Range-partitioning on `l_partkey`
alone puts every row with a given `l_partkey` in the same partition, so
every group key with that `l_partkey` lands together — the group-key
invariant is preserved as a strict superset. Univariate KLL fits
natively, no composite-key gymnastics.

At SF100 there are ~20M distinct `l_partkey` values and ~5 `l_suppkey`
values per `l_partkey` — routing on `l_partkey` alone gives plenty of
distribution headroom for K=16 partitions.

## Half-open interval convention

Mirror `UnorderedRangeRepartitionExec`'s convention: partition k owns
`[cut[k-1], cut[k])` with virtual `-∞` at position 0 and `+∞` at
position K, so K-1 physical cuts define K partitions. Predicate for
task k:

- k=0:      `l_partkey <  cut[0]`
- 0<k<K-1:  `cut[k-1] <= l_partkey AND l_partkey < cut[k]`
- k=K-1:    `l_partkey >= cut[K-2]`

## What "green" looks like

A single-iteration run of Q20 SF100 with slice-1 applied should show:

- One Hash exchange remains (`Hash([ps_partkey], 16)` — the partsupp
  side of the ps⋈part join, untouched by this slice)
- **Spill events on the `l_partkey` stage → 0**
- Row count still 17971
- Wall clock unchanged or better (this slice trades sort-shuffle spill
  for K×P file opens on the read side; net direction is a hypothesis
  to verify, not asserted)

The other two Hash exchanges (`ps_partkey` and `p_partkey`) stay as
they are — subsequent slices will attack them.
