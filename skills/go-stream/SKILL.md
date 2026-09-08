---
name: "go-stream"
description: "Use the go-stream library (github.com/JayceChant/go-stream) correctly — a Go 1.27 generic-methods implementation of the Java Stream API. Invoke when user code imports this module or asks for Stream-style pipelines in Go."
---

# go-stream — coding-agent usage guide

`github.com/JayceChant/go-stream` brings Java-Stream-style lazy, fluent pipelines to Go via Go 1.27 **generic methods**. Requires `go 1.27` or later in go.mod. Zero third-party dependencies. The project is in v0.x: minor releases may contain breaking changes.

## Imports

```go
import (
    stream "github.com/JayceChant/go-stream"      // Stream type, sources, operators
    "github.com/JayceChant/go-stream/collector"   // collector family (as needed)
    "github.com/JayceChant/go-stream/constraints" // Integer / Float / Number
)
```

## Core model

- Sources are lazy; intermediates only declare the pipeline; the terminal triggers ONE fused evaluation pass.
- **Streams are one-shot.** A terminal operation (or a bridge) consumes the stream; consuming it again panics. For replay, `stream.Cache(s)` returns a factory that produces fresh streams from one materialization.
- Infinite sources (`Generate`, `Iterate`) are safe only behind short-circuiting terminals: `Limit`, `First`, `TakeWhile`, `AnyMatch`, `ForEachUntil`.

## Non-obvious idioms

- **Comparators are `func(a, b T) int`** (standard-library `slices.SortFunc` / `cmp.Compare` style). Never write Java `Comparator` or `less(a, b) bool` callbacks.
- **Errors as values, not exceptions**: fallible callbacks use the `Err` variants (`MapErr`, `FilterErr`, `FlatMapErr`, `PeekErr`); pull sources use `FromFunc(next func() (T, bool, error))`. After any terminal, check `s.Err()`; on error, the terminal returns the partial result. Plain callbacks (`Map`, `Filter`, ...) never return errors — do not add error returns to them. Programming bugs (nil callback, double consumption) panic by design.
- **`Sorted` is unstable** (pdqsort, aligned with `slices.SortFunc`). Use `StableSorted` when equal keys must keep encounter order.
- **`collector.ToMap` key conflicts are last-wins** (Go map semantics). Use `ToMapMerge` for a custom merge function.
- **Numeric narrowing**: `Range` (plus `OfNumber`, `FromNumberSlice`, `MapToNumber`, `AsNumber`) produce `*NumberStream[N]`, which adds chainable `Sum()` / `Avg()` / `Min()` / `Max()` / `Contains()` / natural-order `Sorted()` / `Distinct()`. Escape back to `*Stream[T]` with `.AsStream()`. Bridges consume the source (one-shot).

## Method vs package-level — do not "fix"

Go 1.27 methods cannot constrain the receiver's existing type parameter, nor return a derived type of it. Several APIs are therefore intentionally package-level. Never refactor them into methods:

- Package-level only: `Distinct[T comparable]`, `Contains[T comparable]`, natural-order `Sorted` / `Min` / `Max[T cmp.Ordered]`, `Sum` / `Avg[T Number]`, `Chunk(s, n)`, `Enumerate(s)`, `WindowSliding(s, n)`, `Concat`, `Cache`.
- Method forms that DO exist: `DistinctBy[K comparable](key)` on the stream, comparator-based `Sorted(cmp)`, `s1.Zip(s2, f)`; and on `NumberStream`: `Sum()` / `Avg()` / `Min()` / `Max()` / `Contains()` / `Sorted()` / `Distinct()`.

## Parallelism & lifecycle

- `Parallel(n)` / `Sequential()` switch modes before the terminal; `Unordered()` allows a streaming merge. Evaluation automatically falls back to sequential after short-circuit terminals or materializing operators — this is expected behavior, do not work around it.
- `OnClose(f)` / `Close()`: callbacks fire at the end of evaluation (including short-circuit/error/panic paths); explicit close is idempotent; callback errors surface via `Err()`.

## Collectors

`s.Collect(c)` takes collectors from the `collector` package. Built-ins: `ToSlice` `ToSet` `ToMap` `ToMapMerge` `GroupingBy` `GroupingByDownstream` `PartitioningBy` `PartitioningBySlice` `Teeing` `Filtering` `FlatMapping` `CollectingAndThen` `MinBy` `MaxBy` `Joining` `Counting` `Reducing` `Mapping` `Summing` `Averaging` `Summarizing` (`SummaryStats`).

Custom collectors implement the `Collector[T, A, R]` interface (Supplier / Accumulator / Combiner / Finisher). Return a `nil` Combiner to opt out of parallel merging.

## Interop

`s.ToSeq()` returns `iter.Seq[T]` for range-over-func; a consumer `break` short-circuits the underlying source.

## References

- API reference: <https://github.com/JayceChant/go-stream/blob/master/docs/api.md>
- Design doc: <https://github.com/JayceChant/go-stream/blob/master/docs/design.md>
- Runnable examples: <https://github.com/JayceChant/go-stream/tree/master/example> (basics, collectors, numeric, errors, parallel, lifecycle, extensions) and `example_test.go` in the repo root
- Java-to-go-stream mapping table: "Comparison with Java Stream" section of the README
