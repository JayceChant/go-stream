# go-stream

[![CI](https://github.com/JayceChant/go-stream/actions/workflows/ci.yml/badge.svg)](https://github.com/JayceChant/go-stream/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/JayceChant/go-stream/branch/master/graph/badge.svg)](https://codecov.io/gh/JayceChant/go-stream)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=JayceChant_go-stream&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=JayceChant_go-stream)
[![govulncheck](https://github.com/JayceChant/go-stream/actions/workflows/govulncheck.yml/badge.svg)](https://github.com/JayceChant/go-stream/actions/workflows/govulncheck.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/JayceChant/go-stream/badge)](https://api.scorecard.dev/projects/github.com/JayceChant/go-stream)
[![CodeQL](https://github.com/JayceChant/go-stream/actions/workflows/codeql.yml/badge.svg)](https://github.com/JayceChant/go-stream/actions/workflows/codeql.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/JayceChant/go-stream.svg)](https://pkg.go.dev/github.com/JayceChant/go-stream)

English | [简体中文](./README_CN.md)

A Go 1.27 generics implementation of the Java Stream API — built on the newly introduced **generic methods** feature, enabling natural, fluent stream processing in native Go for the first time:

```go
stream.Of(1, 2, 3, 4, 5).
    Filter(func(n int) bool { return n%2 == 1 }).
    Map(func(n int) int { return n * n }).
    ToSlice() // [1 9 25]
```

Before Go 1.27, methods could not declare their own type parameters, so chained APIs like `Map[U]` could only be written as package-level functions (`stream.Map(s, f)`) with no fluent chaining. With generic methods, methods on `Stream[T]` can carry their own type parameters, enabling fully chained declarations with static type migration along the pipeline.

## Features

- **Lazy pipelines**: intermediate operations only declare the pipeline without triggering traversal; a terminal operation triggers a single fused evaluation pass
- **Generic methods**: method-level type parameters such as `Map[U]`/`Zip[U, R]`/`Collect[A, R]` let element types migrate statically along the pipeline
- **Single-pass fusion**: stateless operators fuse into a single pass (Sink chain) at evaluation time; stateful operators materialize in segments
- **Short-circuit evaluation**: `Limit`/`First`/`AnyMatch`/`TakeWhile` and friends stop source traversal as soon as the condition is met (safe for infinite streams)
- **Errors as values**: expected errors (IO source failures, `MapErr` family callback errors) propagate as `error` values — first error short-circuits, partial results are preserved, query via `Err()`; unrecoverable misuses (double consumption, nil callbacks) panic
- **Composition over inheritance**: Java's abstract class hierarchy (AbstractPipeline/StatelessOp/StatefulOp) is translated into "struct embedding + constructors + injected function values" with no simulated inheritance
- **Zero third-party dependencies**: no third-party runtime dependencies in v1

## Installation

```bash
go get github.com/JayceChant/go-stream
```

Requires go 1.27+ (relies on the generic methods feature).

## Quick Start

```go
import (
    "github.com/JayceChant/go-stream"
    "github.com/JayceChant/go-stream/collector" // collector subpackage (as needed)
)

// 1. Build from containers (lazy, no traversal yet)
s := stream.FromSlice(data)          // zero-copy reference
r := stream.Range(0, 100)            // integer range [0, 100)
g := stream.Generate(func() int { return 42 }) // infinite generator

// 2. Intermediate operations (return a new Stream, chainable)
s.Filter(p).Map(f).Sorted(cmp).Limit(10)

// 3. Terminal operations (trigger a single evaluation, consume the stream)
s.ToSlice()
s.Count()
s.AnyMatch(p)
s.Collect(collector.GroupingBy(keyOf, valOf))
```

More runnable examples: [example_test.go](./example_test.go) (verified by `go test`) and the [example/](./example) directory — six standalone, copy-paste-ready programs covering the full API surface:

```bash
go -C example run ./basics      # sources → intermediate → terminal operations
go -C example run ./collectors  # collector family + custom Collector (TopN)
go -C example run ./numeric     # numeric aggregation, Scan, infinite sources, Zip/Chunk/Enumerate
go -C example run ./errors      # errors-as-value model (FromFunc/MapErr family/Err())
go -C example run ./parallel    # Parallel(n)/Unordered, order-preserving merge, auto fallback
go -C example run ./lifecycle   # OnClose/Close resource management, Cache replayable factory
```

`example/` is a separate Go module (not part of the library's tests or coverage) so each file can be copied into your project as-is.

## API Overview

| Category | APIs |
|---|---|
| Construction | `Of` `FromSlice` `FromSeq` `FromChannel` `FromMap` `FromFunc` `Generate` `Iterate` `Range` `Concat` `Empty` |
| Stateless intermediate | `Filter` `Map` `FlatMap` `FlatMapSeq` `Peek` `TakeWhile` `DropWhile` |
| Err variants | `MapErr` `FilterErr` `FlatMapErr` `PeekErr` |
| Stateful intermediate | `Limit` `Skip` `Sorted` `DistinctBy` `Reverse` `Scan` |
| Parallelism control | `Parallel(n)` `Sequential()` `Unordered()` |
| Package-level intermediate | `Distinct` `Sorted` (natural order) `Chunk` `Enumerate` |
| Two-stream | `Zip` |
| Lifecycle | `OnClose(f)` `Close()` `Cache(s)` (replayable factory) |
| Terminal | `ForEach` `ForEachUntil` `ToSlice` `Count` `Reduce` `ReduceOpt` `Collect` `First` `FindAny` `AnyMatch` `AllMatch` `NoneMatch` `Min` `Max` `Err` |
| Collectors (subpackage `collector`) | `ToSlice` `ToSet` `ToMap` `ToMapMerge` `GroupingBy` `Joining` `Counting` `Reducing` `Mapping` |
| Collectors (root package) | `Summing` (relies on the `Number` constraint) |
| Package-level aggregation | `Sum` `Avg` `Contains` `Min` `Max` |

For the full reference and examples, see [docs/api.md](./docs/api.md).

## Comparison with Java Stream

| Java | go-stream | Notes |
|---|---|---|
| `Stream<T>` (interface) | `*Stream[T]` (concrete struct) | In Go 1.27 interface methods cannot declare type parameters; generic methods must live on concrete types |
| `stream.of(...)` / `Arrays.stream` | `stream.Of(...)` / `stream.FromSlice` | |
| `Collectors.toList()` | `collector.ToSlice[T]()` | |
| `Collectors.toMap` | `collector.ToMap` / `ToMapMerge` | Key conflicts: last-wins (aligned with Go map conventions); use ToMapMerge for custom merging |
| `Collectors.groupingBy` | `collector.GroupingBy` | Preserves encounter order within groups |
| `Comparator` | `func(a, b T) int` | Aligned with the standard library's `slices.SortFunc`/`cmp.Compare` conventions |
| `IntStream` specializations | Generics + `Number`/`cmp.Ordered` constraints | Go generics have zero boxing; no specialization needed |
| `stream.parallel()` | `Parallel(n)` / `Sequential()` | TrySplit splitting + goroutines; automatically falls back to sequential after short-circuit terminals or materializing operators |
| `stream.unordered()` | `Unordered()` | Clears the SpOrdered flag; under parallelism, shard results are pushed as they complete (streaming merge) |
| `stream.onClose(f)` / `close()` | `OnClose(f)` / `Close()` | Triggered automatically at the end of evaluation (including short-circuit/error/panic paths); explicit close is idempotent; callback errors are queryable via `Err()` |
| Exception propagation | Errors as values (`Err()`/`MapErr` family) | Aligned with Go's official error style |
| `stream.distinct()` | `DistinctBy[K comparable](key)` method / `Distinct` package-level | A method's own type parameters may carry the `comparable` constraint (keys are compile-time comparable, zero boxing); `Distinct` constrains the element `T` itself, and methods cannot constrain the receiver's `T`, so it stays package-level |

## Design Highlights

- **Sink push chain**: at evaluation time, sinks are wrapped in reverse starting from the terminal operation (`Accept(t) bool` merges Java's `cancellationRequested`); the data source pushes elements through the entire chain in a single pass
- **Segmented evaluation**: stateful operators such as `Sorted`/`Skip` first drive upstream to materialize `[]T`, then transform and replay; `Limit` supports short-circuit collection from infinite sources; `Skip(0)` returns the original stream as a true no-op (no materialization, flags passthrough)
- **Flag propagation**: `SpSized`/`SpOrdered`/`SpSorted`/`SpDistinct` propagate along the pipeline (e.g. Map preserves Sized 1:1 so downstream can preallocate), informing parallel splitting decisions
- **Error model**: modeled after the `bufio.Scanner.Err()` convention — on error, terminal operations return the accumulated partial results, and `Err()` returns the first error

See [docs/design.md](./docs/design.md) for architecture details.

## Performance

`Filter+Map+ToSlice` vs. a hand-written for loop (a realistic scenario including `strconv.Itoa`):

| Scale | Pipeline | Hand-written for | Overhead |
|---|---|---|---|
| 1e2 | ~2.8 μs | ~1.0 μs | 2.8x |
| 1e4 | ~0.48 ms | ~0.18 ms | 2.6x |
| 1e6 | ~35 ms | ~22 ms | 1.6x |

Target of <3x met (AMD Ryzen 5 7535U, benchtime 300ms; reproduce with `go test -bench . -run '^$'`).

## Roadmap

- [x] v1 sequential evaluation engine, full operator set, Collector system, errors-as-values model
- [x] **Parallel evaluation `Parallel(n)` / `Sequential()`**: recursive TrySplit splitting + goroutine-parallel execution + `Collector.Combiner` merging; order-preserving merge by shard order (Ordered); automatic fallback to sequential after short-circuit terminals or materializing operators (correctness first); measured speedup of ~3.3x (4 shards) on CPU-bound workloads
- [x] v1.x: **`onClose`/resource management** (`OnClose(f)` triggered automatically at end of evaluation + idempotent explicit `Close()`), **replayable streams** (`Cache(s)` factory: materialize once, produce a brand-new one-shot stream each time without breaking the one-shot model), **Unordered streaming merge** (`Unordered()` clears the order flag; under parallelism shards push results as they complete, reducing end-to-end latency)

## License

MIT
