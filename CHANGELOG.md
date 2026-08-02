# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-08-02

Initial release.

### Added

- `measure!`, `count!` and `timer!` macros. Every measurement is named by the
  macro's first argument, a string literal or `const` stored as a
  `&'static str`. After it, tags are bare and fields carry a `?` prefix; values
  are arbitrary expressions, and a bare name is shorthand for `name = name`. A
  doc comment inside the invocation is attached to the point.
- A bounded global queue with non-blocking sends: a full queue drops points and
  counts them rather than stalling the code being measured. A background worker
  batches points by size or interval and hands each batch to the backend.
- Backends: `InfluxDb` (v2 write API, token auth, retries on 5xx and transport
  errors), `Stdout` (human-readable or line protocol) and `Composite` (fan-out
  that reports every failure).
- A pool of `Config::writers` tasks publishing batches concurrently, fed by a
  single dispatcher so that batching takes priority over spreading work.
- An async lifecycle throughout: `flush_async` and
  `deregister_global_backend_async` keep runtime threads free, and no backend
  blocks one. The blocking forms remain for synchronous programs.
- The InfluxDB backend reports on itself: `mensura.influxdb.v2.write` records
  the point count, latency, retry count and outcome of each request, without
  reporting on its own reports.
- Warnings on stderr when measurements are lost, naming the cause chain from
  the HTTP client and how many points went with it.
- Global tags and fields on the InfluxDB backend, merged in at serialization
  time; a point that carries the key keeps its own value.
- `mensura::test_backend()`, capturing measurements in-process behind a
  filter/aggregation query API and deregistering on drop.
- Compile-time diagnostics for a missing or non-constant measurement name, a
  `measure!` without a field, duplicate keys within one invocation, and more
  than 16 tags and fields on one point.

[Unreleased]: https://github.com/bldr-project/mensura/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/bldr-project/mensura/releases/tag/v0.1.0
