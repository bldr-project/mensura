# mensura

Metrics instrumentation for Rust with InfluxDB semantics. A measurement has a
**name**, a set of **tags** (indexed dimensions) and a set of **fields**
(values), recorded through macros that never block the code being measured.

```rust
use mensura::{count, measure, timer};

// The name comes first. Tags are bare after it; fields are marked with `?`.
measure!("call_latency", host = "web-1", region = "eu", ?ms = 12.5, ?ok = true);

// Tag and field values are arbitrary expressions…
measure!("http_request", endpoint = format!("GET {route}"), ?bytes = body.len());

// …and a bare name records the variable of that name, like struct shorthand.
measure!("call_latency", host, region, ?ms);

// `count!` implies `count = 1`.
count!("http_request", route = "/search", status = 200);

// `timer!` records `time = <elapsed>` when the binding is dropped.
let _timer = timer!("query_duration", operation = "select");
```

The name is the InfluxDB measurement the point lands in. It is stored as a
`&'static str`, so it must be a string literal or a `const` — never computed at
runtime.

## Layout

| Crate            | What it is                                        |
| ---------------- | ------------------------------------------------- |
| `mensura`        | The library: queue, backends, test helpers        |
| `mensura-macros` | The `measure!` / `count!` / `timer!` proc macros  |

## How it works

Each macro invocation builds a `DataPoint` — a name, a timestamp, an optional
docstring and an inline `ArrayVec` of up to 16 `(kind, key, value)` entries, so
recording allocates nothing beyond what the values themselves need. The point is
`try_send`ed to a bounded global queue. One **dispatcher** task reads that queue
and groups points into batches; a pool of **writer** tasks publishes them,
receiving each batch as a `&Vec<DataPoint>`.

Things worth knowing:

* **Recording never blocks.** If the queue is full the point is dropped, counted
  in `Stats::dropped` and reported on stderr. Instrumentation cannot stall the
  program it measures.
* **Nothing blocks a runtime thread.** The dispatcher, the writers and the
  backends are all async; `flush_async()` and `deregister_global_backend_async()`
  cover the lifecycle. The blocking forms exist for synchronous programs and say
  so in their documentation.
* **Batching comes before parallelism.** Exactly one task reads the queue,
  whatever `Config::writers` says, so ten points arriving together become one
  request rather than being raced into ten single-point requests. Writers only
  publish what the dispatcher has already grouped.
* **With no backend registered, the macros do nothing** — they do not even
  evaluate their arguments, so `measure!("m", ?rows = expensive())` is free until
  you turn metrics on.

## Registering a backend

```rust
use mensura::backends::{Composite, InfluxDb, Stdout};

let backend = Composite::new()
    .with(Stdout::new())
    .with(
        InfluxDb::v2("https://influxdb.example.com")
            .with_auth_token(std::env::var("INFLUXDB_TOKEN")?)
            .with_org("myorganization")
            .with_bucket("metrics"),
    );

mensura::register_global_backend(backend).expect("backend is already registered");
```

Registering from inside a Tokio runtime spawns the worker onto it. Otherwise
mensura starts a dedicated thread with a small current-thread runtime, so
synchronous programs work unchanged.

Before exiting, drain what is still queued:

```rust
mensura::deregister_global_backend_async().await;   // from async code
mensura::deregister_global_backend();               // from a plain `main`
```

Both wait for the final write and shut the tasks down; the async form keeps the
runtime thread free while it happens. `flush_async()` / `flush()` do the same
waiting without deregistering.

`Config` tunes the queue via `register_global_backend_with_config`:
`queue_capacity`, `batch_size`, `flush_interval`, `shutdown_timeout`,
`log_errors`, `dedicated_thread`, and `writers` — how many batches may be
published concurrently. One writer means each batch waits for the previous round
trip; raise it when the backend, not the queue, is the bottleneck.

### Backends

* `InfluxDb` — POSTs line protocol to an InfluxDB write endpoint. The
  constructor names the API version, so a call site says which protocol it
  speaks: `InfluxDb::v2(url)` is `/api/v2/write`, addressed by org and bucket
  and authenticated with `Authorization: Token …`, which InfluxDB 3 servers also
  accept as their v2 compatibility endpoint. Retries transport errors and 5xx
  responses; never retries a 4xx, which cannot succeed on a second try.
* `Stdout` — prints each point, either human-readably or as the exact line
  protocol the InfluxDB backend would send.
* `Composite` — fans one batch out to several backends. A failure in one does not
  stop the others; failures are reported together.

Anything implementing the `Backend` trait can be registered:

```rust
#[async_trait::async_trait]
impl mensura::Backend for MyBackend {
    async fn write(&self, points: &Vec<mensura::DataPoint>) -> Result<(), mensura::BackendError> {
        // …
        Ok(())
    }
}
```

### Reporting on itself

The InfluxDB backend records a measurement for each request it makes:
`mensura.influxdb.v2.write`, tagged `outcome` (`success`/`failure`) with fields
`points`, `time` and `retries`. It is emitted only for batches that carried
application data — a batch of nothing but mensura's own reports produces none,
which is what stops the reporting from feeding itself. Turn it off with
`with_self_metrics(false)`.

Failures are reported on stderr as well, with the whole cause chain from the
HTTP client and the number of measurements lost:

```text
mensura: warning: influxdb backend failed, 512 measurement(s) lost: error sending
request for url (https://influxdb.example.com/api/v2/write?…): client error: connection refused
mensura: warning: queue full, 37 measurement(s) dropped (37 in total); raise
`Config::queue_capacity`, `Config::writers`, or lower `Config::flush_interval`
```

### Global tags and fields

Values that describe the process rather than any one event — host, service,
version, environment — are set once on the backend instead of at every call
site:

```rust
let backend = InfluxDb::v2("https://influxdb.example.com")
    .with_global_tag("host", hostname())
    .with_global_tags([("service", "api"), ("env", "production")])
    .with_global_field("version", env!("CARGO_PKG_VERSION"));
```

They are merged in as each batch is serialized, so they cost nothing per
measurement. A point that already carries the key keeps its own value — the
specific measurement knows better than the process-wide default — and a global
value never appears twice on a line.

Globals live in `line_protocol::Options`, which the `Stdout` backend accepts
too, so a `Composite` can echo exactly what is being shipped:

```rust
let echo = Stdout::line_protocol().with_options(influx.options().clone());
let backend = Composite::new().with(influx).with(echo);
```

## The macros

```rust
measure!("temperature", room = "kitchen", ?celsius = 21.5);
count!("http_request", route = "/search");     // adds `count = 1`
let t = timer!("query_duration", op = "select");  // adds `time = <elapsed>` on drop
```

* **The measurement name comes first**, in every one of the three macros. It is
  the series the point is stored under, so it must be a string literal or a
  `const` — it ends up in the `DataPoint` as a `&'static str`.
* **Values are any expression** — `?rows = rows.len()`,
  `tag = format!("foobar-{baz}")`, `?ok = result.is_ok()`. They are evaluated at
  the call site, in order, and only when a backend is registered.
* **A bare name** is shorthand for `name = name`:
  `measure!("m", host, ?latency)` records the variables `host` and `latency`
  under those keys. It mixes freely with explicit values and works in all three
  macros.
* **Tags** accept strings, `bool`, `char` and integers — deliberately not floats,
  whose cardinality would blow up the index. They are always stored as text.
* **Fields** accept every primitive number, strings, `bool` (as `0`/`1`) and
  `Duration` (as nanoseconds).
* String literals are stored as `&'static str`, so the common case costs no
  allocation. A shorthand or any other expression takes the owned path.
* Keys may be identifiers (including keywords: `type = "x"`) or string literals
  for anything exotic: `"http.status" = 200`. A string-literal key has no
  variable to borrow its name from, so it always needs an explicit value.

Compile errors, rather than surprises at runtime:

* A missing measurement name, or one that is not a constant.
* `measure!` with no field. A point of tags alone records nothing and InfluxDB
  rejects it — use `count!` if you mean "this happened". `count!` and `timer!`
  add a field of their own, so tags alone are fine there.
* A duplicate tag or field within one invocation, which would silently overwrite.
* More than 16 entries, the inline capacity of a `DataPoint`.

A doc comment written **inside** the invocation is attached to the point:

```rust
measure!(
    /// Time the upstream took to answer.
    "call_latency",
    route = "/search",
    ?elapsed = start.elapsed(),
);
```

`timer!` returns an RAII `Timer`. It can be annotated before it reports, stopped
early, or thrown away:

```rust
let mut timer = timer!("load_duration", operation = "load");
timer.field("rows", rows.len());   // recorded alongside `time`
timer.tag("outcome", "ok");
// timer.stop();     -> reports now, returns the elapsed Duration
// timer.discard();  -> records nothing
```

## Testing metrics

`mensura::test_backend()` captures measurements in-process and returns a
queryable store. It deregisters on drop, and holds a process-wide lock for its
lifetime so tests capturing metrics take turns instead of seeing each other's
points.

```rust
#[test]
fn my_test() {
    let metrics = mensura::test_backend();

    measure!("m", tag1 = "value", tag2 = "x", ?field1 = "value", ?field2 = 1u8, ?field3 = 4.0);

    assert_eq!(
        metrics.query()
            .measurement("m")         // recorded under this name
            .tag("tag1", "value")     // tag equals
            .field("field1", "value") // field equals
            .with_tag("tag2")         // tag present
            .with_field("field2")     // field present
            .sum("field3"),
        4.0
    );
}
```

Filters: `measurement`, `tag`, `field`, `with_tag`, `with_field`, `without_tag`,
`without_field`, `docstring`, `matching(predicate)` — all chainable and
repeatable.

Aggregations over one field: `sum`, `sum_f64`, `sum_i64`, `min`, `max`, `mean`,
`p(field, percentile)`, `sample_count`, `values`, `string_values`,
`numeric_values`; plus `count()` and `is_empty()` over the matching points.

Values compare by content, not representation: `.field("n", 1)` matches a point
that recorded `1u8`, `1.0` or `true`, and durations compare as nanoseconds.

## Example

[`examples/influx_report.rs`](examples/influx_report.rs) reports to a real server:

```sh
INFLUXDB_TOKEN=… cargo run --example influx_report -- \
    --url https://influxdb.example.com --org myorganization --bucket metrics --rounds 3 --echo --ping
```

The token is read from `--token` or `$INFLUXDB_TOKEN`; keep it out of the
repository.

## Tests

```sh
cargo test --all      # unit, integration and doc tests
cargo clippy --all --all-targets
```

## Installing

```toml
[dependencies]
mensura = "0.1"
```

The InfluxDB backend is on by default and pulls in `reqwest`. For the macros,
the queue and the stdout backend alone:

```toml
[dependencies]
mensura = { version = "0.1", default-features = false }
```

## Caveats

* Macro expansions name `::mensura`, so the crate has to be reachable under that
  name (no renaming in `Cargo.toml`).
* A doc comment attached *above* a macro invocation is not visible to a
  function-like proc macro; write it inside the parentheses.
* The InfluxDB backend drops points that carry no representable field, since
  InfluxDB rejects them, and reports how many it dropped.

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
