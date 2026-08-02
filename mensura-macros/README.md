# mensura-macros

The `measure!`, `count!` and `timer!` procedural macros for
[mensura](https://crates.io/crates/mensura), a metrics instrumentation library
with InfluxDB semantics.

This crate is an implementation detail. It expands to paths rooted at
`::mensura`, so it is unusable on its own — depend on `mensura`, which
re-exports all three macros:

```toml
[dependencies]
mensura = "0.1"
```

```rust
use mensura::{count, measure, timer};

measure!("call_latency", host = "web-1", ?ms = 12.5);
count!("http_request", route = "/search", status = 200);
let _timer = timer!("query_duration", operation = "select");
```

See the [mensura documentation](https://docs.rs/mensura) for the full macro
syntax.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
