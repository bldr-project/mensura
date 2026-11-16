// Optional: this repository's build definition for bldr (a content-addressed
// CI/CD system), which checks, lints and tests both crates as a workspace
// member. Plain cargo needs none of this; see RELEASING.md.
//
// `bldr/rust-tools` and `rust` are names the consuming bldr workspace gives
// its Rust build library and Rust toolchain image.

import { Cargo, CargoToolchain } from "bldr/rust-tools";
import rust from "rust";

/**
 * Both crates, `mensura` and `mensura-macros`: `check`, `clippy` and `test`
 * targets on this scope. The tests are the libraries' unit tests and the
 * `tests/` integration suites; the doc-tests are cargo's, run by hand.
 */
export const lib: Cargo = new Cargo({
    scope,
    toolchain: new CargoToolchain({ baseImage: rust }),
    source: member.rootDirectory,
    test: { lib: true, tests: true, noFailFast: true, timeoutSecs: 600 },
});
