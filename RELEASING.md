# Releasing

`mensura` depends on `mensura-macros` by both path and version, so the two are
published together, macros first — crates.io will reject `mensura` until the
`mensura-macros` version it names exists.

## Before the first publish

- [ ] `cargo login` with a crates.io token.
- [ ] Push `main` to `git@github.com:bldr-project/mensura.git`, so the
      `repository` link on crates.io resolves.

## Every release

1. Bump `version` in `[workspace.package]` — both crates share it — and update
   the `mensura-macros` version in `[workspace.dependencies]` to match.
2. Move the `CHANGELOG.md` entries under a new version heading and add the
   comparison links.
3. Check the gates:
   ```sh
   cargo fmt --all -- --check
   cargo clippy --all --all-targets -- -D warnings
   cargo test --all
   cargo build -p mensura --no-default-features
   cargo doc --no-deps -p mensura
   cargo package --workspace          # builds each crate from its packaged form
   ```
4. Publish, macros first:
   ```sh
   cargo publish -p mensura-macros
   cargo publish -p mensura
   ```
5. Tag: `git tag -a v0.1.0 -m 'mensura 0.1.0' && git push --tags`.

## Notes

- If `cargo package --workspace` fails to verify with an error that looks like
  stale macro output, run `cargo clean` first. Cargo treats registry versions as
  immutable, so re-packaging the *same* version after changing `mensura-macros`
  reuses the previous build. It cannot happen for a published release, only
  while iterating before one.

- The `LICENSE-*` files in each crate are symlinks to the workspace root; cargo
  copies their contents into the `.crate`, which `cargo package --list` confirms.
- `README.md` at the workspace root is a symlink to `mensura/README.md`, so
  crates.io and the repository landing page stay in step.
- Publishing is irreversible: a version can be yanked but never replaced. Run
  `cargo package --workspace` and read `cargo package --list` first — anything
  listed there is public forever.
