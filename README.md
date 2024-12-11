# <p style="text-align: center">flat_db</p>

A basic flat file database implementation for Rust.

- Supports any struct serializable by serde.

- Keys are hexidecimal hashes.

- Multiple items can be grouped per file to minimize I/O.

- `YAML` is the default file format but could easily be switched to `JSON`.

- Database files are easily commited backed up, restored etc with git.

- Database files are easily read with `jq` or `yq`.

- Simple lock file mechanism protects data during writes.

## Releases and Changes

Releases and a full changelog are available via [GitHub Releases](https://github.com/RogueOneEcho/flat_db/releases).

Release versions follow the [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html) specification.

Commit messages follow the [Conventional commit](https://www.conventionalcommits.org/en/v1.0.0/) specification.
