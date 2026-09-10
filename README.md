# mikui

A desktop Kafka messages viewer. Browse clusters, read and search topics with
schema-aware decoding, produce validated messages — locally, without touching
the data plane of anyone else.

Built with [Tauri 2](https://v2.tauri.app): a Rust backend that talks to Kafka
and owns the data, and a React frontend that only ever sees the visible
viewport.

## Features

### Clusters

- **Multiple saved clusters** with named connections; everything is stored
  locally in the app config directory.
- **Full security matrix**: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`;
  SASL `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`; mutual TLS with a client
  certificate; a custom CA bundle; optional hostname-check and
  certificate-verification switches. Passwords live in the OS keychain, not in
  config files.
- **Confluent Schema Registry** per cluster — URL, basic auth, and its own CA
  bundle (the registry often sits behind a different front than the brokers).
- **Several users per cluster, switchable on the fly**: keep a personal account
  and a service one, and change the active user without re-entering
  credentials.

### Topics

- Topic list with instant, case-insensitive filtering by name; sorted with a
  locale-aware collator, and shortest matches first while searching.

### Reading messages

- **Choose the partitions** to read from, or take them all.
- **Choose the starting point**: newest messages (tail), oldest, or an exact
  `from`–`to` range by offset or by timestamp.
- **Smart ordering**: partitions are merged by timestamp with the offset as a
  tiebreaker, so the view reads like one log regardless of where the messages
  physically are.
- **Format-aware decoding**: `text`, `json`, `json schema`, `proto`, `avro`,
  and `hex` (raw bytes, to copy and carry away).
- **Schema Registry integration**: the subject for a topic is detected
  automatically, the format switches to match, and the schema can also come
  from local `.avsc`/`.proto`/`.json` files.
- **Debezium / Kafka Connect envelopes** are recognized and shown as a lens:
  `before`/`after` diff with human operation names (`create`, `update`, …),
  next to the untouched full envelope.
- **Filters and search over the decoded body** — by key or by body, with or
  without case sensitivity; filtering happens in Rust, before anything crosses
  the UI boundary.
- **Deep search over the whole topic**: runs in the background without
  blocking the view, and can be stopped at any moment.
- **Syntax highlighting and pretty-printing** for decoded bodies; large
  messages are formatted on demand.
- **Keyboard navigation**: `↑`/`↓` walk the message list, `←`/`→` cycle the
  detail tabs (`payload`, `envelope`, `headers`) — no mouse needed.

### Topic info

- Partition count, message counts and their distribution across partitions,
  with per-message size estimates.
- When `DescribeConfigs` is available, the full topic configuration is shown.

### Producing

- Send in any of the reading formats — `json`, `avro`, `proto`, `hex`, and the
  rest.
- **Validation against the topic schema** before sending: the editor explains
  in human terms why the entered value does not fit the contract, rather than
  quietly writing a message no consumer can read.
- **Placeholders worth sending**: a one-click skeleton from the JSON Schema,
  an example Avro record, or a Protobuf template pre-filled with plausible
  values (random UUIDs, realistic timestamps).
- Syntax highlighting and formatting in the producer editor too.

### Favorites and sharing

- **Save any message to disk** — favorites persist across restarts, together
  with the full body.
- **Share a link to a specific message** (`mikui://message/v1?…`): it opens the
  exact message on a colleague's machine, matching the cluster by its Kafka
  `cluster.id` and asking for confirmation before connecting.

## Development

### Prerequisites

- **Node.js 22+** and npm
- **Rust** (stable toolchain, via [rustup](https://rustup.rs))
- Platform extras:
  - **macOS** — Xcode Command Line Tools
  - **Linux** — `libwebkit2gtk-4.1-dev librsvg2-dev patchelf` plus the usual
    build tools (`gcc`, `make`, `perl` — the latter two also cover the vendored
    OpenSSL build)
  - **Windows** — Visual Studio with the *Desktop development with C++*
    workload, and CMake with Ninja. The `cmake` crate that builds librdkafka
    requires an explicit generator: run builds from a VS environment, or set
    `CMAKE_GENERATOR=Ninja` (see the notes in `src-tauri/Cargo.toml` and
    `.github/workflows/ci.yml` for the why)

### Run and build

```sh
npm install          # once
npm run tauri dev    # dev mode with hot reload
npm run tauri build  # production build, bundled per platform
```

Useful extras:

```sh
npm run build                                  # frontend only (tsc + vite)
cargo test --manifest-path src-tauri/Cargo.toml  # backend unit tests
npm run icons                                  # regenerate app icons (python3)
```

### Where things live

Connections, favorites, and settings are stored in the standard per-OS app
config directory (for example
`~/Library/Application Support/com.nahua.mikui` on macOS): `clusters.json` for
cluster definitions, `favorites.json` plus a bodies directory for saved
messages. Passwords and key passphrases never touch those files — they go to
the OS keychain.

## Architecture

Rust owns the data, JS owns the viewport. Messages live in the backend in a
compact form; only the rows physically visible on screen cross the IPC
boundary. Filtering, decoding, and partition merge happen on the Kafka worker
thread, before serialization. This keeps a multi-gigabyte topic scrollable
while the frontend stays light.

## CI

Every push and pull request builds the app on Ubuntu, Windows, and macOS
runners (`.github/workflows/ci.yml`) — `--locked`, against the committed
`Cargo.lock`.
