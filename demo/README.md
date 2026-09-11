# Demo cluster

A throwaway Kafka cluster on your machine, filled with made-up data — enough to
take screenshots of every reading mode mikui has without pointing it at a real
cluster.

## Run it

```sh
./demo/mikui-demo.sh up
```

Requirements: Docker with compose v2, and `python3`. Nothing else — the seeding
script speaks the Kafka protocol itself, so there is no client library, no pip
and no brew step.

The first run pulls two images and unpacks about 5 GB, almost all of it the
Confluent Schema Registry (the broker is 0.6 GB). After that, `up` takes a few
seconds. To reclaim the space once the screenshots are taken:
`docker image rm confluentinc/cp-schema-registry:7.9.0 apache/kafka:3.9.1`.

Then, in mikui, **Add cluster**:

| field | value |
|---|---|
| brokers | `localhost:19092` |
| security | `PLAINTEXT` |
| schema registry | `http://localhost:18081` |

## Commands

```sh
./demo/mikui-demo.sh up          # start, create topics, produce data
./demo/mikui-demo.sh seed        # produce again into an already running cluster
./demo/mikui-demo.sh reset       # wipe and start over (same data, same seed)
./demo/mikui-demo.sh status      # what is running, and where to connect
./demo/mikui-demo.sh topics      # topic list with partition counts
./demo/mikui-demo.sh logs        # broker and registry logs
./demo/mikui-demo.sh down        # stop and remove everything
```

`up --scale 4` produces four times as many messages; `up --scale 0.1`, a tenth.
The generator is seeded with a constant, so a `reset` gives back byte-identical
messages and a screenshot can be retaken exactly.

Nothing persists: there is no volume, so `down` takes the data with it. Bringing
it back up takes a few seconds.

## What is in there

35 topics with varied partition counts and configs (compacted ones, short and
long retention, per-topic compression) — most of them deliberately empty, so the
topic list and its filtering look like a real cluster.

Five topics carry data, one per decoding path:

| topic | what it shows |
|---|---|
| `identity.users.proto.v1` | **Protobuf** in the Confluent wire format. One message covers strings, ints, a double, a bool, an enum, `repeated`, a `map`, nested messages, `bytes` and a proto3 `optional`. Loading a PROTOBUF schema from the registry is not supported yet, so decoding needs the local file: set the format to `proto`, add `demo/schemas/user.proto` and pick the `demo.identity.v1.User` message. Until then the bodies show up as `[binary]`. |
| `payments.transactions.v1` | **Avro** from the registry: unions, enums, arrays, maps, a nested record, `timestamp-millis`. |
| `risk.scoring.requests.v1` | **JSON Schema** from the registry — including validation in the producer editor. |
| `orders.created.v1` | Plain **JSON**, no schema at all: nested objects, arrays, nulls. |
| `crm.customers.cdc` | A **Debezium** envelope, so the `before`/`after` lens and the operation labels (`create`, `update`, `delete`, `read`) have something to show. |

Every message carries a key and a few headers, and timestamps are spread over
the last days — so ordering, the key filter, the `from`–`to` range and the
headers tab all have something to display.

## Screenshots

Raw shots live in `screenshots/`. Before they go anywhere public they get a
disclaimer stamped on:

```sh
./demo/watermark.sh              # screenshots/*.png → screenshots/watermarked/
./demo/watermark.sh --in-place   # overwrite the originals instead
```

Each image gets a caption strip under the frame (covers none of the UI) and a
faint repeated `SYNTHETIC DEMO DATA` across it, which survives cropping. Light
and dark shots get their own palette — the strip is picked from the average
brightness of the frame, so it reads as part of the screenshot rather than as a
black band glued to the bottom. `--opacity`, `--tile`, `--badge` and `--text`
override the defaults; `--help` lists them.

## Files

- `docker-compose.yml` — one Kafka broker in KRaft mode plus Confluent Schema
  Registry. Real Kafka rather than a stand-in: the topic configuration shown by
  `DescribeConfigs` ends up on the screenshots too, and it should be the same
  configuration a live cluster has.
- `seed.py` — creates the topics, registers the schemas, generates and produces
  the messages.
- `watermark.sh`, `watermark.m` — the disclaimer stamp. Objective-C over
  CoreText, compiled on demand: neither ImageMagick nor Pillow is a given on a
  Mac, while CoreText is.
- `schemas/user.proto`, `schemas/transaction.avsc`,
  `schemas/scoring-request.schema.json` — the registered contracts. They are
  import-free on purpose, so the same files also work when loaded into mikui as
  local schema files.
