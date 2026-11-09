# Binance Spot SBE market data

Opt in **per market-data subscription** with `BINANCE_MARKET_DATA_FORMAT=SBE`.
Omitting the option (or setting `JSON`) preserves the existing JSON behavior.
This implementation is confined to Binance Spot market data. Order entry,
cancellations, ACKs, and private order/trade/balance subscriptions continue using
their existing JSON services. No global protocol switch is used.

Official references, checked on 2026-11-09:

- [SBE market-data streams](https://github.com/binance/binance-spot-api-docs/blob/master/sbe-market-data-streams.md)
- [SBE schema stream_1_0.xml](https://github.com/binance/binance-spot-api-docs/blob/master/sbe/schemas/stream_1_0.xml)
  (vendored [here](stream_1_0.xml), schema ID **1**, version **0**, little endian)
- [Local order-book synchronization](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly)

| ccapi field | Options in addition to `BINANCE_MARKET_DATA_FORMAT=SBE` | Actual stream | SBE template |
|---|---|---|---|
| `MARKET_DEPTH` | `BINANCE_MARKET_DATA_STREAM=depth&MARKET_DEPTH_MAX=20` | `btcusdt@depth` | `DepthDiffStreamEvent` (10003) |
| `TRADE` | none | `btcusdt@trade` | `TradesStreamEvent` (10000) |
| `MARKET_DEPTH` | `BINANCE_MARKET_DATA_STREAM=bestBidAsk&MARKET_DEPTH_MAX=1` | `btcusdt@bestBidAsk` | `BestBidAskStreamEvent` (10001) |

Symbols in stream names are lowercase. `depth` is the SBE `MARKET_DEPTH` default,
even with `MARKET_DEPTH_MAX=1` or `20`. The local depth option controls the levels
reported to the consumer (1–5000); the underlying book retains additional known
levels to promote them when better levels are deleted. The implementation never
selects `depth20` (periodic top-20 snapshots, 50 ms), and rejects that option and
its binary template 10002. The normal `MARKET_DEPTH_RETURN_UPDATE=1` option can
be used to report local-book changes instead of the current top N levels.

The incremental `depth` cadence is **nominally 20 ms**, not a network latency
guarantee or a promise to send a message every 20 ms without changes. `trade` and
`bestBidAsk` update in real time. Binance may cull outdated `bestBidAsk` events
under load; sequence gaps in this ticker are expected and do not invalidate the
incremental book.

## Credentials and transport

Provide `BINANCE_SBE_API_KEY` as an environment variable, in `SessionConfigs`
credentials, or in the individual `Subscription` credential map. The latter
overrides the default. It must be the **API key of an Ed25519 key pair**; Binance
validates the key type. No private key, timestamp, or signature is required for
these public streams. Trading permissions are not required. Any configured IP
whitelist still applies. This dedicated credential does not replace
`BINANCE_API_KEY` or any order-entry credentials.

The service connects to `wss://stream-sbe.binance.com:9443/ws` and supplies
`X-MBX-APIKEY` in the HTTP upgrade request. Binance also documents port 443.
SBE TLS connections verify the peer certificate and hostname using the system
OpenSSL trust paths.
SBE subscriptions use separate connection groups from JSON subscriptions.
Subscription control messages and responses use **JSON text frames**:

```json
{"method":"SUBSCRIBE","params":["btcusdt@depth"],"id":0}
```

The deterministic channel-selection test asserts this exact stream for local
depths 1, 5, 20, 21, and 5000. Payloads arrive as **binary frames** with the SBE
header, followed by the event body and symbol. Frames have no JSON envelope.
Unsupported schema/version, unknown templates, invalid frames, subscription
errors, and handshake/authentication failures produce correlated subscription
status errors. There is no JSON or partial-snapshot fallback. Reconnection uses
the same SBE endpoint, credential, and subscription options.

The existing Beast transport replies automatically to server PINGs with PONGs
carrying the same payload. Existing heartbeat and reconnect handling remain in
use, including the 24-hour connection lifetime. A JSON `serverShutdown` event
starts reconnection. SBE credentials and handshake headers are redacted from
subscription/connection diagnostic strings.

## Book and normalized events

The first diff starts a public JSON REST snapshot request to
`/api/v3/depth?symbol=BTCUSDT&limit=5000`. Diffs are buffered while it is pending.
Events with `u <= lastUpdateId` are discarded. The first remaining diff must
cover the next update (`U <= lastUpdateId + 1 <= u`); subsequent diffs must cover
the next update too. The entire buffered chain is checked before publishing a
solicited snapshot followed by its buffered updates. An ahead-of-stream snapshot
waits for the next applicable diff. An old snapshot is retried, with bounded
exponential backoff (1–30 seconds), while buffering continues.

Gaps or more than 10000 buffered diff events cause an explicit error and a fresh
connection/snapshot. Old snapshot callbacks are ignored after close, failure,
or reconnect. REST transport/status/parse failures report errors and retry.
Zero quantities delete levels; quantities are absolute, not additive. As with
the official protocol, unchanged levels outside the initial 5000 per side are
not known until updated by the stream.

Consumers receive the usual `MARKET_DATA_EVENTS_MARKET_DEPTH` and
`MARKET_DATA_EVENTS_TRADE` messages, with their subscription `correlationId`.
Prices and quantities are exact decimal strings. Every trade in a batch retains
its 64-bit trade ID and `IS_BUYER_MAKER` (`1`: seller aggressor; `0`: buyer
aggressor). Trade message time is `transactTime`; book/ticker message time is
`eventTime`, both decoded as **microseconds**. `timeReceived` is local reception
time; buffered diffs retain their original reception time. REST snapshots have
no exchange timestamp, so their recap uses local snapshot reception time.

Without optional conflation, elements also expose `EVENT_TIME_MICROSECONDS`,
`SEQUENCE_NUMBER` (book/ticker final update ID), and `FIRST_SEQUENCE_NUMBER`
(incremental book first update ID). A REST recap has its snapshot sequence ID
and no exchange event-time field. Conflated output follows ccapi's existing
interval semantics; per-frame metadata is omitted for previous intervals.

## Minimal example and tests

See [the three-feed example](../../example/market_data_binance_sbe/main.cpp).
It runs for 30 seconds, prints normalized events, and sends no orders.

```sh
cmake -S example -B build/examples
cmake --build build/examples --target market_data_binance_sbe -j2
# Set BINANCE_SBE_API_KEY securely in the process environment before running:
./build/examples/market_data_binance_sbe/market_data_binance_sbe

cmake -S test -B build/tests -DBUILD_TEST_UNIT=ON
cmake --build build/tests --target market_data_binance_sbe -j2
ctest --test-dir build/tests -R BinanceSbe --output-on-failure
```

CMake accepts the repository's usual `BOOST_INCLUDE_DIR`, `RAPIDJSON_INCLUDE_DIR`,
`HFFIX_INCLUDE_DIR`, and OpenSSL settings for installed dependencies. No SBE code
generator or new runtime dependency is needed. Tests use synthetic wire fixtures
matching the vendored schema, simulated snapshots, and a loopback WebSocket server
for handshake errors, JSON controls, fragmented binary frames, ping/pong,
reconnection, and shutdown; they do not require keys or connect to Binance.
