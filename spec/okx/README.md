# OKX SBE market data

Set `OKX_MARKET_DATA_FORMAT=SBE` on each market-data `Subscription`. Omit it
(or set `JSON`) to retain the existing JSON behavior. Order entry, cancellations,
ACKs, private executions, order updates and balance updates keep their existing
WebSocket + JSON services. This option does not select a private SBE service.

Official references, checked on 2026-11-09:

- [SBE market data, authentication, sequences and snapshots](https://www.okx.com/docs-v5/en/#order-book-trading-sbe-market-data)
- [Order book channel and nominal cadence](https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-channel)
- [Official XML schema](https://www.okx.com/docs-v5/log_en/xml/okx_sbe_1_0.xml),
  vendored as [okx_sbe_1_0.xml](okx_sbe_1_0.xml): schema **1**, version **0**, little endian
  (SHA-256 `c54d597bb346b9a1a1af510d76ac113b1663b3b1ed5d61b0bdff61f1da35f929`).
- [Get instruments](https://www.okx.com/docs-v5/en/#public-data-rest-api-get-instruments)
- [API changelog](https://www.okx.com/docs-v5/log_en/) (port 443 and VIP requirements).

## Subscriptions

Each subscription must specify one instrument and its current integer
`INST_ID_CODE` option. Obtain the code from `GET_INSTRUMENT` (with
`INSTRUMENT_TYPE=SPOT`, `SWAP`, etc.) or `GET_INSTRUMENTS`; ccapi exposes it as
`INST_ID_CODE`. Do not hard-code codes across environments or relistings. OKX may
change the code on relisting and retains it on renaming. The caller supplies the
instrument/code mapping; the service rejects missing or invalid codes rather
than guessing. The [example](../../example/market_data_okx_sbe/main.cpp) obtains
the mapping before subscribing. Instrument types and native quantity units are
those supported by OKX's SBE market-data service, not restricted to Spot.

Add these options to `OKX_MARKET_DATA_FORMAT=SBE&INST_ID_CODE=<current code>`:

| ccapi field | Additional options | Actual SBE channel | Template IDs |
|---|---|---|---|
| `MARKET_DEPTH` | `OKX_MARKET_DATA_STREAM=books-l2-tbt&MARKET_DEPTH_MAX=20` | `books-l2-tbt` | 1001, 1002 |
| `TRADE` | none (or `OKX_MARKET_DATA_STREAM=trades`) | `trades` | 1005 |
| `MARKET_DEPTH` | `OKX_MARKET_DATA_STREAM=bbo-tbt&MARKET_DEPTH_MAX=1` | `bbo-tbt` | 1000 |

The default SBE depth channel is always **`books-l2-tbt`**, including when the
local depth is 1, 20 or 50. `MARKET_DEPTH_MAX` controls local output (1–400 levels),
independently of channel choice or optional conflation. Known deeper levels are
retained to promote them when a better level is deleted. The normal
`MARKET_DEPTH_RETURN_UPDATE=1` option reports local depth changes.

Incremental depth has a **nominal 10 ms cadence when the book changes**. This is
not a network latency guarantee or a promise of updates without changes.
SBE `bbo-tbt` normally sends best bid/ask updates in **real time**, unlike the
10 ms JSON BBO. OKX explicitly permits BBO data loss under overload, differing
between connections; BBO sequence jumps do not trigger depth resynchronization.
Trades are public trades, including the aggregation count provided by OKX.

## Authentication and transport

Provide `OKX_API_KEY`, `OKX_API_SECRET` and `OKX_API_PASSPHRASE` in environment
variables, `SessionConfigs` credentials, or a complete per-subscription credential
map. A nonempty subscription credential map replaces the service defaults.
Production `trades` and `books-l2-tbt` require **VIP4 or higher**; demo requires
VIP1. BBO requires authentication but no minimum VIP tier. API key permissions
and any IP whitelist must allow the connection. No orders are needed.

The endpoint is `wss://ws.okx.com/ws/v5/public-sbe` on **443**. The configured
OKX WebSocket base host is respected, with `/ws/v5/public-sbe` selected only for
these subscriptions. Demo uses `wss://wspap.okx.com/ws/v5/public-sbe`; use a
matching demo instrument code and credential configuration. JSON and SBE are
assigned separate connection groups.

Authentication is in the HTTP upgrade headers: `OK-ACCESS-KEY`,
`OK-ACCESS-PASSPHRASE`, `OK-ACCESS-TIMESTAMP` (Unix seconds) and
`OK-ACCESS-SIGN` (Base64 of HMAC-SHA256 over
`timestamp + "GET" + "/users/self/verify"`). Headers are regenerated on reconnect.
There is no JSON login message on this endpoint. SBE WebSocket TLS connections verify the
peer certificate and hostname using the OpenSSL system trust paths. Credentials,
signatures and authentication headers are redacted from diagnostic strings.

Subscriptions and acknowledgements remain **JSON text frames**, for example
(the numeric code below is illustrative):

```json
{"op":"subscribe","args":[{"channel":"books-l2-tbt","instIdCode":211874}]}
```

Successful `SUBSCRIPTION_STARTED` events include the actual channel/code in
`INFO_MESSAGE`, with the subscription's `correlationId`. Binary WebSocket frames
contain SBE market data. The decoder explicitly rejects unknown schema/version,
templates, invalid enums, invalid lengths, truncated or trailing data. HTTP
authentication failures and JSON subscription errors are reported explicitly,
including safe numeric subscription error codes. There is no fallback to JSON,
`books50-l2-tbt`, `books5` or another snapshot stream.

Beast replies to server PING frames with a PONG carrying the same payload.
Protocol heartbeat/reconnect handling remains active; the JSON application
`ping` timer is disabled only for SBE connections. Notice `64008` triggers
reconnection. Shutdown stops processing and invalidates pending snapshot work.

## Book synchronization and normalized events

On the first incremental or exponent event, buffer the stream and request the
public REST snapshot `/api/v5/market/books-sbe?instIdCode=<code>&source=0` using
the configured OKX REST host. Success must be `application/sbe`, template **1006**,
for the requested instrument. This is an initial 400-level snapshot generated
approximately every 500 ms; it does not replace the 10 ms incremental stream.

Discard buffered events with `seqId <= snapshot.seqId`. The first remaining
event must satisfy `prevSeqId <= snapshot.seqId < seqId`. Subsequent events must
link exactly (`prevSeqId == previous seqId`). Buffered continuity is checked
before publishing the recap. A snapshot matching the last buffered sequence can
publish immediately, even without further changes. An ahead-of-stream snapshot
waits for a bridging event. An old snapshot, HTTP/transport error or JSON REST error is reported and
retried with exponential backoff from 1 to 30 seconds. Invalid binary snapshots,
gaps and buffer overflow (10000 events) trigger an explicit error and a fresh
connection/snapshot. Callbacks from a previous connection/book generation are
ignored.

Template **1002** advances sequence and exponent state without emitting a book
change. Levels are stored as exact decimal values, so existing levels do not
need to be rescaled; each 1001 event carries its own exponents. Quantities are
absolute; zero deletes a level. Empty heartbeat events with equal sequences are
accepted. After synchronization, maintenance resets (`seqId < prevSeqId`) are
applied when `prevSeqId` matches the local sequence. Resets during initialization
restart synchronization to avoid mixing snapshots from different sequence epochs.

Consumers retain the normal `MARKET_DATA_EVENTS_MARKET_DEPTH` and
`MARKET_DATA_EVENTS_TRADE` messages and correlation IDs. Prices and quantities
use exact decimal strings, never `double`. Message time uses **`tsUs` in
microseconds**; OKX currently documents only millisecond precision for this field.
Local `timeReceived` is retained for buffered events. `outTime` also uses
microseconds but comes from another server, so it need not be after `tsUs`.

Without optional conflation, elements expose `INST_ID_CODE`, `SEQUENCE_NUMBER`,
`EVENT_TIME_MICROSECONDS`, and `OUT_TIME_MICROSECONDS` (absent on REST recaps).
Depth updates also expose `PREVIOUS_SEQUENCE_NUMBER`. Trades preserve the 64-bit
`TRADE_ID`, `IS_BUYER_MAKER` (`1` for seller aggressor), `TRADE_COUNT` and raw
`TRADE_SOURCE` (schema values 0=NORMAL, 1=ELP). Order-count fields are decoded and
validated but are not part of ccapi's normalized depth interface. For conflated
output, per-frame metadata is omitted for previous intervals.

## Example and validation

The [three-feed example](../../example/market_data_okx_sbe/main.cpp) looks up
BTC-USDT, subscribes, prints normalized events for 30 seconds, and sends no orders.

```sh
cmake -S example -B build/examples
cmake --build build/examples --target market_data_okx_sbe -j2
# Supply the three OKX credentials securely in the process environment:
./build/examples/market_data_okx_sbe/market_data_okx_sbe

cmake -S test -B build/tests -DBUILD_TEST_UNIT=ON
cmake --build build/tests --target market_data_okx_sbe -j2
ctest --test-dir build/tests -R OkxSbe --output-on-failure
```

CMake accepts the usual `BOOST_INCLUDE_DIR`, `RAPIDJSON_INCLUDE_DIR`,
`HFFIX_INCLUDE_DIR` and OpenSSL settings. No new runtime dependency or SBE code
generator is required. Deterministic tests use synthetic schema fixtures, fake
credentials, simulated snapshots and a loopback WebSocket server. They cover
exact decimals, timestamps, all five templates, synchronization, gaps, exponent
updates, resets, malformed frames, authentication, JSON controls, fragmented
binary frames, ping/pong, reconnection, shutdown and preservation of JSON behavior.
