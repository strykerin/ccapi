# Binance Spot protocol comparisons

The two new options are per subscription. JSON WebSocket and textual FIX remain
the defaults, and can run alongside SBE connections in the same Session.

| Path | Subscription option | Requests | Incoming data |
| --- | --- | --- | --- |
| WebSocket market data JSON | default | JSON controls | JSON |
| WebSocket market data SBE | `BINANCE_MARKET_DATA_ENCODING=SBE` | JSON controls | SBE stream schema 1:0 |
| WebSocket order entry JSON | default | JSON | JSON |
| WebSocket order entry SBE | `BINANCE_WEBSOCKET_ORDER_ENTRY_ENCODING=SBE` | JSON | SBE API schema 3:4 |
| FIX market data / order entry SBE | `BINANCE_FIX_ENCODING=SBE` | SBE | SBE FIX schema 1:2 |

These are Spot-only additions. This change does not configure the engine, select
protocols in its strategy, place live orders, or establish a measured latency
advantage. The codecs are generated from vendored Binance schemas by
`tools/generate_binance_protocol_sbe.py`; generation needs only Python's standard
library. There is no XML parsing at runtime. The generated decoders build bounded
RapidJSON values before ccapi normalization; receive timestamps precede decoding.

## WebSocket order entry

Enable `CCAPI_ENABLE_SERVICE_EXECUTION_MANAGEMENT` and
`CCAPI_ENABLE_EXCHANGE_BINANCE`. Use the existing Ed25519 credentials:
`BINANCE_WEBSOCKET_ORDER_ENTRY_API_KEY` and
`BINANCE_WEBSOCKET_ORDER_ENTRY_API_PRIVATE_KEY_PATH` (optional password variable
with the existing `_PRIVATE_KEY_PASSWORD` suffix).

```cpp
Subscription json("binance", "BTCBRL", CCAPI_EM_WEBSOCKET_ORDER_ENTRY,
                  "", "oe-json");
Subscription sbe("binance", "BTCBRL", CCAPI_EM_WEBSOCKET_ORDER_ENTRY,
                 "BINANCE_WEBSOCKET_ORDER_ENTRY_ENCODING=SBE", "oe-sbe");
session.subscribe(std::vector<Subscription>{json, sbe});
```

Wait for each `AUTHORIZATION_SUCCESS` before sending through
`Session::sendRequestByWebsocket(connectionCorrelationId, request)`. Request
construction remains unchanged. For ACK comparisons, set `newOrderRespType=ACK`
explicitly on both paths. Both paths send JSON requests; SBE describes responses.

The SBE connection adds `responseFormat=sbe&sbeSchemaId=3&sbeSchemaVersion=4`
to the WebSocket API URL. New-order ACK/RESULT/FULL, cancellation responses,
errors, session logon and user-data subscription responses are decoded into the
existing ccapi response types and preserve each request's correlation ID.
`ORDER_UPDATE`, `PRIVATE_TRADE` and `BALANCE_UPDATE` subscription fields also work:
execution reports and absolute account-position balances use the existing
normalized events. Delta-only balance events retain the existing JSON behavior
(they are not emitted as absolute balances).

`Message::getTimeReceived()` is ccapi's timestamp at completed frame reception,
before decoding. `Message::getTime()` retains the exchange microseconds where
present. Element keys ending in `_MILLISECONDS` keep their existing units.
Malformed/unsupported SBE messages fail the subscription and close the connection;
there is no silent JSON fallback. Reconcile account state after a disconnect.
Other WS API operations, SOR allocation events and new order lists are outside
this initial benchmark scope; unsupported binary templates fail explicitly.

## FIX SBE

Also enable `CCAPI_ENABLE_SERVICE_FIX`. Set `BINANCE_FIX_API_KEY` and
`BINANCE_FIX_API_PRIVATE_KEY_PATH` to an Ed25519 key pair. Binance requires
`FIX_API` permission for order entry; `FIX_API_READ_ONLY` suffices for market data.

```cpp
Subscription md("binance", "", CCAPI_FIX_MARKET_DATA,
                "BINANCE_FIX_ENCODING=SBE", "fix-md");
Subscription oe("binance", "", CCAPI_FIX,
                "BINANCE_FIX_ENCODING=SBE&25035=1&25036=2&25045=2", "fix-oe");
session.subscribe(std::vector<Subscription>{md, oe});
```

The default production hosts use port **9002** (`fix-md` / `fix-oe`). Custom URLs
in `SessionConfigs` are preserved for loopback/test environments. TLS SNI,
certificate verification and hostname verification are enabled for FIX SBE.
Port 9001's mixed textual-request/binary-response mode is not implemented.
`25036=2` requests only ACKs; `25045=2` selects compact execution ACKs. Omit those
options to receive full execution reports, including subsequent fills/cancels.
`25035=1` permits unordered processing; keep this setting explicit in comparisons.
FIX SBE's `25000` receive window is **microseconds** and is set on Logon only.

Wait for `AUTHORIZATION_SUCCESS`, then use the existing raw FIX API:

```cpp
Request subscribe(Request::Operation::FIX, "binance");
subscribe.appendFixParam({{35,"V"}, {263,"1"}, {264,"1"},
                         {146,"1"}, {55,"BTCBRL"},
                         {267,"2"}, {269,"0"}, {269,"1"}, {262,"bbo"}});
session.sendRequestByFix("fix-md", subscribe);
```

For trades use entry type `269=2`, group count `267=1`, and omit `264`.
For incremental depth use bid and ask entry types and `264=20` (or another depth).
Use distinct `262` request IDs. Group members must follow their count tag;
all other request fields may be in any order. Repeated group entries are retained.

A new order uses `35=D`, `11=clientOrderId`, `55=symbol`, `54=side`, `40=type`,
`38=quantity`, `44=price`, `59=timeInForce`, plus **25054=price exponent** and
**25055=quantity exponent**. Supply these from instrument metadata; do not assume
the same precision for every symbol. Price/quantity tag values remain decimal
strings, converted exactly to mantissas without rounding or floating point.
For example, `44=350000.25` with `25054=-2` encodes mantissa `35000025`.
Missing required fields, unsupported tags and precision loss reject the request
locally before writing or consuming a FIX sequence number.

The encoder covers logon/logout/heartbeat/test request, new order, cancel,
mass cancel, amend-keep-priority, limit queries, instrument queries and market
subscriptions. Cancel-replace and order-list **requests** are not implemented.
The decoder covers all FIX schema 1:2 message templates, including errors,
execution reports, compact ACKs, trades, book ticker and incremental depth.

### FIX event contract

FIX SBE retains ccapi's **raw FIX API**, not its normalized market-data or order
manager API. Consumers use `Event::Type::FIX`, `Message::Type::FIX` and
`Element::getTagValueList()`. It does not automatically feed the engine's current
WebSocket subscriptions or order manager. The benchmark adapter must read these
FIX events explicitly.

- `35` identifies the FIX message type. `BINANCE_SBE_TEMPLATE_ID` in the element's
  name-value map distinguishes trades (205), BBO (206), depth (207), snapshot (204),
  full execution report (98) and compact execution ACK (198).
- Message correlation ID identifies the connection. Match order responses by
  `11` (client order ID), `41` (original client order ID), `37` (exchange order ID),
  or `45` (rejected message sequence). Do not correlate by arrival order.
- Repeating groups are flattened as count tag followed by each entry's fields;
  use the complete ordered tag list, not `getValue(tag)`, to read every entry.
  `25047` / `25048` identify bid / ask groups. A null `271` is absent and means a
  deletion for incremental depth/BBO; do not interpret it as missing data.
- Decimal tags are exact decimal strings. **Timestamp tags, including 52/60,
  are epoch microseconds**, unlike textual FIX's formatted timestamps.
- `Message::getTime()` is header SendingTime, not a matching-engine event timestamp.
  `getTimeReceived()` is local completed-frame read time, before decoding. Several
  frames coalesced into one socket read share that timestamp.
- Incoming sequences must be continuous. Frames are bounded to the configured FIX
  buffer size. Fragmented frames are retained; concatenated frames are all read.
  Decode/sequence errors emit `FIX_FAILURE`, invalidate the connection and reconnect.
  Resubscribe market-data streams after authorization and reconcile outstanding
  orders; in-flight orders are never automatically resubmitted.

## Comparison methodology

Use simultaneous warmed connections on the same VM and symbol. Pair **trades by
trade ID**, BBO by update ID plus matching content, and count missing/unmatched
samples. SBE BBO may cull superseded events. Compare p50/p95/p99 arrival differences
and decoding/handler completion separately; receive timestamps do not include
decoding but application callbacks do. They are not hardware NIC timestamps.

Compare incremental depth across all paths. WebSocket `depth20` is a periodic
snapshot; FIX `MarketDepth=20` selects only the initial snapshot size, followed by
diffs. Publication cadences and batching differ and are not network latency.
Track book sequence ranges and resulting states rather than assuming one frame
on one protocol equals one frame on another.

For order entry, interleave equivalent orders across warmed paths, track by unique
client IDs, and measure API-call-to-response and socket-read-to-handler separately.
A direct ACK and an unsolicited order update are separate measurements. Count
errors/timeouts instead of dropping them from the dataset. No timeout/disconnect
proves an order failed. A live benchmark needs explicit order sizes/prices and a
cleanup/reconciliation policy. Drop Copy has a documented 1-second delay and is
not the low-latency order-ACK path.

## Offline validation

Build the `binance_protocol_sbe` unit target and run it. The loopback case skips
unless launched by:

```sh
python3 test/run_binance_protocol_loopback.py path/to/binance_protocol_sbe
```

The wrapper creates disposable keys/certificates and runs local TLS servers for
JSON/SBE WebSocket and two simultaneous FIX SBE sessions. It checks request
formats, logon, create/cancel ACK routing, fragmented/coalesced FIX frames, reconnect/resubscription and
market-data delivery. It never connects to Binance. Fixtures come from the
independent Python `struct` packer in `test/generate_binance_protocol_fixtures.py`.

Sources: [FIX SBE](https://github.com/binance/binance-spot-api-docs/blob/master/fix-api.md#fix-sbe),
[SBE FAQ](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/sbe_faq.md),
[market streams](https://github.com/binance/binance-spot-api-docs/blob/master/sbe-market-data-streams.md).
