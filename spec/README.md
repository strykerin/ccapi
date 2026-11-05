# Binance SBE schemas

These files are vendored from Binance's public Spot API documentation:

- [spot-fixsbe-1_2.xml](https://raw.githubusercontent.com/binance/binance-spot-api-docs/master/sbe/schemas/spot-fixsbe-1_2.xml): FIX schema 1:2.
- [spot_3_5.xml](https://raw.githubusercontent.com/binance/binance-spot-api-docs/83f51f523bb61719baa7545b96f95f553606e596/sbe/schemas/spot_3_5.xml): WebSocket/REST schema 3:5, copied from its original upstream commit.
- [spot_3_4.xml](https://raw.githubusercontent.com/binance/binance-spot-api-docs/master/sbe/schemas/spot_3_4.xml): WebSocket/REST schema 3:4, retained for backward-compatibility fixtures.

The similarly named `spot-fixsbe-1_1.xml` upstream entry currently contains an
alias to `spot-fixsbe-1_2.xml`; the complete XML is checked in here. The generator
uses the checked-in files, never a moving `latest` URL. Update the decoder,
fixtures and subscription negotiation together when upgrading a schema.

FIX requests currently target 1:1, filtering newer fields out of the 1:2 XML.
Production market-data logon rejected 1:2 (`-1152`) and accepted 1:1 on
2026-11-05. Decoders retain support through 1:2. Upstream aliases must not be
used as proof that a schema version is deployed to production.

Regenerate headers with `python3 tools/generate_binance_protocol_sbe.py` and
fixtures with `python3 test/generate_binance_protocol_fixtures.py`.

WebSocket API responses observed on 2026-11-05 used schema 3:5 even when
requesting 3:4. Connections now request 3:5 explicitly; the decoder also accepts
3:4 responses and rejects unimplemented newer schemas with the received and
supported versions in the error. The current upstream `spot_3_5.xml` is an
alias to 3:6, so the immutable original commit above is the source of this XML.
