# Scala 2.13 compatibility surface (Todo 22)

Scala 3.9.0 is the authoritative API baseline. Scala 2.13.18 is a retained
compatibility target for the protocol-independent server stack: the same
artifacts cross-publish, the same semantics hold, and version-specific syntax
differences live in version-specific sources. No Scala 3 API was changed or
reduced for Scala 2.

## Artifact matrix

All JVM modules cross-build `2.13.18` + `3.9.0` (`build.mill`
`scalaVersions`); CI runs the full `testEngine` on both.

| Artifact (`dev.zio`) | 2.13 published | Version split | Protocol surface on 2.13 |
|---|---|---|---|
| `zio-http-core` | yes | `scala-2` / `scala-3` (pre-existing) | shared route/context/handler model; macros differ per version, behavior pinned per version |
| `zio-http-server` | yes | `scala-2` / `scala-3` (this todo) | `ProtocolId`, `ProtocolEngine`, `EngineRegistry`, `AppProtocol`, `ProtocolSet`, `TransportKind`, `Negotiation`, `ConnectorValidation`, `Connector`, `EngineDispatcher`, `Server` — all retained; `zio.http.compat.Scala2ProtocolCompat` holds the 2.13 entry points |
| `zio-http-server-loom` | yes | `src/main/scala-2` / `scala-3` + `src/test/scala-2` / `scala-3` (this todo) | `H1Transport`, `H2Engine`, `H1H2TlsEngine`, `H1H2CleartextEngine`, `LoomServer`, dispatch, aggregate lifecycle — all retained; `zio.http.compat.Scala2EngineCompat` holds the 2.13 constructors |
| `zio-http-h1-codec` | cross-built, snapshot-list gap owned by Todo 19 | none — shared sources, zero deltas | `H1Model`, `H1Limits`, `H1Framing`, `H1Decoder`, `H1Encoder`, `H1Error` — all retained, no facade needed |
| `zio-http-h2-codec` | yes | none — shared sources, zero deltas | frame layer retained, no facade needed |
| `zio-http-client-java` | yes | none | test-scope driver only, untouched |
| `zio-http-endpoint`, `zio-http-zio`, `zio-http-testkit`, `zio-http-client` | yes | endpoint pre-split; others shared | outside the protocol-server surface, untouched |

Note: at this base `snapshot.yml` publishes
`{core,server,client,endpoint,h2Codec,serverLoom,clientJava,zio,testkit}`;
the `h1-codec` snapshot entry is Todo 19's integration scope, not this todo's.

## Retained facades (delegate, never fork)

- `zio.http.compat.Scala2ProtocolCompat` (`zio-http-server`, `scala-2`
  sources): protocol-set constants, `registryOf`, `singleEngineRegistry`,
  `protocolSetOf`. Forwards to `EngineRegistry.build` / `ProtocolSet.fromSeq`.
- `zio.http.compat.Scala2EngineCompat` (`zio-http-server-loom`, `scala-2`
  sources): `h1Engine`, `h2Engine`, `withEngines`. Forwards to `H1Transport` /
  `H2Engine` / `LoomServer.withEngines`.
- `zio.http.compat.Scala3ProtocolCompat` / `Scala3EngineCompat` (`scala-3`
  sources): same signatures, idiomatic Scala 3 syntax. Parity between each
  pair is pinned by `Scala213ProtocolConsumerSpec` /
  `Scala3ProtocolConsumerSpec` (registry paths, typed failures, engine claims,
  H3 refusal, one live H1 loopback each).

Depending on the facades needs no new coordinates: they ship inside the
existing `zio-http-server` / `zio-http-server-loom` artifacts.

## Omitted APIs

None on this stack: every protocol-server API in the table above
cross-compiles and cross-passes. The rule for future work stands — if a
feature cannot be expressed faithfully on Scala 2, omit that Scala 2 API with
a migration note here instead of adding a compromised abstraction, so the gap
fails at compile/dependency resolution, never as a runtime stub.

Documented unsupported capability (both versions, by design): H3/QUIC has no
installed engine. `Protocol.H3` exists in the model but never validates:
`ProtocolSet.fromLegacy(H3)` reports `ConnectorFailure.H3NotAdvertised`, UDP
bindings fail `ConnectorValidation.validateTransport`, and `LoomServer.serve`
refuses with `InvalidConnector` before any socket is bound.

## Scala 2.13 authoring subset (mandatory for shared sources)

Shared `scala/` sources must stay in the common subset, or the 2.13 leg fails
while 3.9 stays green:

- regular-class construction uses explicit `new` (`new H1Transport(...)`,
  `new H2Engine(...)`, `new EngineDispatcher(...)`);
- `Set`/`List` literals that cross an API boundary carry explicit element
  ascriptions (`Set[ProtocolId](...)`, `List[ProtocolEngine](...)`) or flow
  through a facade helper with a declared result type;
- `with`, never `&`, for compound types;
- `@experimental` on objects/traits/classes only — never on top-level case
  classes (the 2.13 scope rule rejects those cross-references);
- codec feeds take `Chunk`, not `Array`;
- `handler { (_: Request) => throw ... }` (a `Nothing` body) matches no
  overload — use `Handler.fromRequest`.

## Migration notes

- Scala 3 consumers: use the shared APIs directly, or the `Scala3*` facades
  for the documented entry points.
- Scala 2.13 consumers: use the `Scala2*` facades in `zio.http.compat` for
  registry/engine construction; every other protocol API is called as
  documented for Scala 3.
- If a future API is Scala 3-only, it will be listed under Omitted APIs above
  with its replacement; its absence on 2.13 is then a compile error pointing
  back to this page — never a silent behavior change.
