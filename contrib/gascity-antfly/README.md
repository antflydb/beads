# Antfly for Gas City

This schema-2 pack exposes `antfly context`, `antfly sync`, and `antfly watch`
commands. Its executable Beads provider uses the Antfly-enabled `bd` build for
both city and rig scopes. It implements Gas City's create/get/update/close,
reopen/delete/list/children/ready/metadata/dependency protocol, including
explicit ephemeral readiness. Claims for durable work use the native atomic
claim operation; run beads retain their ephemeral/no-history flags.

Build `//cmd/bd:bd` from this fork and put it on PATH, or set `ANTFLY_BD_BIN`.
Select the executable in `city.toml`:

```toml
[beads]
provider = "exec:/absolute/path/to/beads/contrib/gascity-antfly/bin/gc-beads-antfly"

[imports.antfly]
source = "/absolute/path/to/beads/contrib/gascity-antfly"
```

For embedded storage set `ANTFLY_LIBRARY_PATH` to a matching libantfly build.
For remote storage set `GC_ANTFLY_URL` and optionally `GC_ANTFLY_TABLE`,
`GC_ANTFLY_DATABASE`, `GC_ANTFLY_NAMESPACE`, and `GC_ANTFLY_TOKEN_ENV`. The last
value names an environment variable containing the bearer token. Nothing
persists the token. Set `GC_ANTFLY_WORKSPACE` to a stable shared name when
several hosts share one logical scope; otherwise the provider derives a
separate workspace from each scope's canonical root path. Existing Antfly
routing is retained, and an existing different backend is refused. Lifecycle `init <scope-dir> <prefix>`
honors Gas City's positional scope and prefix; `start` initializes nothing
because remote servers are externally managed and embedded handles are per call.
Lifecycle health calls also accept the SDK's `BEADS_DIR` or canonical city-root
projection. Data calls require `GC_STORE_ROOT`.

Configure knowledge from a city/rig directory, then supervise the foreground
consumer through the factory's normal process manager:

```sh
printf '{"revision":"factory-v1"}' | bd antfly knowledge configure
bd antfly knowledge watch
bd antfly context 'guidance for token refresh' --max-tokens 2048
```

The default knowledge pipeline needs no model: it publishes immutable source
evidence and promotes exact entities/events. Supply native enrichment/index
and memory-space configuration to enable model extraction and semantic
search. Explicit resolver reviews and memory corrections remain operator
choices. Run `scripts/antfly-eval.py` on a versioned JSONL dataset and record
actual review/test outcomes before changing guidance policy.

For a Go embedder, import `backend/antfly`, register it once, and use its
`Open` with `backend.OpenOptions`. Supply a separate `TokenCredential` and
optional HTTP client per city. The implementation carries those injections
through knowledge operations and branch opens without changing environment
variables. Executable-provider `Tx` calls remain sequential, as Gas City's
protocol specifies; use the Go workspace's `RunInTransaction` when atomic
multi-operation application callbacks are required.

Protocol and pack references:
[executable Beads providers](https://github.com/gastownhall/gascity/blob/main/docs/reference/exec-beads-provider.md),
[pack specification](https://github.com/gastownhall/gascity/blob/main/docs/reference/specs/pack-spec.md).
