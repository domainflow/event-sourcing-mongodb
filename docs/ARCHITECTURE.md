# Architecture

`domainflow/event-sourcing-mongodb` is a thin adapter for `domainflow/event-sourcing-core`. It contains no aggregate modelling or domain rules. Its responsibility is translating Core value objects and interfaces to MongoDB documents and translating driver failures back to Core exceptions.

## Public components

| Component | Responsibility |
| --- | --- |
| `MongoDbEventStorage` | Event streams, global reads, pagination, concurrency, transactions, and optional outbox enrollment |
| `MongoDbEventEntryFactory` | BSON document encoding and event reconstruction |
| `MongoDbSnapshotStorage` | Latest snapshot per aggregate |
| `MongoDbSnapshotHistoryStorage` | Append-only snapshot history |
| `MongoDbProcessManagerStorage` | Versioned process-manager state |
| `MongoDbOutboxStorage` | Transactional event delivery queue and dead-letter state |
| `MongoDbSchemaManager` | Explicit creation, inspection, and removal of required collections and indexes |

## Event documents

Each event is one document in the `events` collection. The document contains `event_id`, `aggregate_id`, `event_class`, `version`, `occurred_on`, `global_position`, `payload`, and `metadata`. Payload and metadata are native BSON documents and are converted to string-keyed PHP arrays through the collection type map.

The payload includes Core's `_schemaVersion` marker. This is important for event upcasting: a document carrying the current schema must not be migrated again, while a document without the marker is treated as legacy data. The adapter's factory writes the marker and its migration integration fixture represents both forms.

Event classes may be stored under a logical name through `EventTypeRegistry`. Driver-specific classes and exceptions never cross the Core storage interfaces.

## Ordering and concurrency

Events in one aggregate are read by version. Global reads are ordered by the monotonic `global_position` assigned by a counter. A unique compound index on `(aggregate_id, version)` detects concurrent appends. Duplicate-key errors are translated into Core's `ConcurrencyException`.

`storeEvents()` groups events by aggregate for contiguous global positions, then treats the complete call as one unit. On a replica set it uses one multi-document transaction. The transaction and collection writes use `w: majority` and journaling. On a standalone MongoDB, the adapter refuses writes by default because the interface promises all-or-nothing behaviour. `allowNonAtomicBatches: true` explicitly enables ordered insertion with best-effort compensation after a failure.

## Snapshots and process managers

The latest snapshot collection uses one document per aggregate and replaces that document on write. Snapshot history uses one document per `(aggregate_id, version)` with a unique compound index. State is stored as native BSON and validated when read.

Process-manager state is one document per process. Writes use the version loaded by the caller as an optimistic concurrency check. Timeouts are stored as UTC strings with microsecond precision and reconstructed in UTC.

## Transactional outbox

When configured, `MongoDbEventStorage` enrolls outbox entries in the same MongoDB session as the event transaction. A relay claims entries with a lease based on the server clock, delivers them at least once, and records permanently rejected entries in the same collection using `abandoned_at`. Consumers must be idempotent and must not assume delivery order across aggregates.

## Deployment

Run `MongoDbSchemaManager::ensureSchema()` as a deployment step. Application users may then operate without schema-creation privileges when the required indexes already exist; a missing required index still fails loudly. Production event writes should use a replica set. A single-node replica set is sufficient for development and tests.
