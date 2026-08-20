# DomainFlow EventSourcing MongoDB

[![Tests](https://github.com/domainflow/event-sourcing-mongodb/actions/workflows/tests.yml/badge.svg)](https://github.com/domainflow/event-sourcing-mongodb/actions/workflows/tests.yml)
![Packagist Version](https://img.shields.io/packagist/v/domainflow/event-sourcing-mongodb)
![PHP Version](https://img.shields.io/packagist/php-v/domainflow/event-sourcing-mongodb)
![License](https://img.shields.io/github/license/domainflow/event-sourcing-mongodb)
![PHPStan](https://img.shields.io/badge/PHPStan-Level%210-brightgreen.svg)

A MongoDB storage adapter for [`domainflow/event-sourcing-core`](https://github.com/domainflow/event-sourcing-core) 
— implements `EventStorageInterface`, `SnapshotStorageInterface`, `SnapshotHistoryStorageInterface`, and `ProcessManagerStorageInterface` against MongoDB. No domain logic of its own — no aggregate modeling, no business rules, just translation between Core's interfaces and MongoDB.

## Requirements

- PHP 8.4+
- `ext-mongodb` 2.x 
- `mongodb/mongodb` ^2.0 composer package
- A reachable MongoDB 7+ instance, running as a replica set (see below)


## Installation

```bash
composer require domainflow/event-sourcing-mongodb
```

## Production requirements

**Run MongoDB as a replica set.** Multi-document transactions have required one since MongoDB 4.0 and still do, and `storeEvents()` needs a transaction to append a whole call atomically — which is what `EventStorageInterface` promises: either every event in the call lands or none does, across every aggregate it touches.

On a standalone `mongod` there are no transactions, so the adapter **refuses to write**, throwing on the first `storeEvents()` call rather than quietly handing you something weaker than the interface says. A single-node replica set is enough to satisfy it, and is what `docker-compose.yml` runs.

If a standalone is genuinely what you have, opt out explicitly:

```php
$storage = new MongoDbEventStorage($database, allowNonAtomicBatches: true);
```

That re-enables the fallback: a version pre-check, then an ordered insert, then a compensating delete of whatever landed before a failure. It is best-effort, not atomic — there is a real window between the check and the insert, and a process that dies between a failed insert and its compensation leaves the partial batch behind for good. The flag exists so that trade is a decision someone made, not one they were never told about.

**Writes are durable by default and this is not configurable.** Both collections are opened with `w: majority, j: true`. Under MongoDB's default (`w: 1, j: false`) an insert is acknowledged once one node holds it in memory, so an event can be reported as stored and then lost in a failover — leaving an aggregate that believes it emitted history the store never kept. That trade is not one an event store gets to make.


## Development

```bash
docker compose up -d          # start a local MongoDB instance
composer install
composer quality              # lint + static analysis + full test suite (100% coverage required) + audit
```

## License

MIT — see [LICENSE](LICENSE).
