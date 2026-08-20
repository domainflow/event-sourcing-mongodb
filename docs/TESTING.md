# Testing

The package uses PHPUnit contract tests from `domainflow/event-sourcing-core` and adapter-specific unit tests. Integration tests run against MongoDB so that indexes, transactions, BSON conversion, concurrency, snapshots, process-manager state, and outbox behaviour are exercised at their public boundaries.

## Local database

Start the single-node replica set before running integration tests:

```sh
docker compose up -d
```

The test setup reads `MONGO_URI` and `MONGO_DB`; defaults are `mongodb://127.0.0.1:27017` and `event_sourcing_test`.

## Quality commands

```sh
composer test              # unit tests
composer test-integration  # integration tests
composer test-all          # complete PHPUnit suite
composer test:coverage     # complete suite and 100% project-source line coverage
composer phpstan           # static analysis
composer lint              # coding-style check
composer format            # apply coding style
composer quality           # all mandatory checks, including Composer audit
```

Every normal test command enables coverage explicitly. The coverage assertion rejects uncovered project code; vendor code is excluded by the PHPUnit configuration.

## Test design

Tests observe public storage and factory interfaces. MongoDB is used directly only for setup, schema inspection, and fixtures that represent pre-existing documents. Unit tests use doubles only where a driver failure or a server capability cannot be reproduced economically. Tests cover both replica-set transactions and the explicitly opt-in compensating path for standalone MongoDB.
