<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Schema;

use DomainFlow\EventSourcing\Interface\SchemaManagerInterface;
use DomainFlow\EventSourcingMongoDB\Outbox\MongoDbOutboxStorage;
use DomainFlow\EventSourcingMongoDB\ProcessManager\MongoDbProcessManagerStorage;
use DomainFlow\EventSourcingMongoDB\Snapshot\MongoDbSnapshotHistoryStorage;
use DomainFlow\EventSourcingMongoDB\Snapshot\MongoDbSnapshotStorage;
use DomainFlow\EventSourcingMongoDB\Storage\MongoDbEventStorage;
use DomainFlow\EventSourcingMongoDB\Support\EnsuresMongoIndexes;
use MongoDB\Database;

/**
 * Creates the indexes this adapter's write paths rest on, as a call rather
 * than as a side effect of the first write.
 *
 * MongoDB has no DDL for collections worth speaking of — a collection appears
 * when something is written to it — so "the schema" here is the indexes. That
 * is not a smaller thing than it sounds: the unique indexes are what makes a
 * concurrent append detectable at all.
 */
final readonly class MongoDbSchemaManager implements SchemaManagerInterface
{
    use EnsuresMongoIndexes;

    /**
     * Every collection this adapter writes to.
     *
     * `counters` holds the global-position sequence rather than events, and it
     * is listed here because dropping the events without it leaves a counter
     * that keeps counting — a fresh store whose first event claims position
     * nine thousand.
     *
     * @var list<string>
     */
    private const array COLLECTIONS = [
        MongoDbEventStorage::DEFAULT_COLLECTION_NAME,
        MongoDbEventStorage::COUNTERS_COLLECTION_NAME,
        MongoDbSnapshotStorage::DEFAULT_COLLECTION_NAME,
        MongoDbSnapshotHistoryStorage::DEFAULT_COLLECTION_NAME,
        MongoDbProcessManagerStorage::DEFAULT_COLLECTION_NAME,
        MongoDbOutboxStorage::DEFAULT_COLLECTION_NAME,
    ];

    public function __construct(
        private Database $database
    ) {
    }

    public function ensureSchema(): void
    {
        foreach ($this->indexes() as [$collection, $key, $options]) {
            $this->ensureIndexExists($this->database->selectCollection($collection), $key, $options);
        }
    }

    public function dropSchema(): void
    {
        foreach (self::COLLECTIONS as $collection) {
            $this->database->dropCollection($collection);
        }
    }

    /**
     * @return list<string>
     */
    public function describeSchema(): array
    {
        return array_map(
            static fn (array $index): string => sprintf(
                'CREATE INDEX %s ON %s (%s)',
                is_string($index[2]['name'] ?? null) ? $index[2]['name'] : 'unnamed',
                $index[0],
                implode(', ', array_keys($index[1]))
            ),
            $this->indexes()
        );
    }

    /**
     * @return list<array{0: string, 1: array<string, int>, 2: array<string, mixed>}>
     */
    private function indexes(): array
    {
        return [
            [
                MongoDbEventStorage::DEFAULT_COLLECTION_NAME,
                ['aggregate_id' => 1, 'version' => 1],
                ['unique' => true, 'name' => 'uq_aggregate_version'],
            ],
            [
                MongoDbEventStorage::DEFAULT_COLLECTION_NAME,
                ['global_position' => 1],
                ['unique' => true, 'name' => 'uq_global_position'],
            ],
            [
                MongoDbSnapshotHistoryStorage::DEFAULT_COLLECTION_NAME,
                ['aggregate_id' => 1, 'version' => 1],
                ['unique' => true, 'name' => 'uq_aggregate_version'],
            ],
            [
                MongoDbProcessManagerStorage::DEFAULT_COLLECTION_NAME,
                ['process_id' => 1],
                ['unique' => true, 'name' => 'uq_process_id'],
            ],
            [
                MongoDbProcessManagerStorage::DEFAULT_COLLECTION_NAME,
                ['timeout' => 1],
                ['name' => 'idx_timeout'],
            ],
        ];
    }
}
