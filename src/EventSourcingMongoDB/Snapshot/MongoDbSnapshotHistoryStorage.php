<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Snapshot;

use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Event\EventVersion;
use DomainFlow\EventSourcing\Event\OccurredOn;
use DomainFlow\EventSourcing\Interface\EntityIdentifierInterface;
use DomainFlow\EventSourcing\Interface\SnapshotHistoryStorageInterface;
use DomainFlow\EventSourcing\Interface\SnapshotInterface;
use DomainFlow\EventSourcing\Snapshot\GenericSnapshot;
use DomainFlow\EventSourcingMongoDB\Support\AssertsMongoDocument;
use DomainFlow\EventSourcingMongoDB\Support\EnsuresMongoIndexes;
use MongoDB\Collection;
use MongoDB\Database;

/**
 * MongoDB-backed SnapshotHistoryStorageInterface implementation: one document
 * per (aggregate_id, version) pair in a single snapshot_history collection,
 * with a unique compound index mirroring the SQL adapter's composite primary
 * key.
 */
final class MongoDbSnapshotHistoryStorage implements SnapshotHistoryStorageInterface
{
    use AssertsMongoDocument;
    use EnsuresMongoIndexes;

    public const string DEFAULT_COLLECTION_NAME = 'snapshot_history';

    private readonly Collection $collection;

    private bool $indexesEnsured = false;

    public function __construct(
        Database $database,
        string $collectionName = self::DEFAULT_COLLECTION_NAME
    ) {
        $this->collection = $database->selectCollection($collectionName, [
            'typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array'],
        ]);
    }

    /**
     * Lazily, and tolerated when refused if the index is already there.
     *
     * This used to run in the constructor, which meant building the object
     * issued DDL and required a reachable server — so a service could not even
     * be constructed while its database was down, which is exactly when the
     * rest of it needs to behave sensibly. Every other storage in this adapter
     * had already settled that question the other way.
     */
    private function ensureIndexes(): void
    {
        if ($this->indexesEnsured) {
            return;
        }

        $this->ensureIndexExists(
            $this->collection,
            ['aggregate_id' => 1, 'version' => 1],
            ['unique' => true, 'name' => 'uq_aggregate_version']
        );

        $this->indexesEnsured = true;
    }

    public function persistVersioned(
        SnapshotInterface $snapshot
    ): void {
        $this->ensureIndexes();

        $this->collection->insertOne([
            'aggregate_id' => (string) $snapshot->getAggregateId(),
            'version' => $snapshot->getVersion()->toInt(),
            'occurred_on' => $snapshot->getOccurredOn()->format('Y-m-d H:i:s.u'),
            'state' => $snapshot->getState(),
        ]);
    }

    public function deleteSingle(
        EntityIdentifierInterface $aggregateId,
        int $version
    ): void {
        $this->collection->deleteOne([
            'aggregate_id' => (string) $aggregateId,
            'version' => $version,
        ]);
    }

    public function deleteAll(
        EntityIdentifierInterface $aggregateId
    ): void {
        $this->collection->deleteMany(['aggregate_id' => (string) $aggregateId]);
    }

    /**
     * @return array<SnapshotInterface>
     */
    public function retrieveAll(
        EntityIdentifierInterface $aggregateId
    ): array {
        $cursor = $this->collection->find(
            ['aggregate_id' => (string) $aggregateId],
            ['sort' => ['version' => 1]]
        );

        $snapshots = [];
        foreach ($cursor as $raw) {
            $snapshots[] = $this->hydrate($aggregateId, $raw);
        }

        return $snapshots;
    }

    private function hydrate(
        EntityIdentifierInterface $aggregateId,
        mixed $raw
    ): SnapshotInterface {
        $document = $this->assertDocument(
            $raw,
            sprintf('Failed to decode snapshot history state for aggregate "%s": document is malformed.', $aggregateId)
        );

        $version = isset($document['version']) && is_numeric($document['version']) ? (int) $document['version'] : 0;

        $state = $this->assertDocument(
            $document['state'] ?? null,
            sprintf(
                'Failed to decode snapshot history state for aggregate "%s" at version %d',
                $aggregateId,
                $version
            )
        );

        $occurredOn = isset($document['occurred_on']) && is_string($document['occurred_on'])
            ? $document['occurred_on']
            : 'now';

        return new GenericSnapshot(
            EntityIdentifier::fromString((string) $aggregateId),
            EventVersion::fromInt($version),
            $state,
            OccurredOn::fromString($occurredOn)
        );
    }
}
