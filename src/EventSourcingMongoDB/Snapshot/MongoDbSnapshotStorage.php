<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Snapshot;

use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Event\EventVersion;
use DomainFlow\EventSourcing\Event\OccurredOn;
use DomainFlow\EventSourcing\Interface\EntityIdentifierInterface;
use DomainFlow\EventSourcing\Interface\SnapshotInterface;
use DomainFlow\EventSourcing\Interface\SnapshotStorageInterface;
use DomainFlow\EventSourcing\Snapshot\GenericSnapshot;
use DomainFlow\EventSourcingMongoDB\Support\AssertsMongoDocument;
use MongoDB\Collection;
use MongoDB\Database;

/**
 * MongoDB-backed SnapshotStorageInterface implementation: one document per
 * aggregate, keyed on aggregate_id, holding the latest snapshot only.
 *
 * storeSnapshot() uses replaceOne(..., upsert: true) — the direct analogue of
 * the SQL adapter's REPLACE INTO. No optimistic version guard on the filter:
 * snapshots are a read-side cache Core rebuilds from events on mismatch, not
 * the concurrency source of truth (see docs/ARCHITECTURE.md).
 */
final class MongoDbSnapshotStorage implements SnapshotStorageInterface
{
    use AssertsMongoDocument;

    public const string DEFAULT_COLLECTION_NAME = 'snapshots';

    private readonly Collection $collection;

    public function __construct(
        Database $database,
        string $collectionName = self::DEFAULT_COLLECTION_NAME
    ) {
        $this->collection = $database->selectCollection($collectionName, [
            'typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array'],
        ]);
    }

    public function storeSnapshot(
        SnapshotInterface $snapshot
    ): void {
        $this->collection->replaceOne(
            ['aggregate_id' => (string) $snapshot->getAggregateId()],
            [
                'aggregate_id' => (string) $snapshot->getAggregateId(),
                'version' => $snapshot->getVersion()->toInt(),
                'occurred_on' => $snapshot->getOccurredOn()->format('Y-m-d H:i:s.u'),
                'state' => $snapshot->getState(),
                'snapshot_class' => get_class($snapshot),
            ],
            ['upsert' => true]
        );
    }

    public function deleteSnapshot(
        EntityIdentifierInterface $aggregateId
    ): void {
        $this->collection->deleteOne(['aggregate_id' => (string) $aggregateId]);
    }

    public function retrieveSnapshot(
        EntityIdentifierInterface $aggregateId
    ): ?SnapshotInterface {
        $raw = $this->collection->findOne(['aggregate_id' => (string) $aggregateId]);

        if ($raw === null) {
            return null;
        }

        $document = $this->assertDocument(
            $raw,
            sprintf('Failed to decode snapshot state for aggregate "%s": document is malformed.', $aggregateId)
        );

        $state = $this->assertDocument(
            $document['state'] ?? null,
            sprintf('Failed to decode snapshot state for aggregate "%s": state is not a document.', $aggregateId)
        );

        $version = isset($document['version']) && is_numeric($document['version']) ? (int) $document['version'] : 0;
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
