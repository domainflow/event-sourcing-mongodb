<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Integration\Repository;

use DomainFlow\EventSourcingCore\Provider\Integration\CounterProjectionRepositoryInterface;
use MongoDB\Collection;
use MongoDB\Database;

/**
 * Test-only fixture, mirroring the MySQL adapter's MySqlCounterProjectionRepository and
 * the Redis adapter's RedisCounterProjectionRepository: a single dedicated collection
 * (one document per aggregate) instead of a dedicated SQL table or Hash.
 */
final readonly class MongoDbCounterProjectionRepository implements CounterProjectionRepositoryInterface
{
    private const string COLLECTION_NAME = 'counter_projection';

    private Collection $collection;

    public function __construct(
        Database $database
    ) {
        $this->collection = $database->selectCollection(self::COLLECTION_NAME, [
            'typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array'],
        ]);
    }

    public function getCounter(
        string $aggregateId
    ): ?int {
        $document = $this->collection->findOne(['aggregate_id' => $aggregateId]);

        if (!is_array($document) || !isset($document['counter']) || !is_numeric($document['counter'])) {
            return null;
        }

        return (int) $document['counter'];
    }

    public function saveCounter(
        string $aggregateId,
        int $counter
    ): void {
        $this->collection->replaceOne(
            ['aggregate_id' => $aggregateId],
            ['aggregate_id' => $aggregateId, 'counter' => $counter],
            ['upsert' => true]
        );
    }

    public function reset(): void
    {
        $this->collection->deleteMany([]);
    }

    /**
     * @return array<string, mixed>[]
     */
    public function all(): array
    {
        $rows = [];

        foreach ($this->collection->find() as $document) {
            if (!is_array($document)) {
                continue;
            }

            $rows[] = [
                'aggregate_id' => $document['aggregate_id'] ?? null,
                'counter' => $document['counter'] ?? null,
            ];
        }

        return $rows;
    }
}
