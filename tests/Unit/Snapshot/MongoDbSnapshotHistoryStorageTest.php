<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Unit\Snapshot;

use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Interface\SnapshotHistoryStorageInterface;
use DomainFlow\EventSourcingCore\Provider\Unit\AbstractSnapshotHistoryStorageTestCase;
use DomainFlow\EventSourcingMongoDB\Snapshot\MongoDbSnapshotHistoryStorage;
use DomainFlow\EventSourcingMongoDB\Tests\Setup\MongoDbHelper;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(MongoDbSnapshotHistoryStorage::class)]
final class MongoDbSnapshotHistoryStorageTest extends AbstractSnapshotHistoryStorageTestCase
{
    use MongoDbHelper;

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropMongoDatabase();

        $collection = $this->getMongoDatabase()->selectCollection(MongoDbSnapshotHistoryStorage::DEFAULT_COLLECTION_NAME);

        $collection->insertOne([
            'aggregate_id' => 'corrupt-agg',
            'version' => 1,
            'occurred_on' => '2024-01-01 00:00:00.000000',
            'state' => 'not-a-document',
        ]);

        $collection->insertOne([
            'aggregate_id' => 'invalid-date-agg',
            'version' => 1,
            'occurred_on' => '2024-01-01 00:00:00.000000',
            'state' => ['foo' => 'bar'],
        ]);
    }

    protected function tearDown(): void
    {
        $this->dropMongoDatabase();
    }

    protected function getSnapshotHistoryStorage(): SnapshotHistoryStorageInterface
    {
        return new MongoDbSnapshotHistoryStorage($this->getMongoDatabase());
    }

    public function test_retrieveAllFallsBackToNowWhenOccurredOnFieldIsMissing(): void
    {
        $this->getMongoDatabase()->selectCollection(MongoDbSnapshotHistoryStorage::DEFAULT_COLLECTION_NAME)->insertOne([
            'aggregate_id' => 'missing-occurred-on-agg',
            'version' => 1,
            'state' => ['foo' => 'bar'],
        ]);

        $snapshots = $this->getSnapshotHistoryStorage()->retrieveAll(EntityIdentifier::fromString('missing-occurred-on-agg'));

        $this->assertCount(1, $snapshots);
    }
}
