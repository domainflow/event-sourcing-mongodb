<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Unit\Snapshot;

use DateTimeImmutable;
use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Event\EventVersion;
use DomainFlow\EventSourcing\Event\OccurredOn;
use DomainFlow\EventSourcing\Interface\SnapshotStorageInterface;
use DomainFlow\EventSourcing\Snapshot\GenericSnapshot;
use DomainFlow\EventSourcingCore\Provider\Unit\AbstractSnapshotStorageTestCase;
use DomainFlow\EventSourcingMongoDB\Snapshot\MongoDbSnapshotStorage;
use DomainFlow\EventSourcingMongoDB\Tests\Setup\MongoDbHelper;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(MongoDbSnapshotStorage::class)]
final class MongoDbSnapshotStorageTest extends AbstractSnapshotStorageTestCase
{
    use MongoDbHelper;

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropMongoDatabase();

        $collection = $this->getMongoDatabase()->selectCollection(MongoDbSnapshotStorage::DEFAULT_COLLECTION_NAME);

        $collection->insertOne([
            'aggregate_id' => 'json-corrupt-id',
            'version' => 1,
            'occurred_on' => '2024-01-01 00:00:00.000000',
            'state' => 'INVALID_JSON',
            'snapshot_class' => GenericSnapshot::class,
        ]);

        $collection->insertOne([
            'aggregate_id' => 'bad-occurred-id',
            'version' => 1,
            'occurred_on' => '2024-01-01 00:00:00.000000',
            'state' => ['x' => 'y'],
            'snapshot_class' => GenericSnapshot::class,
        ]);
    }

    protected function tearDown(): void
    {
        $this->dropMongoDatabase();
    }

    protected function getSnapshotStorage(): SnapshotStorageInterface
    {
        return new MongoDbSnapshotStorage($this->getMongoDatabase());
    }

    public function test_storeSnapshotReplacesExistingSnapshotForSameAggregate(): void
    {
        $storage = $this->getSnapshotStorage();
        $aggregateId = EntityIdentifier::fromString('ReplaceAggregate');

        $storage->storeSnapshot(new GenericSnapshot(
            $aggregateId,
            EventVersion::fromInt(1),
            ['step' => 'first'],
            OccurredOn::now()
        ));
        $storage->storeSnapshot(new GenericSnapshot(
            $aggregateId,
            EventVersion::fromInt(2),
            ['step' => 'second'],
            OccurredOn::now()
        ));

        $retrieved = $storage->retrieveSnapshot($aggregateId);

        $this->assertNotNull($retrieved);
        $this->assertSame(2, $retrieved->getVersion()->toInt());
        $this->assertSame(['step' => 'second'], $retrieved->getState());
    }

    public function test_retrieveSnapshotFallsBackToNowWhenOccurredOnFieldIsMissing(): void
    {
        $this->getMongoDatabase()->selectCollection(MongoDbSnapshotStorage::DEFAULT_COLLECTION_NAME)->insertOne([
            'aggregate_id' => 'MissingOccurredOnAggregate',
            'version' => 1,
            'state' => ['x' => 'y'],
            'snapshot_class' => GenericSnapshot::class,
        ]);

        $snapshot = $this->getSnapshotStorage()->retrieveSnapshot(EntityIdentifier::fromString('MissingOccurredOnAggregate'));

        $this->assertNotNull($snapshot);
        $this->assertInstanceOf(DateTimeImmutable::class, $snapshot->getOccurredOn());
    }
}
