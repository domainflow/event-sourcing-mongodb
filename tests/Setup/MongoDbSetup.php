<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Setup;

use DomainFlow\EventSourcing\Interface\EventStorageInterface;
use DomainFlow\EventSourcing\Interface\ProcessManagerStorageInterface;
use DomainFlow\EventSourcing\Interface\SnapshotHistoryStorageInterface;
use DomainFlow\EventSourcing\Interface\SnapshotStorageInterface;
use DomainFlow\EventSourcingMongoDB\ProcessManager\MongoDbProcessManagerStorage;
use DomainFlow\EventSourcingMongoDB\Snapshot\MongoDbSnapshotHistoryStorage;
use DomainFlow\EventSourcingMongoDB\Snapshot\MongoDbSnapshotStorage;
use DomainFlow\EventSourcingMongoDB\Storage\MongoDbEventStorage;

trait MongoDbSetup
{
    use MongoDbHelper;

    protected MongoDbEventStorage $eventStorage;
    protected MongoDbSnapshotStorage $snapshotStorage;
    protected MongoDbSnapshotHistoryStorage $snapshotHistoryStorage;
    protected MongoDbProcessManagerStorage $processManagerStorage;

    public function setUp(): void
    {
        parent::setUp();
        $this->dropMongoDatabase();
    }

    public function tearDown(): void
    {
        $this->dropMongoDatabase();
    }

    protected function getStorage(): EventStorageInterface
    {
        return new MongoDbEventStorage(
            $this->getMongoDatabase(),
            allowNonAtomicBatches: $this->allowNonAtomicBatches()
        );
    }

    protected function getSnapshotStorage(): SnapshotStorageInterface
    {
        return new MongoDbSnapshotStorage($this->getMongoDatabase());
    }

    protected function getSnapshotHistoryStorage(): SnapshotHistoryStorageInterface
    {
        return new MongoDbSnapshotHistoryStorage($this->getMongoDatabase());
    }

    protected function getProcessManagerStorage(): ProcessManagerStorageInterface
    {
        return new MongoDbProcessManagerStorage($this->getMongoDatabase());
    }
}
