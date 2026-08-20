<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Integration;

use DomainFlow\EventSourcing\Interface\EventEntryFactoryInterface;
use DomainFlow\EventSourcingCore\Provider\Integration\CustomFieldsStorageTestCase;
use DomainFlow\EventSourcingMongoDB\Storage\MongoDbEventStorage;
use DomainFlow\EventSourcingMongoDB\Tests\Setup\MongoDbSetup;
use PHPUnit\Framework\Attributes\CoversNothing;

/**
 * Unlike the MySQL adapter's equivalent, this needs no schema setup: MongoDbEventStorage
 * writes whatever fields the factory's EventPersistenceRecord produces, schemaless.
 */
#[CoversNothing]
final class CustomFieldsStorageTest extends CustomFieldsStorageTestCase
{
    use MongoDbSetup;

    public function getStorageWithFactory(
        EventEntryFactoryInterface $factory
    ): MongoDbEventStorage {
        return new MongoDbEventStorage(
            $this->getMongoDatabase(),
            $factory,
            allowNonAtomicBatches: $this->allowNonAtomicBatches()
        );
    }
}
