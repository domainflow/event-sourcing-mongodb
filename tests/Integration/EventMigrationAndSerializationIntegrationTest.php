<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Integration;

use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Event\EventEntry;
use DomainFlow\EventSourcingCore\Provider\Integration\EventMigrationAndSerializationIntegrationTestCase;
use DomainFlow\EventSourcingCore\Provider\Integration\MigratableDummyEvent;
use DomainFlow\EventSourcingMongoDB\Storage\MongoDbEventStorage;
use DomainFlow\EventSourcingMongoDB\Tests\Setup\MongoDbSetup;
use PHPUnit\Framework\Attributes\CoversNothing;

/**
 * Bypasses MongoDbEventStorage entirely (a raw insertOne, not storeEvents()) to simulate
 * pre-existing legacy-shaped data, mirroring the MySQL adapter's raw INSERT fixture. The
 * payload is inserted as a native BSON document (not a JSON string), matching this
 * adapter's default MongoDbEventEntryFactory. Only exercises retrieveEvents() on a single
 * aggregate, so no other collections/indexes need seeding.
 */
#[CoversNothing]
final class EventMigrationAndSerializationIntegrationTest extends EventMigrationAndSerializationIntegrationTestCase
{
    use MongoDbSetup;

    /**
     * @param array<string, mixed> $payload
     */
    protected function insertEvent(
        string $eventId,
        EntityIdentifier $aggregateId,
        string $eventClass,
        int $version,
        string $occurredOn,
        array $payload
    ): void {
        $this->getMongoDatabase()->selectCollection(MongoDbEventStorage::DEFAULT_COLLECTION_NAME)->insertOne([
            'event_id' => $eventId,
            'aggregate_id' => (string) $aggregateId,
            'event_class' => $eventClass,
            'version' => $version,
            'occurred_on' => $occurredOn,
            'payload' => $payload,
        ]);
    }

    protected function insertLegacyEvent(
        EntityIdentifier $aggregateId,
        string $eventId,
        string $occurredOn
    ): void {
        $this->insertEvent(
            $eventId,
            $aggregateId,
            MigratableDummyEvent::class,
            1,
            $occurredOn,
            [
                'aggregateId' => (string) $aggregateId,
                'eventId' => $eventId,
                'occurredOn' => $occurredOn,
                'version' => 1,
                'delta' => 3,
            ]
        );
    }

    protected function insertNewEventData(
        EntityIdentifier $aggregateId,
        string $eventId,
        string $occurredOn,
        string $description
    ): void {
        $this->insertEvent(
            $eventId,
            $aggregateId,
            MigratableDummyEvent::class,
            2,
            $occurredOn,
            [
                'aggregateId' => (string) $aggregateId,
                'eventId' => $eventId,
                'occurredOn' => $occurredOn,
                'version' => 2,
                'delta' => 7,
                'description' => $description,
                EventEntry::SCHEMA_VERSION_KEY => 2,
            ]
        );
    }
}
