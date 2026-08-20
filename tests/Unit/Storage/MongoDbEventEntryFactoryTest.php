<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Unit\Storage;

use DateTimeImmutable;
use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Event\EventEntry;
use DomainFlow\EventSourcing\Event\EventPersistenceRecord;
use DomainFlow\EventSourcing\Event\EventTypeRegistry;
use DomainFlow\EventSourcing\Event\EventVersion;
use DomainFlow\EventSourcing\Interface\DomainEventInterface;
use DomainFlow\EventSourcing\Interface\EntityIdentifierInterface;
use DomainFlow\EventSourcing\Trait\HasEventMetadata;
use DomainFlow\EventSourcingMongoDB\Storage\MongoDbEventEntryFactory;
use DomainFlow\Uuid\UuidV6;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;

#[CoversClass(MongoDbEventEntryFactory::class)]
class MongoDbEventEntryFactoryTest extends TestCase
{
    public function test_createFromDomainEventMergesDatabaseFields(): void
    {
        $factory = new MongoDbEventEntryFactory();
        $aggregateId = EntityIdentifier::fromString('DatabaseFieldsAggregate');

        $record = $factory->createFromDomainEvent(new EventWithDatabaseFields($aggregateId));

        $this->assertSame('secondary-index-value', $record->toArray()['custom_field']);
    }

    public function test_createFromDomainEventStoresTheEventSchemaVersion(): void
    {
        $record = (new MongoDbEventEntryFactory())
            ->createFromDomainEvent(new NamedMongoEvent('SchemaAggregate'))
            ->toArray();

        $this->assertIsArray($record['payload']);
        $this->assertSame(
            EventEntry::declaredSchemaVersion(NamedMongoEvent::class),
            $record['payload'][EventEntry::SCHEMA_VERSION_KEY]
        );
    }

    public function test_it_writes_the_logical_event_name_and_reads_it_back(): void
    {
        $registry = new EventTypeRegistry();
        $registry->register('mongo.fields', NamedMongoEvent::class);

        $factory = new MongoDbEventEntryFactory(null, $registry);
        $record = $factory->createFromDomainEvent(new NamedMongoEvent('NamedAggregate'));

        $this->assertSame('mongo.fields', $record->toArray()['event_class']);
        $this->assertInstanceOf(NamedMongoEvent::class, $factory->recordToDomainEvent($record));
    }

    public function test_a_document_written_before_the_registry_still_reads(): void
    {
        $written = (new MongoDbEventEntryFactory())->createFromDomainEvent(new NamedMongoEvent('LegacyAggregate'));

        $this->assertSame(NamedMongoEvent::class, $written->toArray()['event_class']);

        $registry = new EventTypeRegistry();
        $registry->register('mongo.fields', NamedMongoEvent::class);

        $this->assertInstanceOf(
            NamedMongoEvent::class,
            (new MongoDbEventEntryFactory(null, $registry))->recordToDomainEvent($written)
        );
    }

    public function test_the_stored_event_id_is_the_one_the_event_carries(): void
    {
        $eventId = (string) UuidV6::generate();

        $record = (new MongoDbEventEntryFactory())
            ->createFromDomainEvent(new IdentifiedMongoEvent('IdentityAggregate', $eventId))
            ->toArray();

        $payload = $record['payload'];

        $this->assertIsArray($payload);
        $this->assertSame(
            $eventId,
            (string) $record['event_id'],
            'Consumer-side deduplication rests on the column, and the payload has to name the same event.'
        );
        $this->assertSame($eventId, $payload['eventId']);
    }

    public function test_an_event_without_a_usable_id_still_gets_one(): void
    {
        $record = (new MongoDbEventEntryFactory())
            ->createFromDomainEvent(new NamedMongoEvent('NoIdAggregate'))
            ->toArray();

        $this->assertNotSame('', (string) $record['event_id']);
    }

    public function test_recordToDomainEventThrowsWhenEventClassIsNotString(): void
    {
        $factory = new MongoDbEventEntryFactory();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Event class field is not a string.');

        $factory->recordToDomainEvent(EventPersistenceRecord::fromArray([
            'event_class' => 12345,
        ]));
    }

    public function test_recordToDomainEventThrowsWhenEventClassNotFound(): void
    {
        $factory = new MongoDbEventEntryFactory();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('not found');

        $factory->recordToDomainEvent(EventPersistenceRecord::fromArray([
            'event_class' => 'DomainFlow\\Nonexistent\\EventClass',
        ]));
    }

    public function test_recordToDomainEventThrowsWhenPayloadIsNotADocument(): void
    {
        $factory = new MongoDbEventEntryFactory();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('is not a document');

        $factory->recordToDomainEvent(EventPersistenceRecord::fromArray([
            'event_class' => EventWithDatabaseFields::class,
            'aggregate_id' => 'PayloadValidationAggregate',
            'event_id' => (string) UuidV6::generate(),
            'payload' => 'not-a-document',
        ]));
    }

    public function test_recordToDomainEventThrowsWhenPayloadHasNonStringKeys(): void
    {
        $factory = new MongoDbEventEntryFactory();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('is not a document');

        $factory->recordToDomainEvent(EventPersistenceRecord::fromArray([
            'event_class' => EventWithDatabaseFields::class,
            'aggregate_id' => 'PayloadValidationAggregate',
            'event_id' => (string) UuidV6::generate(),
            'payload' => ['first-list-item', 'second-list-item'],
        ]));
    }
}

final class EventWithDatabaseFields implements DomainEventInterface
{
    use HasEventMetadata;

    private EventVersion $version;
    private DateTimeImmutable $occurredOn;

    public function __construct(
        private readonly EntityIdentifierInterface $aggregateId
    ) {
        $this->version = EventVersion::new();
        $this->occurredOn = new DateTimeImmutable();
    }

    public function getAggregateId(): EntityIdentifierInterface
    {
        return $this->aggregateId;
    }

    public function getOccurredOn(): DateTimeImmutable
    {
        return $this->occurredOn;
    }

    public function getVersion(): EventVersion
    {
        return $this->version;
    }

    public function toArray(): array
    {
        return [];
    }

    public function setVersion(
        EventVersion $version
    ): void {
        $this->version = $version;
    }

    /**
     * @return array<string, mixed>
     */
    public function getDatabaseFields(): array
    {
        return ['custom_field' => 'secondary-index-value'];
    }
}

/**
 * A payload that matches its own constructor, so it survives the reflection
 * round trip the registry cases are actually about.
 */
final class NamedMongoEvent implements DomainEventInterface
{
    use HasEventMetadata;

    private EventVersion $version;
    private DateTimeImmutable $occurredOn;

    public function __construct(
        private readonly string $aggregateId
    ) {
        $this->version = EventVersion::new();
        $this->occurredOn = new DateTimeImmutable();
    }

    public function getAggregateId(): EntityIdentifierInterface
    {
        return EntityIdentifier::fromString($this->aggregateId);
    }

    public function getOccurredOn(): DateTimeImmutable
    {
        return $this->occurredOn;
    }

    public function getVersion(): EventVersion
    {
        return $this->version;
    }

    public function setVersion(
        EventVersion $version
    ): void {
        $this->version = $version;
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return ['aggregateId' => $this->aggregateId];
    }
}

/**
 * Carries its own event id, which is what a real event does and what the
 * identity cases are about.
 */
final class IdentifiedMongoEvent implements DomainEventInterface
{
    use HasEventMetadata;

    private EventVersion $version;
    private DateTimeImmutable $occurredOn;

    public function __construct(
        private readonly string $aggregateId,
        private readonly string $eventId
    ) {
        $this->version = EventVersion::new();
        $this->occurredOn = new DateTimeImmutable();
    }

    public function getAggregateId(): EntityIdentifierInterface
    {
        return EntityIdentifier::fromString($this->aggregateId);
    }

    public function getOccurredOn(): DateTimeImmutable
    {
        return $this->occurredOn;
    }

    public function getVersion(): EventVersion
    {
        return $this->version;
    }

    public function setVersion(
        EventVersion $version
    ): void {
        $this->version = $version;
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return ['aggregateId' => $this->aggregateId, 'eventId' => $this->eventId];
    }
}
