<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Storage;

use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Event\EventEntry;
use DomainFlow\EventSourcing\Event\EventId;
use DomainFlow\EventSourcing\Event\EventMetadata;
use DomainFlow\EventSourcing\Event\EventPersistenceRecord;
use DomainFlow\EventSourcing\Event\EventTypeRegistry;
use DomainFlow\EventSourcing\Event\EventVersion;
use DomainFlow\EventSourcing\Event\OccurredOn;
use DomainFlow\EventSourcing\Interface\DomainEventInterface;
use DomainFlow\EventSourcing\Interface\EventEntryFactoryInterface;
use DomainFlow\EventSourcing\Interface\EventFactoryInterface;
use DomainFlow\EventSourcingMongoDB\Support\AssertsMongoDocument;
use DomainFlow\Uuid\UuidV6;
use Random\RandomException;
use RuntimeException;

/**
 * Stores the event payload as a native BSON subdocument rather than Core's
 * DefaultEventEntryFactory JSON-string encoding — see docs/ARCHITECTURE.md
 * ("Payload encoding").
 */
final class MongoDbEventEntryFactory implements EventEntryFactoryInterface
{
    use AssertsMongoDocument;

    /**
     * @param EventFactoryInterface|null $eventFactory Used when rebuilding a
     *        stored event. Held here rather than in EventEntry's statics, so
     *        two stores in one service cannot disarm each other.
     * @param EventTypeRegistry|null $eventTypes Maps the class to the name it
     *        is stored under and back. This adapter has its own entry
     *        factory, so Core's resolution is not in the path and this one
     *        needs its own. Without a registry, an event is stored under its
     *        own class name exactly as before.
     */
    public function __construct(
        private readonly ?EventFactoryInterface $eventFactory = null,
        private readonly ?EventTypeRegistry $eventTypes = null
    ) {
    }

    /**
     * @throws RandomException
     */
    public function createFromDomainEvent(
        DomainEventInterface $event
    ): EventPersistenceRecord {
        $payload = $event->toArray();
        $payload[EventEntry::SCHEMA_VERSION_KEY] = EventEntry::declaredSchemaVersion($event::class);

        // Preserve the event's existing identity in both the document column
        // and the payload so consumers can deduplicate reliably.
        $eventId = isset($payload['eventId']) && is_string($payload['eventId']) && UuidV6::isValid($payload['eventId'])
            ? $payload['eventId']
            : (string) UuidV6::generate();

        $base = [
            'event_id' => $eventId,
            'aggregate_id' => (string) $event->getAggregateId(),
            'event_class' => $this->eventTypes?->nameFor($event::class) ?? $event::class,
            'version' => $event->getVersion()->toInt(),
            'occurred_on' => $event->getOccurredOn()->format(EventEntry::DATE_FORMAT),
            'payload' => $payload,
            // A native subdocument, like the payload beside it, rather than an
            // encoded string — the reason this adapter has its own factory at
            // all.
            'metadata' => $event->getMetadata()->toArray(),
        ];

        if (method_exists($event, 'getDatabaseFields')) {
            // Duck-typed hook, so the return type is whatever the event gives
            // back. Reported here rather than let into array_merge(), where a
            // non-document is a TypeError naming this factory instead of the
            // event that got it wrong.
            $base = array_merge($base, $this->assertDocument(
                $event->getDatabaseFields(),
                sprintf('%s::getDatabaseFields() must return a document keyed by column name.', $event::class)
            ));
        }

        return EventPersistenceRecord::fromArray($base);
    }

    public function recordToDomainEvent(
        EventPersistenceRecord $record
    ): DomainEventInterface {
        $row = $record->toArray();

        $eventClass = $row['event_class'] ?? '';
        $aggregateId = $row['aggregate_id'] ?? '';
        $eventId = $row['event_id'] ?? '';
        $occurredOn = $row['occurred_on'] ?? '';
        $version = $row['version'] ?? 0;

        if (!is_string($eventClass)) {
            throw new RuntimeException('Event class field is not a string.');
        }

        // Resolved before the EventEntry is built: the upcaster check,
        // migratePayload() and getFactory() all key off the class.
        $eventClass = $this->eventTypes !== null
            ? $this->eventTypes->classFor($eventClass)
            : $eventClass;

        if (!class_exists($eventClass)) {
            throw new RuntimeException("Event class {$eventClass} not found.");
        }

        return (new EventEntry(
            eventClass: $eventClass,
            aggregateId: EntityIdentifier::fromString(is_string($aggregateId) ? $aggregateId : ''),
            eventId: EventId::fromString(is_string($eventId) ? $eventId : (string) UuidV6::generate()),
            occurredOn: OccurredOn::fromString(is_string($occurredOn) ? $occurredOn : (string) OccurredOn::now()),
            version: EventVersion::fromInt(is_numeric($version) ? (int) $version : 0),
            payload: $this->assertDocument($row['payload'] ?? [], "Payload for event {$eventClass} is not a document."),
            factory: $this->eventFactory,
            metadata: EventMetadata::fromArray($this->toDocument($row['metadata'] ?? null)),
        ))->toDomainEvent();
    }
}
