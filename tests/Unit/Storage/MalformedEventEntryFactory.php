<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Unit\Storage;

use DomainFlow\EventSourcing\Event\EventPersistenceRecord;
use DomainFlow\EventSourcing\Interface\DomainEventInterface;
use DomainFlow\EventSourcing\Interface\EventEntryFactoryInterface;
use DomainFlow\EventSourcingMongoDB\Storage\MongoDbEventEntryFactory;

/**
 * Produces documents MongoDB will refuse, so the write paths that exist for
 * failure can be reached deterministically.
 *
 * Three knobs, each standing in for a real failure mode:
 *
 * - `constantEventId` makes every document of a batch collide on a unique
 *   index other than (aggregate_id, version). That is the stand-in for the
 *   concurrent writer that takes a version between the fallback path's
 *   pre-check and its insert — the case compensation exists for, and the one
 *   a test cannot produce by timing alone.
 * - `unencodableFromCall` puts a value in the document that the BSON encoder
 *   rejects, so insertMany() fails with something that is not a write error.
 *   A custom serializer handing back an unsupported type does exactly this.
 * - `dropEventId` removes the event id, which is what compensation gets when
 *   it has nothing to identify the documents by.
 */
final class MalformedEventEntryFactory implements EventEntryFactoryInterface
{
    private int $calls = 0;

    private readonly MongoDbEventEntryFactory $delegate;

    public function __construct(
        private readonly ?string $constantEventId = null,
        private readonly ?int $unencodableFromCall = null,
        private readonly bool $dropEventId = false
    ) {
        $this->delegate = new MongoDbEventEntryFactory();
    }

    public function createFromDomainEvent(
        DomainEventInterface $event
    ): EventPersistenceRecord {
        $this->calls++;

        $values = $this->delegate->createFromDomainEvent($event)->getValues();

        if ($this->constantEventId !== null) {
            $values['event_id'] = $this->constantEventId;
        }

        if ($this->dropEventId) {
            unset($values['event_id']);
        }

        if ($this->unencodableFromCall !== null && $this->calls >= $this->unencodableFromCall) {
            // A stream resource has no BSON representation, so the driver
            // rejects the document while assembling the bulk write.
            $values['payload'] = ['handle' => fopen('php://memory', 'r')];
        }

        return EventPersistenceRecord::fromArray($values);
    }

    public function recordToDomainEvent(
        EventPersistenceRecord $record
    ): DomainEventInterface {
        return $this->delegate->recordToDomainEvent($record);
    }
}
