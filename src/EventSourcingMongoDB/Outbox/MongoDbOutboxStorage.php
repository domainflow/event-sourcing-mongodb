<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Outbox;

use DomainFlow\EventSourcing\Clock\ClockInterface;
use DomainFlow\EventSourcing\Clock\SystemClock;
use DomainFlow\EventSourcing\Event\EventPersistenceRecord;
use DomainFlow\EventSourcing\Interface\DomainEventInterface;
use DomainFlow\EventSourcing\Interface\EventEntryFactoryInterface;
use DomainFlow\EventSourcing\Interface\OutboxStorageInterface;
use DomainFlow\EventSourcing\Outbox\OutboxEntry;
use DomainFlow\EventSourcingMongoDB\Storage\MongoDbEventEntryFactory;
use DomainFlow\EventSourcingMongoDB\Support\AssertsMongoDocument;
use DomainFlow\Uuid\UuidV6;
use MongoDB\BSON\ObjectId;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Driver\Session;
use MongoDB\Driver\WriteConcern;

/**
 * The outbox collection, on the same database as the events.
 *
 * Unlike MySQL, a MongoDB transaction is not ambient on the connection — it is
 * a session object that every participating write has to be handed. That is
 * why `MongoDbEventStorage` takes this concrete class rather than the
 * interface: it needs `enqueueInSession()` to enrol the entry in the
 * transaction it opened. The interface method exists for the relay, which has
 * no transaction to join.
 */
final class MongoDbOutboxStorage implements OutboxStorageInterface
{
    use AssertsMongoDocument;

    public const string DEFAULT_COLLECTION_NAME = 'outbox';

    private readonly Collection $collection;
    private readonly EventEntryFactoryInterface $entryFactory;

    /**
     * Two clocks are in play here, and which one does what is the whole point
     * of the server clock.
     *
     * **The lease is the server's**: `reserved_at` is stamped with `$$NOW` and
     * compared against `$$NOW` inside the claiming update, so every relay is
     * measured against one clock however many hosts they run on. It used to be
     * computed from `$clock` in the relay's own process, which meant a fleet
     * whose hosts disagreed about the time also disagreed about when a claim
     * had lapsed — the fast relay took entries the slow one was still
     * delivering, turning at-least-once into a duplicate every cycle with
     * nothing reported anywhere.
     *
     * **`$clock` is the relay's own**, and it now decides nothing: it stamps
     * `abandoned_at`, which records when *this* relay gave up and is never
     * compared against anything. Injectable so a test can pin that instant.
     *
     * @param Database $database The same database the events are written to.
     * @param int $leaseSeconds How long a claim is honoured before another
     *        relay may take the entry, so a relay dying between claiming and
     *        marking cannot strand its entries forever. Measured by the
     *        server, not by the caller.
     * @param ClockInterface $clock The relay's clock. Stamps `abandoned_at`
     *        only — see above.
     */
    public function __construct(
        Database $database,
        ?EventEntryFactoryInterface $entryFactory = null,
        private readonly int $leaseSeconds = 300,
        string $collectionName = self::DEFAULT_COLLECTION_NAME,
        private readonly ClockInterface $clock = new SystemClock()
    ) {
        $this->collection = $database->selectCollection($collectionName, [
            'typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array'],
            'writeConcern' => new WriteConcern(WriteConcern::MAJORITY, 0, true),
        ]);
        $this->entryFactory = $entryFactory ?? new MongoDbEventEntryFactory();
    }

    /**
     * @param array<DomainEventInterface> $events
     * @return void
     */
    public function enqueue(
        array $events
    ): void {
        $this->enqueueInSession($events, null);
    }

    /**
     * Enrols the entries in a transaction the caller already opened.
     *
     * With a session, the entry and the events commit together. Without one —
     * the standalone fallback path — they do not, and the outbox inherits
     * exactly the weaker guarantee that path already carries.
     *
     * @param array<DomainEventInterface> $events
     * @param Session|null $session
     * @return void
     */
    public function enqueueInSession(
        array $events,
        ?Session $session
    ): void {
        if ($events === []) {
            return;
        }

        $documents = array_map(
            fn (DomainEventInterface $event): array => $this->entryFactory->createFromDomainEvent($event)->getValues()
                + ['attempts' => 0, 'reserved_at' => null, 'reserved_by' => null, 'abandoned_at' => null],
            array_values($events)
        );

        $options = ['ordered' => true];

        if ($session !== null) {
            $options['session'] = $session;
        }

        $this->collection->insertMany($documents, $options);
    }

    /**
     * Claims entries by stamping them, then reading back what was stamped.
     *
     * Three steps rather than one, because updateMany() takes no limit: pick
     * candidates, stamp the ones still free, read back by token. A relay that
     * lost the race for a candidate simply gets fewer entries this pass, which
     * is correct — never the same entry as its competitor.
     *
     * Every instant involved is the server's. The candidate query and
     * the claiming update share one filter, so a claim that lapsed between the
     * two is not stamped by mistake.
     *
     * @param int $limit
     * @return list<OutboxEntry>
     */
    public function reserve(
        int $limit
    ): array {
        if ($limit <= 0) {
            return [];
        }

        $claimable = $this->claimableFilter();

        $candidates = $this->collection->find($claimable, ['sort' => ['_id' => 1], 'limit' => $limit, 'projection' => ['_id' => 1]])->toArray();

        if ($candidates === []) {
            return [];
        }

        $ids = array_map(
            static fn (array $document): mixed => $document['_id'] ?? null,
            $this->toDocuments($candidates)
        );
        $token = (string) UuidV6::generate();

        // An aggregation-expression update, so the claim is stamped with the
        // server's clock rather than the caller's. It has to be the
        // same clock the filter below compares against, or the lease boundary
        // is measured between two clocks and means nothing.
        $this->collection->updateMany(
            ['_id' => ['$in' => $ids]] + $claimable,
            [['$set' => ['reserved_at' => '$$NOW', 'reserved_by' => $token]]]
        );

        $entries = [];

        foreach ($this->toDocuments($this->collection->find(['reserved_by' => $token], ['sort' => ['_id' => 1]])->toArray()) as $document) {
            $entries[] = $this->toEntry($document);
        }

        return $entries;
    }

    public function markDelivered(
        OutboxEntry $entry
    ): void {
        $this->collection->deleteOne(['_id' => new ObjectId($entry->getId())]);
    }

    public function markFailed(
        OutboxEntry $entry
    ): void {
        $this->collection->updateOne(
            ['_id' => new ObjectId($entry->getId())],
            ['$inc' => ['attempts' => 1], '$set' => ['reserved_at' => null, 'reserved_by' => null]]
        );
    }

    /**
     * Stamps the entry as abandoned, which takes it out of every claimable
     * filter and out of countPending().
     *
     * A field rather than a second collection, unlike the MySQL adapter's
     * `outbox_dead` table. The reason the split is worth it there is that
     * `reserve()` claims with one indexed UPDATE whose predicate every relay
     * pays on every pass; here `reserve()` already runs a find with an `$or`
     * over the lease, so `abandoned_at` joins a filter that exists rather than
     * adding cost to one that did not.
     *
     * Conditioned on the field still being absent, so a repeated call cannot
     * move the timestamp — a relay dying between abandoning and recording it
     * retries the whole step, and the interesting number is when the entry
     * *first* stopped being deliverable.
     *
     * @param OutboxEntry $entry
     * @return void
     */
    public function markAbandoned(
        OutboxEntry $entry
    ): void {
        $this->collection->updateOne(
            ['_id' => new ObjectId($entry->getId()), 'abandoned_at' => null],
            ['$set' => ['abandoned_at' => $this->stamp(), 'reserved_at' => null, 'reserved_by' => null]]
        );
    }

    /**
     * @param int $limit
     * @return list<OutboxEntry>
     */
    public function retrieveAbandoned(
        int $limit
    ): array {
        if ($limit <= 0) {
            return [];
        }

        $entries = [];

        $cursor = $this->collection->find(
            ['abandoned_at' => ['$ne' => null]],
            ['sort' => ['_id' => 1], 'limit' => $limit]
        );

        foreach ($this->toDocuments($cursor->toArray()) as $document) {
            $entries[] = $this->toEntry($document);
        }

        return $entries;
    }

    public function countPending(): int
    {
        return $this->collection->countDocuments(['abandoned_at' => null]);
    }

    public function countAbandoned(): int
    {
        return $this->collection->countDocuments(['abandoned_at' => ['$ne' => null]]);
    }

    /**
     * The relay's own now, as this collection stores it.
     *
     * Only `abandoned_at` is written this way — a record of when this relay
     * gave up, never compared against anything. The lease uses `$$NOW`
     * instead, because a boundary two relays have to agree on cannot be
     * measured by whichever of them happens to ask.
     *
     * @return UTCDateTime
     */
    private function stamp(): UTCDateTime
    {
        return new UTCDateTime($this->clock->now()->getTimestamp() * 1000);
    }

    /**
     * Unclaimed, or claimed so long ago that the claim has lapsed — and never
     * one the relay has given up on.
     *
     * @return array<string, mixed>
     */
    private function claimableFilter(): array
    {
        // `$$NOW` rather than a cutoff computed here, so the boundary is the
        // server's to draw. Both sides of the comparison are then read
        // from one clock, which is the property a relay fleet depends on: with
        // the cutoff computed in PHP, two relays whose hosts were a minute
        // apart disagreed about when a claim had lapsed.
        //
        // An aggregation expression cannot be served by an index, which costs
        // nothing here — the previous `$or` over `reserved_at` could not be
        // either, and this collection carries no index beyond `_id`.
        $lapsed = ['$lt' => ['$reserved_at', ['$subtract' => ['$$NOW', max(0, $this->leaseSeconds) * 1000]]]];

        return [
            'abandoned_at' => null,
            // Unclaimed, or claimed so long ago that the claim has lapsed.
            '$expr' => ['$or' => [['$eq' => ['$reserved_at', null]], $lapsed]],
        ];
    }

    /**
     * @param array<string, mixed> $document
     * @return OutboxEntry
     */
    private function toEntry(
        array $document
    ): OutboxEntry {
        $id = $document['_id'] ?? null;
        $attempts = $document['attempts'] ?? 0;

        unset(
            $document['_id'],
            $document['attempts'],
            $document['reserved_at'],
            $document['reserved_by'],
            $document['abandoned_at']
        );

        // The document's own id, not the claim token: a token covers every
        // entry claimed in one pass, so marking by token would take the whole
        // batch down with a single delivery.
        return new OutboxEntry(
            $id instanceof ObjectId ? (string) $id : '',
            $this->entryFactory->recordToDomainEvent(EventPersistenceRecord::fromArray($document)),
            is_numeric($attempts) ? (int) $attempts : 0
        );
    }
}
