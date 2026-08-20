<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Storage;

use DomainFlow\EventSourcing\Event\EventPersistenceRecord;
use DomainFlow\EventSourcing\Event\EventVersion;
use DomainFlow\EventSourcing\Event\GlobalEventPage;
use DomainFlow\EventSourcing\Exception\ConcurrencyException;
use DomainFlow\EventSourcing\Exception\EventSourcingException;
use DomainFlow\EventSourcing\Interface\DomainEventInterface;
use DomainFlow\EventSourcing\Interface\EntityIdentifierInterface;
use DomainFlow\EventSourcing\Interface\EventEntryFactoryInterface;
use DomainFlow\EventSourcing\Interface\EventFactoryInterface;
use DomainFlow\EventSourcing\Interface\EventStorageInterface;
use DomainFlow\EventSourcing\Interface\OutboxBackedStorageInterface;
use DomainFlow\EventSourcingMongoDB\Outbox\MongoDbOutboxStorage;
use DomainFlow\EventSourcingMongoDB\Support\AssertsMongoDocument;
use DomainFlow\EventSourcingMongoDB\Support\EnsuresMongoIndexes;
use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Driver\Exception\BulkWriteException;
use MongoDB\Driver\WriteConcern;
use MongoDB\Operation\FindOneAndUpdate;
use RuntimeException;
use Throwable;

/**
 * MongoDB-backed EventStorageInterface implementation.
 *
 * Document shape (see docs/ARCHITECTURE.md): one document per event in a single
 * `events` collection, with the payload stored as a native BSON subdocument
 * (via MongoDbEventEntryFactory) rather than an encoded JSON string.
 *
 * Concurrency is enforced by a unique compound index on (aggregate_id, version);
 * a duplicate-version write surfaces as the driver's BulkWriteException, which
 * this class translates into Core's ConcurrencyException so no MongoDB-specific
 * type crosses the EventStorageInterface boundary.
 */
final class MongoDbEventStorage implements EventStorageInterface, OutboxBackedStorageInterface
{
    use AssertsMongoDocument;
    use EnsuresMongoIndexes;

    public const string DEFAULT_COLLECTION_NAME = 'events';

    /** Holds the global position counter; one document, one field. */
    public const string COUNTERS_COLLECTION_NAME = 'counters';

    private const string GLOBAL_POSITION_COUNTER = 'events_global_position';

    /** MongoDB's duplicate-key error. */
    private const int DUPLICATE_KEY_ERROR = 11000;

    private readonly Collection $collection;
    private readonly Collection $counters;
    private readonly EventEntryFactoryInterface $entryFactory;
    private readonly Database $database;

    /** Lazily resolved on first write; null until then. */
    private ?bool $supportsTransactions = null;

    /** Whether the unique index has been ensured in this instance's lifetime. */
    private bool $indexesEnsured = false;

    /**
     * @param bool|null $transactionsSupported Overrides the deployment probe.
     *        Null — the default — asks the server on first write. Pass false to
     *        force the compensating fallback (a test needs a way to reach it on
     *        a replica set, and an operator may want it for a deployment where
     *        transactions exist but are not wanted), or true to skip the probe
     *        on a deployment known to support them.
     * @param bool $allowNonAtomicBatches Whether to write against a deployment
     *        that has no transactions and therefore cannot give the
     *        all-or-nothing guarantee EventStorageInterface::storeEvents()
     *        states. False — the default — refuses on the first write. Pass
     *        true only for a standalone mongod whose operator has accepted
     *        that a process dying mid-compensation leaves a partial batch
     *        behind.
     */
    public function __construct(
        Database $database,
        ?EventEntryFactoryInterface $entryFactory = null,
        ?EventFactoryInterface $eventFactory = null,
        string $collectionName = self::DEFAULT_COLLECTION_NAME,
        ?bool $transactionsSupported = null,
        private readonly ?MongoDbOutboxStorage $outbox = null,
        private readonly bool $allowNonAtomicBatches = false
    ) {
        $this->database = $database;
        $this->supportsTransactions = $transactionsSupported;
        // w: majority, j: true on both collections rather than inheriting the
        // deployment default of w:1, j:false. Under that default an insert is
        // acknowledged once one node has it in memory, so an event can be
        // acknowledged to the aggregate and then lost in a failover — the
        // aggregate believes it emitted something the store never kept. Set
        // here rather than left to the consumer, because a store that quietly
        // loses acknowledged writes is not a store.
        $options = [
            'typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array'],
            'writeConcern' => self::durableWriteConcern(),
        ];

        $this->collection = $database->selectCollection($collectionName, $options);
        $this->counters = $database->selectCollection(self::COUNTERS_COLLECTION_NAME, $options);
        // The event factory goes to the entry factory that will use it, not
        // into a process-wide static.
        $this->entryFactory = $entryFactory ?? new MongoDbEventEntryFactory($eventFactory);
    }

    /**
     * Appends the whole call atomically: either every event lands, or none
     * does, across every aggregate the batch touches.
     *
     * On a replica set (or sharded cluster) that is one multi-document
     * transaction covering the entire call — not one per aggregate, which is
     * what the storage contract requires. A transaction is not limited to a
     * single aggregate; scoping it that way was a choice, and it gave a
     * consumer writing two aggregates in one call a weaker answer here than
     * from MySQL or the in-memory reference.
     *
     * On a standalone deployment there are no transactions at all, so this
     * refuses to write unless the operator passed $allowNonAtomicBatches. What
     * that flag buys is the compensating path below: the batch is validated
     * against the stored versions first, and anything that did land is removed
     * again if the write still fails. That restores the store in every case
     * except a process that dies between the failed insert and its
     * compensation — which is exactly the case a transaction rules out and
     * best-effort compensation cannot.
     *
     * @param array<DomainEventInterface> $events
     */
    public function storeEvents(
        array $events
    ): void {
        if ($events === []) {
            return;
        }

        $grouped = $this->groupByAggregate($events);
        $useTransaction = $this->supportsTransactions();

        if (!$useTransaction) {
            $this->assertNonAtomicBatchesAreAllowed();
        }

        // Grouped rather than in call order, so one aggregate's events stay
        // contiguous and take consecutive global positions. The batch is one
        // unit either way; this only decides how the positions are laid out.
        $ordered = array_merge(...array_values($grouped));

        $documents = array_values(array_map(
            fn (DomainEventInterface $event): array => $this->entryFactory->createFromDomainEvent($event)->getValues(),
            $ordered
        ));

        $this->ensureIndexes();

        $firstPosition = $this->reservePositions(count($documents));

        foreach ($documents as $index => $document) {
            $documents[$index] = ['global_position' => $firstPosition + $index] + $document;
        }

        if ($useTransaction) {
            $this->insertInTransaction($documents, $ordered);

            return;
        }

        foreach ($grouped as $aggregateIdString => $group) {
            $this->assertVersionsAreFree((string) $aggregateIdString, $group);
        }

        $this->insertWithCompensation($documents, $ordered, array_map(strval(...), array_keys($grouped)));
    }

    /**
     * Refuses a deployment that cannot give the guarantee
     * EventStorageInterface::storeEvents() states.
     *
     * Raised on the first write rather than in the constructor, because the
     * deployment probe is lazy and building a storage object must not
     * require a reachable server. So a misconfigured service starts up fine and
     * fails the moment it tries to store something — noisy, but never silent.
     *
     * @throws EventSourcingException
     * @return void
     */
    private function assertNonAtomicBatchesAreAllowed(): void
    {
        if ($this->allowNonAtomicBatches) {
            return;
        }

        throw new EventSourcingException(
            'This MongoDB deployment has no transactions, which means it is a standalone mongod: '
            . 'multi-document transactions have required a replica set since 4.0 and still do. '
            . 'EventStorageInterface::storeEvents() promises that a failed call leaves the store '
            . 'exactly as it was, and without transactions that can only be approximated by deleting '
            . 'the documents that already landed — which a process that dies mid-compensation will '
            . 'not do, leaving a partial batch behind for good. Run this against a replica set (a '
            . 'single-node one is enough), or pass $allowNonAtomicBatches: true to accept that '
            . 'weaker guarantee deliberately.'
        );
    }

    /**
     * Acknowledged by a majority of the replica set and on disk before the
     * write is reported as done. See the constructor for why this is not left
     * to the deployment default.
     *
     * @return WriteConcern
     */
    private static function durableWriteConcern(): WriteConcern
    {
        return new WriteConcern(WriteConcern::MAJORITY, 0, true);
    }

    /**
     * @param array<DomainEventInterface> $events
     * @return array<string, non-empty-list<DomainEventInterface>>
     */
    private function groupByAggregate(
        array $events
    ): array {
        $grouped = [];

        foreach ($events as $event) {
            $grouped[(string) $event->getAggregateId()][] = $event;
        }

        return $grouped;
    }

    /**
     * @param list<array<string, mixed>> $documents
     * @param array<DomainEventInterface> $events
     */
    private function insertInTransaction(
        array $documents,
        array $events
    ): void {
        $session = $this->database->getManager()->startSession();

        // A transaction takes its write concern from the transaction, not from
        // the collection, so the collection's setting would be ignored here.
        $session->startTransaction(['writeConcern' => self::durableWriteConcern()]);

        try {
            $this->collection->insertMany($documents, ['ordered' => true, 'session' => $session]);

            // Enrolled in the same transaction, so the pending delivery and
            // the events it describes commit together or not at all.
            $this->outbox?->enqueueInSession($events, $session);

            $session->commitTransaction();
        } catch (BulkWriteException $exception) {
            $session->abortTransaction();

            throw $this->translateWriteFailure($exception, $documents);
        } catch (Throwable $throwable) {
            $session->abortTransaction();

            throw $throwable;
        }
    }

    /**
     * Without transactions, insertMany(ordered) stops at the first failure but
     * keeps whatever it already wrote. Since every document carries its own
     * event id, the ones that landed can be removed again, restoring the
     * stream to its previous state without touching anything this call did
     * not write.
     *
     * @param list<array<string, mixed>> $documents
     * @param array<DomainEventInterface> $events
     * @param list<string> $aggregateIds
     */
    private function insertWithCompensation(
        array $documents,
        array $events,
        array $aggregateIds
    ): void {
        try {
            $this->collection->insertMany($documents, ['ordered' => true]);

            // No session to join on a standalone, so this is a second write.
            // The outbox inherits exactly the weaker guarantee this path
            // already carries — see the fallback note in docs/ARCHITECTURE.md.
            $this->outbox?->enqueue($events);
        } catch (BulkWriteException $exception) {
            $this->compensate($documents, $aggregateIds);

            throw $this->translateWriteFailure($exception, $documents);
        } catch (Throwable $throwable) {
            $this->compensate($documents, $aggregateIds);

            throw $throwable;
        }
    }

    /**
     * Removes this batch's documents, identified by event id.
     *
     * Not by version: a version identifies a slot in the stream, not a
     * document of ours. A concurrent writer that takes one of the batch's
     * versions between the pre-check and the insert is precisely what makes
     * the insert fail — and deleting by version would then remove that
     * writer's committed event along with ours, leaving the caller with a
     * ConcurrencyException and no hint that a foreign event just vanished.
     * Event ids are minted per event by the writing process, so they can only
     * ever match documents this call wrote.
     *
     * The aggregate ids narrow the scan to the streams this call touched —
     * every one of them, since the call is the unit rather than the
     * aggregate. They are a filter, not the identity; the event ids are that.
     *
     * @param list<array<string, mixed>> $documents
     * @param list<string> $aggregateIds
     */
    private function compensate(
        array $documents,
        array $aggregateIds
    ): void {
        $eventIds = array_values(array_filter(array_map(
            static fn (array $document): mixed => $document['event_id'] ?? null,
            $documents
        ), static fn (mixed $eventId): bool => is_string($eventId) && $eventId !== ''));

        if ($eventIds === []) {
            return;
        }

        $this->collection->deleteMany([
            'aggregate_id' => ['$in' => $aggregateIds],
            'event_id' => ['$in' => $eventIds],
        ]);
    }

    /**
     * @param array<DomainEventInterface> $events
     */
    private function assertVersionsAreFree(
        string $aggregateIdString,
        array $events
    ): void {
        $versions = array_map(
            static fn (DomainEventInterface $event): int => $event->getVersion()->toInt(),
            $events
        );

        if (count(array_unique($versions)) !== count($versions)) {
            throw new ConcurrencyException(
                sprintf('Batch for aggregate %s contains duplicate event versions.', $aggregateIdString)
            );
        }

        $clash = $this->collection->findOne(
            ['aggregate_id' => $aggregateIdString, 'version' => ['$in' => $versions]],
            ['projection' => ['version' => 1]]
        );

        if (is_array($clash)) {
            throw new ConcurrencyException(sprintf(
                'Event version %s for aggregate %s already exists.',
                is_scalar($clash['version'] ?? null) ? (string) $clash['version'] : '?',
                $aggregateIdString
            ));
        }
    }

    /**
     * Only a duplicate-key violation is a concurrency conflict. Reporting an
     * oversized document or a write-concern failure as one would send a
     * consumer that retries on ConcurrencyException into an endless loop.
     *
     * The message names the document the driver actually rejected, found by
     * the write error's own index into the batch. Naming the batch's first
     * event instead was close enough while a batch was one aggregate; now that
     * a call may span several, it would point at the wrong stream.
     *
     * @param list<array<string, mixed>> $documents
     */
    private function translateWriteFailure(
        BulkWriteException $exception,
        array $documents
    ): Throwable {
        foreach ($exception->getWriteResult()->getWriteErrors() as $error) {
            if ($error->getCode() === self::DUPLICATE_KEY_ERROR) {
                $document = $documents[$error->getIndex()] ?? [];

                return new ConcurrencyException(
                    sprintf(
                        'Event version %s for aggregate %s already exists.',
                        self::describe($document['version'] ?? null),
                        self::describe($document['aggregate_id'] ?? null)
                    ),
                    previous: $exception
                );
            }
        }

        return $exception;
    }

    /**
     * A document field as it belongs in a message, or a placeholder when the
     * driver handed back an index this call cannot resolve.
     */
    private static function describe(
        mixed $value
    ): string {
        return is_scalar($value) ? (string) $value : '?';
    }

    /**
     * Creates the unique index both write paths rest on — the transaction for
     * its conflict detection, the fallback for the duplicate-key error that
     * triggers compensation.
     *
     * Lazily rather than in the constructor: building a storage object should
     * not issue DDL or require a reachable server, and a constructor that does
     * both cannot be built at all when the deployment is down — which is
     * exactly when the code below has to behave sensibly.
     */
    private function ensureIndexes(): void
    {
        if ($this->indexesEnsured) {
            return;
        }

        // Tolerated when it is refused and the index is already there
        // when the index already exists, so an application user need not keep index privileges for
        // an operation that is setup rather than traffic. Refused *and*
        // missing still throws: these indexes are what makes a concurrent
        // append detectable.
        $this->ensureIndexExists(
            $this->collection,
            ['aggregate_id' => 1, 'version' => 1],
            ['unique' => true, 'name' => 'uq_aggregate_version']
        );

        $this->ensureIndexExists(
            $this->collection,
            ['global_position' => 1],
            ['unique' => true, 'name' => 'uq_global_position']
        );

        $this->indexesEnsured = true;
    }

    /**
     * Reserves a contiguous block of global positions.
     *
     * MongoDB has no auto-increment, and `_id` is not a substitute: an ObjectId
     * is a second-resolution timestamp plus five bytes of per-process
     * randomness, so two processes writing in the same second produce ids in
     * arbitrary order. A reader scanning `_id > cursor` would skip whatever a
     * peer wrote with a smaller id — precisely the failure a resumable cursor
     * exists to rule out. A counter document incremented with $inc is the one
     * primitive here that is genuinely monotonic.
     *
     * Reserved before the write and deliberately outside any transaction. Two
     * concurrent transactions incrementing the same document would conflict and
     * abort each other, serialising every writer in the deployment. The cost of
     * staying outside is that a failed batch burns its block, leaving a gap —
     * harmless, because readers scan for "greater than" and never for "the next
     * number".
     *
     * @param int $count
     * @return int The first position of the reserved block.
     */
    private function reservePositions(
        int $count
    ): int {
        $counter = $this->counters->findOneAndUpdate(
            ['_id' => self::GLOBAL_POSITION_COUNTER],
            ['$inc' => ['value' => $count]],
            ['upsert' => true, 'returnDocument' => FindOneAndUpdate::RETURN_DOCUMENT_AFTER]
        );

        $value = is_array($counter) ? ($counter['value'] ?? null) : null;

        if (!is_numeric($value)) {
            throw new RuntimeException('The global position counter did not return a number.');
        }

        return (int) $value - $count + 1;
    }

    private function supportsTransactions(): bool
    {
        if ($this->supportsTransactions !== null) {
            return $this->supportsTransactions;
        }

        try {
            // Database::command() returns documents under the *database's*
            // default type map, which is stdClass — not the array type map this
            // class sets on its own collection. Ask for arrays explicitly.
            $cursor = $this->database->command(['hello' => 1]);
            $cursor->setTypeMap(['root' => 'array', 'document' => 'array', 'array' => 'array']);

            $first = $cursor->toArray()[0] ?? [];
            $info = is_array($first) ? $first : [];

            $isReplicaSet = isset($info['setName']);
            $isSharded = ($info['msg'] ?? null) === 'isdbgrid';

            return $this->supportsTransactions = $isReplicaSet || $isSharded;
        } catch (Throwable) {
            // Deliberately not remembered. Caching this would let one transient
            // probe failure pin a long-lived instance to the weaker fallback
            // path for the rest of the process, silently giving up the
            // atomicity the deployment actually offers.
            return false;
        }
    }

    /**
     * @return array<DomainEventInterface>
     */
    public function retrieveEvents(
        EntityIdentifierInterface $aggregateId
    ): array {
        // Ordered by version, not occurred_on: a stream's order is defined by
        // the aggregate, not by the writing process's wall clock. This also
        // lets the uq_aggregate_version index serve the sort instead of an
        // in-memory sort bounded by the 32MB limit.
        $cursor = $this->collection->find(
            ['aggregate_id' => (string) $aggregateId],
            ['sort' => ['version' => 1]]
        );

        return $this->hydrateEvents($this->toDocuments($cursor->toArray()));
    }

    /**
     * Retrieve an aggregate's events newer than a given version.
     *
     * The bound is in the query, not in a filter afterwards: the snapshot load
     * path exists to avoid reading the events a snapshot already accounts for.
     * uq_aggregate_version serves both the range and the sort, so this also
     * stays clear of the 32MB in-memory sort limit.
     *
     * @param EntityIdentifierInterface $aggregateId
     * @param EventVersion $afterVersion
     * @return array<DomainEventInterface>
     */
    public function retrieveEventsFromVersion(
        EntityIdentifierInterface $aggregateId,
        EventVersion $afterVersion
    ): array {
        $cursor = $this->collection->find(
            [
                'aggregate_id' => (string) $aggregateId,
                'version' => ['$gt' => $afterVersion->toInt()],
            ],
            ['sort' => ['version' => 1]]
        );

        return $this->hydrateEvents($this->toDocuments($cursor->toArray()));
    }

    /**
     * Read the global stream from a position.
     *
     * @param string|null $afterPosition
     * @param int $limit
     * @return GlobalEventPage
     */
    public function retrieveEventsFromPosition(
        ?string $afterPosition,
        int $limit
    ): GlobalEventPage {
        // find() reads limit=0 as "no limit", not "no results", so passing the
        // number straight through would hand the whole store to a caller that
        // asked for none of it.
        if ($limit <= 0) {
            return new GlobalEventPage([], $afterPosition);
        }

        $documents = $this->collection->find(
            ['global_position' => ['$gt' => $afterPosition === null ? 0 : (int) $afterPosition]],
            ['sort' => ['global_position' => 1], 'limit' => $limit]
        )->toArray();

        $documents = $this->toDocuments($documents);
        $position = $afterPosition;

        if ($documents !== []) {
            $lastPosition = $documents[array_key_last($documents)]['global_position'] ?? null;
            $position = is_numeric($lastPosition) ? (string) $lastPosition : $afterPosition;
        }

        return new GlobalEventPage($this->hydrateEvents($documents), $position);
    }

    /**
     * Ordered by global position rather than by occurred_on. A wall clock is not an
     * ordering: two writers with skewed clocks interleave, and the index on
     * global_position also serves the sort instead of an in-memory sort bounded
     * by MongoDB's 32MB limit.
     *
     * Streamed rather than materialised: toArray() pulled the whole collection
     * into PHP, and the 32MB sort limit was
     * only the first of the two ceilings.
     *
     * @return iterable<DomainEventInterface>
     */
    public function retrieveAllEvents(): iterable
    {
        foreach ($this->collection->find([], ['sort' => ['global_position' => 1]]) as $document) {
            yield $this->hydrateEvent($this->toDocument($document));
        }
    }

    public function deleteEvents(
        EntityIdentifierInterface $aggregateId
    ): void {
        $this->collection->deleteMany(['aggregate_id' => (string) $aggregateId]);
    }

    /**
     * @return array<DomainEventInterface>
     */
    public function retrievePaginatedEvents(
        ?int $offset,
        ?int $limit
    ): array {
        // MongoDB's find() treats limit=0 as "no limit", not "zero results".
        if ($limit === 0) {
            return [];
        }

        $options = ['sort' => ['global_position' => 1]];

        if ($offset !== null) {
            $options['skip'] = $offset;
        }

        if ($limit !== null) {
            $options['limit'] = $limit;
        }

        $cursor = $this->collection->find([], $options);

        return $this->hydrateEvents($this->toDocuments($cursor->toArray()));
    }

    public function getCurrentMaxVersion(
        EntityIdentifierInterface $aggregateId
    ): EventVersion {
        $document = $this->collection->findOne(
            ['aggregate_id' => (string) $aggregateId],
            ['sort' => ['version' => -1], 'projection' => ['version' => 1]]
        );

        $version = is_array($document) && isset($document['version']) && is_numeric($document['version'])
            ? (int) $document['version']
            : 0;

        return EventVersion::fromInt($version);
    }

    /**
     * @param array<array<string, mixed>> $documents
     * @return array<DomainEventInterface>
     */
    private function hydrateEvents(
        array $documents
    ): array {
        $events = [];

        foreach ($documents as $document) {
            $events[] = $this->hydrateEvent($document);
        }

        return $events;
    }

    /**
     * @param array<string, mixed> $document
     * @return DomainEventInterface
     */
    private function hydrateEvent(
        array $document
    ): DomainEventInterface {
        unset($document['_id']);

        return $this->entryFactory->recordToDomainEvent(EventPersistenceRecord::fromArray($document));
    }

    /**
     * Whether a relay, rather than this process, will deliver what is written
     * here.
     *
     * Read from the outbox handed to this storage: it is the
     * configuration in force, not the classes installed. `EventSourcingFacade`
     * needs the answer because the second delivery path — a dispatcher — is
     * given to *it*, and with both in place every event goes out twice with
     * nothing reporting it.
     *
     * @return bool
     */
    public function deliversThroughOutbox(): bool
    {
        return $this->outbox !== null;
    }
}
