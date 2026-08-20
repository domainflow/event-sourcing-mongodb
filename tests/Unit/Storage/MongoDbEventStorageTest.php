<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Unit\Storage;

use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Exception\ConcurrencyException;
use DomainFlow\EventSourcing\Exception\EventSourcingException;
use DomainFlow\EventSourcing\Interface\EventStorageInterface;
use DomainFlow\EventSourcing\Upcaster\ReflectionEventFactory;
use DomainFlow\EventSourcingCore\Provider\Unit\AbstractEventStorageTestCase;
use DomainFlow\EventSourcingCore\Provider\Unit\AnotherDummyEvent;
use DomainFlow\EventSourcingMongoDB\Outbox\MongoDbOutboxStorage;
use DomainFlow\EventSourcingMongoDB\Storage\MongoDbEventEntryFactory;
use DomainFlow\EventSourcingMongoDB\Storage\MongoDbEventStorage;
use DomainFlow\EventSourcingMongoDB\Tests\Setup\MongoDbHelper;
use DomainFlow\Uuid\UuidV6;
use MongoDB\BSON\Decimal128;
use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Driver\Exception\BulkWriteException;
use MongoDB\Driver\WriteConcern;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\UsesClass;
use ReflectionProperty;
use RuntimeException;
use Throwable;

#[CoversClass(MongoDbEventStorage::class)]
#[CoversClass(MongoDbEventEntryFactory::class)]
#[UsesClass(MongoDbOutboxStorage::class)]
class MongoDbEventStorageTest extends AbstractEventStorageTestCase
{
    use MongoDbHelper;

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropMongoDatabase();
    }

    /**
     * The adapter is only genuinely atomic on a deployment that supports
     * multi-document transactions, and that is a replica set — a standalone
     * mongod refuses startTransaction() at any server version. If the test
     * environment ever drifts back to a standalone, the atomicity contract
     * case would keep passing on the weaker fallback path and hide it, so
     * assert the branch that is actually being exercised.
     */
    #[Group('replicaset')]
    public function test_the_test_deployment_exercises_the_transaction_path(): void
    {
        $storage = new MongoDbEventStorage($this->getMongoDatabase());
        $aggregateId = EntityIdentifier::fromString('TransactionPathProbe');

        $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);

        $property = new ReflectionProperty(MongoDbEventStorage::class, 'supportsTransactions');

        $this->assertTrue(
            $property->getValue($storage),
            'The test MongoDB must run as a replica set — see docker-compose.yml.'
        );
    }

    protected function tearDown(): void
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

    protected function getStorageWhoseWritesFailWithoutConflict(): EventStorageInterface
    {
        // A schema validator rejects the write with error 121, not 11000 —
        // a real write error that is not a version clash.
        $this->getMongoDatabase()->createCollection('non_conflicting_failure_events', [
            'validator' => ['$jsonSchema' => ['bsonType' => 'object', 'required' => ['a_field_no_event_has']]],
        ]);

        return new MongoDbEventStorage(
            $this->getMongoDatabase(),
            null,
            null,
            'non_conflicting_failure_events',
            allowNonAtomicBatches: $this->allowNonAtomicBatches()
        );
    }

    protected function getStorageWithFactory(): EventStorageInterface
    {
        return new MongoDbEventStorage(
            $this->getMongoDatabase(),
            null,
            new ReflectionEventFactory(),
            allowNonAtomicBatches: $this->allowNonAtomicBatches()
        );
    }

    public function test_storeEventsThrowsConcurrencyExceptionOnDuplicateVersion(): void
    {
        $storage = $this->getStorage();
        $aggregateId = EntityIdentifier::fromString('ConcurrentAggregate');

        $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);

        $this->expectException(ConcurrencyException::class);

        $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);
    }

    public function test_storeEventsDoesNotAdvanceMaxVersionOnConflict(): void
    {
        $storage = $this->getStorage();
        $aggregateId = EntityIdentifier::fromString('ConcurrentAggregate2');

        $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);

        try {
            $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);
            $this->fail('Expected ConcurrencyException was not thrown.');
        } catch (ConcurrencyException) {
            // expected
        }

        $this->assertEquals(1, $storage->getCurrentMaxVersion($aggregateId)->toInt());
        $this->assertCount(1, iterator_to_array($storage->retrieveAllEvents(), false));
    }

    public function test_retrieveEventsThrowsOnMissingPayloadField(): void
    {
        $database = $this->getMongoDatabase();
        $aggregateId = EntityIdentifier::fromString('CorruptPayloadAggregate');

        $database->selectCollection(MongoDbEventStorage::DEFAULT_COLLECTION_NAME)->insertOne([
            'event_id' => (string) UuidV6::generate(),
            'aggregate_id' => 'CorruptPayloadAggregate',
            'event_class' => AnotherDummyEvent::class,
            'version' => 1,
            'occurred_on' => '2026-01-01 00:00:00.000000',
            'payload' => ['aggregateId' => 'CorruptPayloadAggregate'],
        ]);

        $storage = $this->getStorage();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Missing required payload field');

        $storage->retrieveEvents($aggregateId);
    }

    public function test_retrievePaginatedEventsReturnsEmptyArrayForZeroLimit(): void
    {
        $storage = $this->getStorage();
        $aggregateId = EntityIdentifier::fromString('ZeroLimitAggregate');

        $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);

        $this->assertSame([], $storage->retrievePaginatedEvents(0, 0));
    }

    public function test_withoutTransactionSupportABatchIsStored(): void
    {
        $storage = $this->getStorageWithoutTransactions();
        $aggregateId = EntityIdentifier::fromString('FallbackHappyPath');

        $storage->storeEvents([
            new AnotherDummyEvent($aggregateId, 1),
            new AnotherDummyEvent($aggregateId, 2),
        ]);

        $this->assertCount(2, $storage->retrieveEvents($aggregateId), 'The compensating path must still store a healthy batch.');
    }

    public function test_withoutTransactionSupportABatchRepeatingAVersionIsRejectedBeforeAnythingIsWritten(): void
    {
        $storage = $this->getStorageWithoutTransactions();
        $aggregateId = EntityIdentifier::fromString('FallbackRepeatedVersion');

        try {
            $storage->storeEvents([
                new AnotherDummyEvent($aggregateId, 1),
                new AnotherDummyEvent($aggregateId, 1),
            ]);
            $this->fail('A batch carrying the same version twice must be rejected.');
        } catch (ConcurrencyException $exception) {
            $this->assertStringContainsString('duplicate event versions', $exception->getMessage());
        }

        $this->assertSame([], $storage->retrieveEvents($aggregateId), 'Nothing may be written when the batch is rejected up front.');
    }

    public function test_withoutTransactionSupportAVersionThatIsAlreadyStoredIsRejectedBeforeAnythingIsWritten(): void
    {
        $storage = $this->getStorageWithoutTransactions();
        $aggregateId = EntityIdentifier::fromString('FallbackVersionTaken');

        $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);

        try {
            $storage->storeEvents([
                new AnotherDummyEvent($aggregateId, 2),
                new AnotherDummyEvent($aggregateId, 1),
            ]);
            $this->fail('A batch reusing a stored version must be rejected.');
        } catch (ConcurrencyException $exception) {
            $this->assertStringContainsString('already exists', $exception->getMessage());
        }

        $this->assertCount(
            1,
            $storage->retrieveEvents($aggregateId),
            'The pre-check must run before the insert, so version 2 must not have landed either.'
        );
    }

    public function test_withoutTransactionSupportDocumentsThatLandedBeforeAFailureAreCompensated(): void
    {
        // Without transactions an ordered insertMany() keeps what it already
        // wrote. The unique index below makes the second document fail for a
        // reason the version pre-check cannot see, which is what a concurrent
        // writer does in production.
        $this->getMongoDatabase()
            ->selectCollection(MongoDbEventStorage::DEFAULT_COLLECTION_NAME)
            ->createIndex(['event_id' => 1], ['unique' => true, 'name' => 'uq_event_id_probe']);

        $storage = new MongoDbEventStorage(
            $this->getMongoDatabase(),
            new MalformedEventEntryFactory(constantEventId: 'collides-with-itself'),
            null,
            MongoDbEventStorage::DEFAULT_COLLECTION_NAME,
            transactionsSupported: false,
            allowNonAtomicBatches: true
        );
        $aggregateId = EntityIdentifier::fromString('FallbackCompensated');

        try {
            $storage->storeEvents([
                new AnotherDummyEvent($aggregateId, 1),
                new AnotherDummyEvent($aggregateId, 2),
            ]);
            $this->fail('The second document should have violated the unique index.');
        } catch (Throwable) {
            // The failure itself is the other tests' subject; this one is about
            // what is left behind.
        }

        $this->assertSame(
            [],
            $storage->retrieveEvents($aggregateId),
            'The document that landed before the failure must be compensated away — a half-written stream is unrecoverable.'
        );
    }

    public function test_withoutTransactionSupportAFailureThatIsNotAWriteErrorSurfacesUnchanged(): void
    {
        // No event id, so compensation has nothing to identify documents by
        // and must do nothing rather than fall back to a broader match.
        $storage = new MongoDbEventStorage(
            $this->getMongoDatabase(),
            new MalformedEventEntryFactory(unencodableFromCall: 1, dropEventId: true),
            null,
            MongoDbEventStorage::DEFAULT_COLLECTION_NAME,
            transactionsSupported: false,
            allowNonAtomicBatches: true
        );
        $aggregateId = EntityIdentifier::fromString('FallbackEncodingFailure');

        try {
            $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);
            $this->fail('An unencodable document should have failed the write.');
        } catch (ConcurrencyException) {
            $this->fail('An encoding failure is not a concurrency conflict.');
        } catch (Throwable $throwable) {
            $this->assertNotInstanceOf(BulkWriteException::class, $throwable, 'Guard: this case is meant to fail before the write, not during it.');
        }

        $this->assertSame([], iterator_to_array($storage->retrieveAllEvents(), false), 'Nothing may be left behind.');
    }

    #[Group('replicaset')]
    public function test_theTransactionPathRethrowsAFailureThatIsNotAWriteError(): void
    {
        $storage = new MongoDbEventStorage(
            $this->getMongoDatabase(),
            new MalformedEventEntryFactory(unencodableFromCall: 1),
            null,
            MongoDbEventStorage::DEFAULT_COLLECTION_NAME,
            transactionsSupported: true
        );
        $aggregateId = EntityIdentifier::fromString('TransactionEncodingFailure');

        try {
            $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);
            $this->fail('An unencodable document should have failed the write.');
        } catch (ConcurrencyException) {
            $this->fail('An encoding failure is not a concurrency conflict.');
        } catch (Throwable $throwable) {
            $this->assertNotInstanceOf(BulkWriteException::class, $throwable, 'Guard: this case is meant to fail before the write, not during it.');
        }

        $this->assertSame([], iterator_to_array($storage->retrieveAllEvents(), false), 'The aborted transaction must leave nothing behind.');
    }

    public function test_aWriteErrorThatIsNotADuplicateKeyKeepsItsOwnIdentity(): void
    {
        // A schema validator rejects the write with error 121, not 11000. If
        // that arrived as a ConcurrencyException, a consumer retrying on
        // concurrency conflicts would retry it forever.
        $collectionName = 'validated_events';
        $this->getMongoDatabase()->createCollection($collectionName, [
            'validator' => ['$jsonSchema' => ['bsonType' => 'object', 'required' => ['a_field_no_event_has']]],
        ]);

        $storage = new MongoDbEventStorage(
            $this->getMongoDatabase(),
            null,
            null,
            $collectionName,
            allowNonAtomicBatches: $this->allowNonAtomicBatches()
        );
        $aggregateId = EntityIdentifier::fromString('ValidatorRejected');

        try {
            $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);
            $this->fail('The validator should have rejected the document.');
        } catch (ConcurrencyException) {
            $this->fail('A validation failure must keep its own identity, not arrive as a concurrency conflict.');
        } catch (BulkWriteException $exception) {
            $this->assertNotSame(11000, $exception->getWriteResult()->getWriteErrors()[0]->getCode(), 'Guard: this case is meant to exercise a non-duplicate-key write error.');
        }
    }

    public function test_aFailedDeploymentProbeIsNotRemembered(): void
    {
        // Port 1 has nothing behind it, so the probe cannot complete. Remembering
        // that answer would pin this instance to the weaker fallback path for the
        // rest of the process, even once the deployment is healthy again.
        $database = (new Client('mongodb://127.0.0.1:1/', ['serverSelectionTimeoutMS' => 250]))
            ->selectDatabase(getenv('MONGO_DB') ?: 'event_sourcing_test');

        $storage = new MongoDbEventStorage($database);
        $aggregateId = EntityIdentifier::fromString('UnreachableDeployment');

        try {
            $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);
            $this->fail('Writing to an unreachable deployment should have failed.');
        } catch (ConcurrencyException) {
            $this->fail('An unreachable server is not a concurrency conflict.');
        } catch (Throwable) {
            // The driver's own failure, which is the right thing to surface.
        }

        $property = new ReflectionProperty(MongoDbEventStorage::class, 'supportsTransactions');

        $this->assertNull(
            $property->getValue($storage),
            'A probe that could not run must leave the question open, not answer it with "no transactions".'
        );
    }

    public function test_constructingAStorageNeitherIssuesDdlNorNeedsAReachableServer(): void
    {
        $database = (new Client('mongodb://127.0.0.1:1/', ['serverSelectionTimeoutMS' => 250]))
            ->selectDatabase(getenv('MONGO_DB') ?: 'event_sourcing_test');

        $storage = new MongoDbEventStorage($database);

        $this->assertInstanceOf(MongoDbEventStorage::class, $storage, 'Building the object must not talk to the server.');
    }

    public function test_compensationLeavesAConcurrentWritersEventsAlone(): void
    {
        $database = new RacingDatabase(
            $this->getMongoDatabase()->getManager(),
            getenv('MONGO_DB') ?: 'event_sourcing_test'
        );

        $storage = new MongoDbEventStorage(
            $database,
            null,
            null,
            MongoDbEventStorage::DEFAULT_COLLECTION_NAME,
            transactionsSupported: false,
            allowNonAtomicBatches: true
        );

        $aggregateId = EntityIdentifier::fromString('CompensationRace');
        $foreignEventId = (string) UuidV6::generate();

        // Another writer takes version 2 in the window between the pre-check
        // and the insert — the exact race compensation is there to survive.
        $database->collection(MongoDbEventStorage::DEFAULT_COLLECTION_NAME)->raceOnNextRead(static function (RacingCollection $collection) use ($foreignEventId): void {
            $collection->insertOne([
                'event_id' => $foreignEventId,
                'aggregate_id' => 'CompensationRace',
                'event_class' => AnotherDummyEvent::class,
                'version' => 2,
                'occurred_on' => '2026-01-01 00:00:00.000000',
                'payload' => ['aggregateId' => 'CompensationRace'],
            ]);
        });

        try {
            $storage->storeEvents([
                new AnotherDummyEvent($aggregateId, 1),
                new AnotherDummyEvent($aggregateId, 2),
            ]);
            $this->fail('Version 2 was taken by the concurrent writer, so the batch had to fail.');
        } catch (ConcurrencyException) {
            // expected — the batch lost the race
        }

        $collection = $this->getMongoDatabase()->selectCollection(MongoDbEventStorage::DEFAULT_COLLECTION_NAME);

        $this->assertSame(
            1,
            $collection->countDocuments(['event_id' => $foreignEventId]),
            'Compensation must undo only what this batch wrote. Deleting by version removes whatever occupies that slot — here a committed event belonging to another writer, which the caller then never learns about.'
        );
        $this->assertSame(
            0,
            $collection->countDocuments(['aggregate_id' => 'CompensationRace', 'version' => 1]),
            'The document this batch did write must still be compensated away.'
        );
    }

    /**
     * A wrong global position is worse than a failed write: every projector
     * downstream would resume from a number that means nothing. If the counter
     * cannot be read as one, the write has to stop.
     *
     * Decimal128 is not a contrived value here — $inc accepts it, so a counter
     * seeded or migrated as a decimal keeps working at the server and only
     * stops being an integer on the way back.
     */
    public function test_aGlobalPositionCounterThatIsNotANumberStopsTheWrite(): void
    {
        $this->getMongoDatabase()
            ->selectCollection(MongoDbEventStorage::COUNTERS_COLLECTION_NAME)
            ->insertOne(['_id' => 'events_global_position', 'value' => new Decimal128('5')]);

        $storage = $this->getStorage();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('global position counter');

        $storage->storeEvents([new AnotherDummyEvent(EntityIdentifier::fromString('CorruptCounter'), 1)]);
    }

    /**
     * Under MongoDB's default write concern (w:1, j:false) an insert is
     * acknowledged as soon as one node has it in memory, so an event can be
     * reported as stored to the aggregate and then lost in a failover. The
     * aggregate would then believe it emitted history the store never kept,
     * which is the same class of damage a transactional batch prevents.
     *
     * Asserted structurally because durability cannot be observed from a test
     * that does not kill a node — but a missing write concern can.
     */
    public function test_bothCollectionsRequireADurableWriteConcern(): void
    {
        $storage = $this->getStorage();

        foreach (['collection', 'counters'] as $property) {
            $collection = (new ReflectionProperty(MongoDbEventStorage::class, $property))->getValue($storage);
            $this->assertInstanceOf(Collection::class, $collection);

            $writeConcern = $collection->getWriteConcern();
            $this->assertInstanceOf(WriteConcern::class, $writeConcern, sprintf('%s must not inherit the deployment default.', $property));
            $this->assertSame(WriteConcern::MAJORITY, $writeConcern->getW(), sprintf('%s must wait for a majority.', $property));
            $this->assertTrue($writeConcern->getJournal(), sprintf('%s must wait for the journal.', $property));
        }
    }

    public function test_aStoredBatchQueuesOneDeliveryPerEvent(): void
    {
        $outbox = new MongoDbOutboxStorage($this->getMongoDatabase());
        $storage = new MongoDbEventStorage(
            $this->getMongoDatabase(),
            null,
            null,
            MongoDbEventStorage::DEFAULT_COLLECTION_NAME,
            null,
            $outbox,
            allowNonAtomicBatches: $this->allowNonAtomicBatches()
        );
        $aggregateId = EntityIdentifier::fromString('outbox-happy-path');

        $storage->storeEvents([
            new AnotherDummyEvent($aggregateId, 1),
            new AnotherDummyEvent($aggregateId, 2),
        ]);

        $this->assertSame(2, $outbox->countPending());
    }

    /**
     * The property the outbox rests on, in the direction that actually
     * distinguishes anything: if the delivery cannot be recorded, the events
     * must not be stored either. Events with no pending delivery are exactly
     * the silent message loss the outbox exists to prevent.
     *
     * Needs a transaction, so it is a replica-set case. The standalone fallback
     * has no session to enrol the entry in and cannot make this promise — which
     * is stated in docs/ARCHITECTURE.md rather than glossed over.
     */
    #[Group('replicaset')]
    public function test_anOutboxThatCannotBeWrittenRollsBackTheEventsToo(): void
    {
        $database = $this->getMongoDatabase();

        // A validator the outbox document can never satisfy, so enqueuing
        // inside the transaction fails and has to take the batch with it.
        $database->createCollection('rejecting_outbox', [
            'validator' => ['$jsonSchema' => ['bsonType' => 'object', 'required' => ['a_field_no_event_has']]],
        ]);

        $outbox = new MongoDbOutboxStorage($database, null, 300, 'rejecting_outbox');
        $storage = new MongoDbEventStorage(
            $database,
            null,
            null,
            MongoDbEventStorage::DEFAULT_COLLECTION_NAME,
            true,
            $outbox
        );
        $aggregateId = EntityIdentifier::fromString('outbox-unwritable');

        try {
            $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);
            $this->fail('Recording the delivery failed, so the write had to fail.');
        } catch (Throwable) {
            // expected
        }

        $this->assertSame(
            [],
            $storage->retrieveEvents($aggregateId),
            'The events must not survive a delivery that could not be recorded.'
        );
    }

    /**
     * A guarantee everybody meets except one deployment that says nothing is
     * not a guarantee. A standalone mongod cannot roll a failed call
     * back, so it refuses to write at all until the operator says, in the
     * constructor, that they accept that.
     *
     * Driven with transactionsSupported: false rather than by pointing at a
     * second server — the flag exists precisely so this branch is reachable
     * without one, and the standalone CI job covers the real thing.
     */
    public function test_aDeploymentWithoutTransactionsRefusesToWriteUntilTheOperatorOptsOut(): void
    {
        $storage = new MongoDbEventStorage(
            $this->getMongoDatabase(),
            null,
            null,
            MongoDbEventStorage::DEFAULT_COLLECTION_NAME,
            transactionsSupported: false
        );
        $aggregateId = EntityIdentifier::fromString('RefusesWithoutTransactions');

        try {
            $storage->storeEvents([new AnotherDummyEvent($aggregateId, 1)]);
            $this->fail('A deployment without transactions must refuse the write.');
        } catch (EventSourcingException $exception) {
            $this->assertStringContainsString(
                'allowNonAtomicBatches',
                $exception->getMessage(),
                'The message must name the flag that turns the refusal off.'
            );
            $this->assertStringContainsString(
                'partial batch',
                $exception->getMessage(),
                'The message must say what is given up, not only that something is.'
            );
        }

        $this->assertSame(
            [],
            $storage->retrieveEvents($aggregateId),
            'A refused write must not have written.'
        );
    }

    /**
     * The compensating path, opted into deliberately. A
     * deployment without transactions refuses to write at all unless the
     * operator says so, which is the whole point of the flag — so every test
     * that wants to exercise the fallback has to ask for it by name.
     */
    private function getStorageWithoutTransactions(): MongoDbEventStorage
    {
        return new MongoDbEventStorage(
            $this->getMongoDatabase(),
            null,
            null,
            MongoDbEventStorage::DEFAULT_COLLECTION_NAME,
            transactionsSupported: false,
            allowNonAtomicBatches: true
        );
    }

    public function test_deleteEventsOnlyRemovesTargetAggregate(): void
    {
        $storage = $this->getStorage();
        $keepId = EntityIdentifier::fromString('KeepAggregate');
        $deleteId = EntityIdentifier::fromString('DeleteAggregate');

        $storage->storeEvents([new AnotherDummyEvent($keepId, 1)]);
        $storage->storeEvents([new AnotherDummyEvent($deleteId, 1)]);

        $storage->deleteEvents($deleteId);

        $allEvents = iterator_to_array($storage->retrieveAllEvents(), false);
        $this->assertCount(1, $allEvents);
        $this->assertEquals((string) $keepId, (string) $allEvents[0]->getAggregateId());
    }

    /**
     * The same storage, built so its writes are enqueued for a relay.
     *
     * @return EventStorageInterface|null
     */
    protected function getStorageDeliveringThroughOutbox(): ?EventStorageInterface
    {
        return new MongoDbEventStorage(
            $this->getMongoDatabase(),
            outbox: new MongoDbOutboxStorage($this->getMongoDatabase()),
            allowNonAtomicBatches: $this->allowNonAtomicBatches()
        );
    }
}
