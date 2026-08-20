<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Unit\Outbox;

use DateTimeImmutable;
use DomainFlow\EventSourcing\Clock\FrozenClock;
use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcingCore\Provider\Unit\AbstractOutboxStorageTestCase;
use DomainFlow\EventSourcingCore\Provider\Unit\AnotherDummyEvent;
use DomainFlow\EventSourcingMongoDB\Outbox\MongoDbOutboxStorage;
use DomainFlow\EventSourcingMongoDB\Storage\MongoDbEventEntryFactory;
use DomainFlow\EventSourcingMongoDB\Tests\Setup\MongoDbHelper;
use MongoDB\BSON\UTCDateTime;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\UsesClass;

#[CoversClass(MongoDbOutboxStorage::class)]
#[UsesClass(MongoDbEventEntryFactory::class)]
final class MongoDbOutboxStorageTest extends AbstractOutboxStorageTestCase
{
    use MongoDbHelper;

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropMongoDatabase();
    }

    protected function tearDown(): void
    {
        $this->dropMongoDatabase();
    }

    protected function getOutbox(): MongoDbOutboxStorage
    {
        return new MongoDbOutboxStorage($this->getMongoDatabase());
    }

    /**
     * Two relays over one collection, each with its own clock.
     *
     * @param int $leaseSeconds
     * @param int $skewSeconds
     * @return array{0: MongoDbOutboxStorage, 1: MongoDbOutboxStorage}
     */
    protected function getRelaysWithSkewedClocks(
        int $leaseSeconds,
        int $skewSeconds
    ): array {
        $now = new DateTimeImmutable('now');

        return [
            new MongoDbOutboxStorage(
                $this->getMongoDatabase(),
                leaseSeconds: $leaseSeconds,
                clock: new FrozenClock($now)
            ),
            new MongoDbOutboxStorage(
                $this->getMongoDatabase(),
                leaseSeconds: $leaseSeconds,
                clock: new FrozenClock($now->modify(sprintf('+%d seconds', $skewSeconds)))
            ),
        ];
    }

    /**
     * A relay that dies between claiming and marking would otherwise strand
     * its entries: a queue that stops draining while reporting nothing.
     */
    public function test_anExpiredClaimIsPickedUpByTheNextRelay(): void
    {
        $outbox = new MongoDbOutboxStorage($this->getMongoDatabase(), leaseSeconds: 300);

        $outbox->enqueue([new AnotherDummyEvent(EntityIdentifier::fromString('OutboxStranded'), 1)]);

        $this->assertCount(1, $outbox->reserve(1), 'Precondition: the first relay claims it.');
        $this->assertSame([], $outbox->reserve(1), 'And holds it while the lease is live.');

        $this->getMongoDatabase()
            ->selectCollection(MongoDbOutboxStorage::DEFAULT_COLLECTION_NAME)
            ->updateMany([], [['$set' => ['reserved_at' => ['$subtract' => ['$$NOW', 3600 * 1000]]]]]);

        $this->assertCount(1, $outbox->reserve(1), 'With the lease lapsed it has to become claimable again.');
    }

    /**
     * The one instant this storage still takes from the relay's clock, and the
     * reason the parameter is not decorative.
     *
     * `abandoned_at` records when *this* relay gave up. Nothing compares it to
     * anything, so it does not need a fleet to agree on it — unlike the lease,
     * which is why the lease was moved to `$$NOW` and this was not.
     */
    public function test_theDeadLetterStampComesFromTheRelaysOwnClock(): void
    {
        $clock = new FrozenClock('2026-01-01 12:00:00.000000');
        $outbox = new MongoDbOutboxStorage($this->getMongoDatabase(), clock: $clock);

        $outbox->enqueue([new AnotherDummyEvent(EntityIdentifier::fromString('OutboxRelayStamp'), 1)]);
        $outbox->markAbandoned($outbox->reserve(1)[0]);

        $document = $this->getMongoDatabase()
            ->selectCollection(MongoDbOutboxStorage::DEFAULT_COLLECTION_NAME)
            ->findOne(['abandoned_at' => ['$ne' => null]], ['typeMap' => ['root' => 'array', 'document' => 'array']]);

        $this->assertIsArray($document);
        $this->assertInstanceOf(UTCDateTime::class, $document['abandoned_at']);
        $this->assertSame(
            '2026-01-01 12:00:00',
            $document['abandoned_at']->toDateTime()->format('Y-m-d H:i:s'),
            'The relay stamps its own giving-up; the store owns the lease and nothing else.'
        );
    }

    /**
     * The mechanism that makes this outbox transactional, asserted on its own
     * rather than through the event storage: an entry enrolled in a session
     * rolls back with that session's transaction.
     *
     * MongoDB transactions are not ambient on the connection the way MySQL's
     * are — they are a session object every participating write has to be
     * handed. That is the whole reason MongoDbEventStorage takes this concrete
     * class instead of the interface.
     */
    #[Group('replicaset')]
    public function test_anEntryEnrolledInATransactionRollsBackWithIt(): void
    {
        $outbox = $this->getOutbox();

        $session = $this->getMongoClient()->startSession();
        $session->startTransaction();

        $outbox->enqueueInSession(
            [new AnotherDummyEvent(EntityIdentifier::fromString('OutboxEnrolled'), 1)],
            $session
        );

        $session->abortTransaction();

        $this->assertSame(0, $outbox->countPending(), 'The entry has to go back with the transaction.');
    }

    public function test_reservingNothingAsksTheDatabaseNothing(): void
    {
        $this->assertSame([], $this->getOutbox()->reserve(0));
    }
}
