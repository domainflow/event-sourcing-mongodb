<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Unit\Crypto;

use DateTimeImmutable;
use DomainFlow\EventSourcing\Attribute\DataSubjectId;
use DomainFlow\EventSourcing\Attribute\PersonalData;
use DomainFlow\EventSourcing\Crypto\EncryptingEventEntryFactory;
use DomainFlow\EventSourcing\Crypto\InMemoryPersonalDataKeyStore;
use DomainFlow\EventSourcing\Crypto\RedactedValue;
use DomainFlow\EventSourcing\Crypto\SodiumCipher;
use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Event\EventVersion;
use DomainFlow\EventSourcing\Event\SourceEvent;
use DomainFlow\EventSourcing\Interface\EntityIdentifierInterface;
use DomainFlow\EventSourcing\Interface\EventEntryFactoryInterface;
use DomainFlow\EventSourcing\Interface\EventStorageInterface;
use DomainFlow\EventSourcing\Upcaster\ReflectionEventFactory;
use DomainFlow\EventSourcingCore\Provider\Unit\AbstractEventStorageTestCase;
use DomainFlow\EventSourcingMongoDB\Outbox\MongoDbOutboxStorage;
use DomainFlow\EventSourcingMongoDB\Storage\MongoDbEventEntryFactory;
use DomainFlow\EventSourcingMongoDB\Storage\MongoDbEventStorage;
use DomainFlow\EventSourcingMongoDB\Tests\Setup\MongoDbHelper;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;

/**
 * The whole storage contract, run through the crypto-shredding decorator.
 *
 * This adapter is the interesting one for the claim. It has **its own entry
 * factory**, and it stores the payload as a native subdocument rather than an
 * encoded string — so the envelope becomes a nested document, and any
 * assumption the decorator made about the payload being a JSON string would
 * surface right here.
 */
#[CoversClass(MongoDbEventStorage::class)]
#[UsesClass(MongoDbEventEntryFactory::class)]
#[UsesClass(MongoDbOutboxStorage::class)]
final class EncryptedMongoDbEventStorageTest extends AbstractEventStorageTestCase
{
    use MongoDbHelper;

    private ?InMemoryPersonalDataKeyStore $keys = null;

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropMongoDatabase();
    }

    protected function tearDown(): void
    {
        $this->dropMongoDatabase();
    }

    private function keys(): InMemoryPersonalDataKeyStore
    {
        return $this->keys ??= new InMemoryPersonalDataKeyStore();
    }

    private function encrypting(
        EventEntryFactoryInterface $inner
    ): EncryptingEventEntryFactory {
        return new EncryptingEventEntryFactory($inner, $this->keys(), new SodiumCipher());
    }

    protected function getStorage(): EventStorageInterface
    {
        return new MongoDbEventStorage(
            $this->getMongoDatabase(),
            $this->encrypting(new MongoDbEventEntryFactory(new ReflectionEventFactory())),
            allowNonAtomicBatches: $this->allowNonAtomicBatches()
        );
    }

    protected function getStorageWithFactory(): EventStorageInterface
    {
        return $this->getStorage();
    }

    protected function getStorageWhoseWritesFailWithoutConflict(): EventStorageInterface
    {
        $this->getMongoDatabase()->createCollection('non_conflicting_failure_events', [
            'validator' => ['$jsonSchema' => ['bsonType' => 'object', 'required' => ['a_field_no_event_has']]],
        ]);

        return new MongoDbEventStorage(
            $this->getMongoDatabase(),
            $this->encrypting(new MongoDbEventEntryFactory(new ReflectionEventFactory())),
            null,
            'non_conflicting_failure_events',
            allowNonAtomicBatches: $this->allowNonAtomicBatches()
        );
    }

    public function test_an_erased_subject_is_redacted_when_the_stream_is_replayed(): void
    {
        $storage = $this->getStorage();
        $aggregateId = EntityIdentifier::fromString('order-erased');

        $event = new MongoEncryptedCustomerRegistered($aggregateId, null, 'customer-1', 'ada@example.com', 'ORD-42');
        $event->setVersion(EventVersion::fromInt(1));
        $storage->storeEvents([$event]);

        $document = $this->getMongoDatabase()->selectCollection('events')->findOne(['aggregate_id' => 'order-erased']);
        $this->assertStringNotContainsString(
            'ada@example.com',
            json_encode($document, JSON_THROW_ON_ERROR),
            'The personal data reached the document in the clear.'
        );

        $this->keys()->forget('customer-1');

        $replayed = $storage->retrieveEvents($aggregateId);

        $this->assertCount(1, $replayed);
        $this->assertInstanceOf(MongoEncryptedCustomerRegistered::class, $replayed[0]);
        $this->assertTrue(RedactedValue::isRedacted($replayed[0]->email));
        $this->assertSame('ORD-42', $replayed[0]->orderReference);
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

final class MongoEncryptedCustomerRegistered extends SourceEvent
{
    public function __construct(
        ?EntityIdentifierInterface $aggregateId,
        ?EntityIdentifierInterface $eventId,
        #[DataSubjectId]
        public string $customerId = '',
        #[PersonalData]
        public string $email = '',
        public string $orderReference = '',
        ?DateTimeImmutable $occurredOn = null,
        ?EventVersion $version = null
    ) {
        parent::__construct($aggregateId, $eventId, $occurredOn, $version);
    }

    public function toArray(): array
    {
        return parent::toArray() + [
            'customerId' => $this->customerId,
            'email' => $this->email,
            'orderReference' => $this->orderReference,
        ];
    }
}
