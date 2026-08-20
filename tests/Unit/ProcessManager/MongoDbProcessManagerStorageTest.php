<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Unit\ProcessManager;

use DateTimeImmutable;
use DateTimeZone;
use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Interface\ProcessManagerStorageInterface;
use DomainFlow\EventSourcing\ProcessManager\ProcessManagerState;
use DomainFlow\EventSourcing\ProcessManager\ProcessManagerStateEnum;
use DomainFlow\EventSourcingCore\Provider\Unit\AbstractProcessManagerStorageTestCase;
use DomainFlow\EventSourcingMongoDB\ProcessManager\MongoDbProcessManagerStorage;
use DomainFlow\EventSourcingMongoDB\Tests\Setup\MongoDbHelper;
use MongoDB\Driver\Exception\BulkWriteException;
use PHPUnit\Framework\Attributes\CoversClass;
use RuntimeException;

#[CoversClass(MongoDbProcessManagerStorage::class)]
final class MongoDbProcessManagerStorageTest extends AbstractProcessManagerStorageTestCase
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

    protected function getProcessManagerStorage(): ProcessManagerStorageInterface
    {
        return new MongoDbProcessManagerStorage($this->getMongoDatabase());
    }

    public function test_storeAndRetrieveRoundtripsTimeout(): void
    {
        $storage = $this->getProcessManagerStorage();
        $processId = EntityIdentifier::fromString('process-with-timeout');

        $state = new ProcessManagerState($processId);
        $state->setTimeout(new DateTimeImmutable('2026-01-01 00:00:00'));

        $storage->store($state);
        $retrieved = $storage->retrieve($processId);

        $this->assertNotNull($retrieved);
        $this->assertNotNull($retrieved->getTimeout());
        $this->assertSame('2026-01-01 00:00:00', $retrieved->getTimeout()->format('Y-m-d H:i:s'));
    }

    public function test_retrieveFallsBackToWaitingWhenStatusFieldIsMissing(): void
    {
        $this->getMongoDatabase()->selectCollection(MongoDbProcessManagerStorage::DEFAULT_COLLECTION_NAME)->insertOne([
            'process_id' => 'missing-status-process',
            'data' => [],
        ]);

        $retrieved = $this->getProcessManagerStorage()->retrieve(EntityIdentifier::fromString('missing-status-process'));

        $this->assertNotNull($retrieved);
        $this->assertSame(ProcessManagerStateEnum::WAITING, $retrieved->getStatus());
    }
    /**
     * A find by timeout has no requested id to fall back on, so a document that
     * does not say which process it is cannot be turned into one. Handing back
     * an empty identifier would give a timeout worker a saga it cannot store,
     * and the version check would then report a malformed document as a lost
     * race — the wrong diagnosis, on the one day it matters.
     */
    public function test_a_timed_out_document_without_a_process_id_is_reported_as_malformed(): void
    {
        $this->getMongoDatabase()->selectCollection(MongoDbProcessManagerStorage::DEFAULT_COLLECTION_NAME)->insertOne([
            'status' => ProcessManagerStateEnum::WAITING->value,
            'data' => [],
            'timeout' => '2026-01-01 12:00:00.000000',
            'version' => 1,
        ]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('process_id is missing or not a string');

        $this->getProcessManagerStorage()->findTimedOut(
            new DateTimeImmutable('2099-01-01 00:00:00.000000', new DateTimeZone('UTC')),
            10
        );
    }

    /**
     * A validation failure is not a lost race. Reporting it as one would send a
     * caller that reloads and retries on conflicts into an endless loop over a
     * document the server will never accept.
     */
    public function test_aWriteFailureThatIsNotAConflictKeepsItsOwnIdentity(): void
    {
        $this->getMongoDatabase()->createCollection('rejecting_states', [
            'validator' => ['$jsonSchema' => ['bsonType' => 'object', 'required' => ['a_field_no_state_has']]],
        ]);

        $storage = new MongoDbProcessManagerStorage($this->getMongoDatabase(), 'rejecting_states');

        $this->expectException(BulkWriteException::class);

        $storage->store(new ProcessManagerState(EntityIdentifier::fromString('validated-process')));
    }

}
