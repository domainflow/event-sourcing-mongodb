<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Unit\Schema;

use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Interface\SchemaManagerInterface;
use DomainFlow\EventSourcingCore\Provider\Unit\AbstractSchemaManagerTestCase;
use DomainFlow\EventSourcingCore\Provider\Unit\AnotherDummyEvent;
use DomainFlow\EventSourcingMongoDB\Schema\MongoDbSchemaManager;
use DomainFlow\EventSourcingMongoDB\Storage\MongoDbEventEntryFactory;
use DomainFlow\EventSourcingMongoDB\Storage\MongoDbEventStorage;
use DomainFlow\EventSourcingMongoDB\Tests\Setup\MongoDbHelper;
use MongoDB\Driver\Exception\CommandException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;

#[CoversClass(MongoDbSchemaManager::class)]
#[UsesClass(MongoDbEventStorage::class)]
#[UsesClass(MongoDbEventEntryFactory::class)]
final class MongoDbSchemaManagerTest extends AbstractSchemaManagerTestCase
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

    protected function getSchemaManager(): SchemaManagerInterface
    {
        return new MongoDbSchemaManager($this->getMongoDatabase());
    }

    protected function writeAnEvent(): void
    {
        (new MongoDbEventStorage($this->getMongoDatabase()))
            ->storeEvents([new AnotherDummyEvent(EntityIdentifier::fromString('schema-probe'), 1)]);
    }

    /**
     * A collection exists in MongoDB once something is in it, so "the schema"
     * here is the indexes — and the unique one is what makes a concurrent
     * append detectable at all.
     */
    protected function schemaExists(): bool
    {
        foreach ($this->getMongoDatabase()->selectCollection('events')->listIndexes() as $index) {
            if ($index->getName() === 'uq_aggregate_version') {
                return true;
            }
        }

        return false;
    }

    /**
     * Once an operator has run setup, the
     * application user does not need index privileges any more. Simulated by
     * making creation fail rather than by taking rights away — an index under
     * a different name over the same key is refused as a conflict, which is
     * the same shape of failure as being told no.
     */
    public function test_a_write_survives_index_creation_being_refused_when_the_index_is_there(): void
    {
        $events = $this->getMongoDatabase()->selectCollection('events');
        $events->createIndex(['aggregate_id' => 1, 'version' => 1], ['unique' => true, 'name' => 'someone_elses_name']);
        $events->createIndex(['global_position' => 1], ['unique' => true, 'name' => 'uq_global_position']);

        $this->writeAnEvent();

        $this->assertSame(1, $events->countDocuments());
    }

    /**
     * The other half, and the half that makes the first one safe: refused
     * *and* missing still throws. Swallowing it would trade a loud permission
     * error for a silently lost conflict check — the write would go on working
     * right up until two of them raced.
     */
    public function test_a_write_still_fails_when_the_index_is_refused_and_absent(): void
    {
        // The name is taken, over the wrong key — so creating the real one is
        // refused, and looking for the key it should cover finds nothing.
        $this->getMongoDatabase()->selectCollection('events')
            ->createIndex(['occurred_on' => 1], ['name' => 'uq_aggregate_version']);

        $this->expectException(CommandException::class);

        $this->writeAnEvent();
    }

    public function test_describe_schema_names_the_indexes_and_their_collections(): void
    {
        $description = $this->getSchemaManager()->describeSchema();

        $this->assertCount(5, $description);
        $this->assertStringContainsString('uq_aggregate_version', $description[0]);
        $this->assertStringContainsString('events', $description[0]);
        $this->assertStringContainsString('aggregate_id, version', $description[0]);
    }
}
