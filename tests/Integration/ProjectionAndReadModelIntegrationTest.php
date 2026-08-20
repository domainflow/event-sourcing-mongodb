<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Integration;

use DomainFlow\EventSourcingCore\Provider\Integration\CounterProjectionRepositoryInterface;
use DomainFlow\EventSourcingCore\Provider\Integration\ProjectionAndReadModelIntegrationTestCase;
use DomainFlow\EventSourcingMongoDB\Tests\Integration\Repository\MongoDbCounterProjectionRepository;
use DomainFlow\EventSourcingMongoDB\Tests\Setup\MongoDbSetup;
use PHPUnit\Framework\Attributes\CoversNothing;

#[CoversNothing]
final class ProjectionAndReadModelIntegrationTest extends ProjectionAndReadModelIntegrationTestCase
{
    use MongoDbSetup;

    protected function setupCounterProjections(): void
    {
        $this->dropMongoDatabase();
    }

    protected function getCounterProjectionRepository(): CounterProjectionRepositoryInterface
    {
        return new MongoDbCounterProjectionRepository($this->getMongoDatabase());
    }

    protected function getCounterFromProjection(string $aggregateId): ?int
    {
        return $this->getCounterProjectionRepository()->getCounter($aggregateId);
    }

    protected function getProjectionCounter(string $aggregateId): ?int
    {
        return $this->getCounterProjectionRepository()->getCounter($aggregateId);
    }

    protected function projectionRowExists(string $aggregateId): bool
    {
        return $this->getCounterProjectionRepository()->getCounter($aggregateId) !== null;
    }
}
