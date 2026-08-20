<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Integration;

use DomainFlow\EventSourcing\Interface\ProjectorInterface;
use DomainFlow\EventSourcingCore\Provider\Integration\ProjectionReplayIntegrationTestCase;
use DomainFlow\EventSourcingMongoDB\Tests\Integration\Repository\MongoDbCounterProjectionRepository;
use DomainFlow\EventSourcingMongoDB\Tests\Integration\Repository\MongoDbCounterProjector;
use DomainFlow\EventSourcingMongoDB\Tests\Setup\MongoDbSetup;
use PHPUnit\Framework\Attributes\CoversNothing;

#[CoversNothing]
final class ProjectionReplayIntegrationTest extends ProjectionReplayIntegrationTestCase
{
    use MongoDbSetup;

    protected function setupCounterProjections(): void
    {
        $this->dropMongoDatabase();
    }

    protected function getCounterProjectionRepository(): ProjectorInterface
    {
        return new MongoDbCounterProjector($this->getMongoDatabase());
    }

    protected function getProjectionCounter(string $aggregateId): ?int
    {
        return (new MongoDbCounterProjectionRepository($this->getMongoDatabase()))->getCounter($aggregateId);
    }

    protected function projectionRowExists(string $aggregateId): bool
    {
        return $this->getProjectionCounter($aggregateId) !== null;
    }

    protected function getAllProjectionRows(): array
    {
        return (new MongoDbCounterProjectionRepository($this->getMongoDatabase()))->all();
    }
}
