<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Integration;

use DomainFlow\EventSourcingCore\Provider\Integration\DummyAggregateIntegrationTestCase;
use DomainFlow\EventSourcingMongoDB\Tests\Setup\MongoDbSetup;
use PHPUnit\Framework\Attributes\CoversNothing;

#[CoversNothing]
final class DummyAggregateIntegrationTest extends DummyAggregateIntegrationTestCase
{
    use MongoDbSetup;
}
