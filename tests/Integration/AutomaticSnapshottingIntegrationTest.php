<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Integration;

use DomainFlow\EventSourcingCore\Provider\Integration\AutomaticSnapshottingIntegrationTestCase;
use DomainFlow\EventSourcingMongoDB\Tests\Setup\MongoDbSetup;
use PHPUnit\Framework\Attributes\CoversNothing;

#[CoversNothing]
final class AutomaticSnapshottingIntegrationTest extends AutomaticSnapshottingIntegrationTestCase
{
    use MongoDbSetup;
}
