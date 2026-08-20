<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Setup;

use MongoDB\Client;
use MongoDB\Database;

trait MongoDbHelper
{
    private static ?Client $mongoClient = null;

    public function getMongoClient(): Client
    {
        if (self::$mongoClient === null) {
            $uri = getenv('MONGO_URI') ?: 'mongodb://127.0.0.1:27017';

            self::$mongoClient = new Client($uri);
        }

        return self::$mongoClient;
    }

    public function getMongoDatabase(): Database
    {
        $dbName = getenv('MONGO_DB') ?: 'event_sourcing_test';

        return $this->getMongoClient()->selectDatabase($dbName);
    }

    protected function dropMongoDatabase(): void
    {
        $this->getMongoDatabase()->drop();
    }

    /**
     * Whether this run is pointed at a standalone mongod and has therefore
     * accepted the weaker guarantee.
     *
     * Since a deployment without transactions refuses to write until the
     * operator opts out, the standalone CI job has to say so — and it says so
     * the same way an operator would, by setting the flag rather than by the
     * adapter quietly deciding for itself. Everything else runs against the
     * replica set and never reaches this.
     */
    protected function allowNonAtomicBatches(): bool
    {
        return (getenv('MONGO_ALLOW_NON_ATOMIC_BATCHES') ?: '0') === '1';
    }
}
