<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Support;

use MongoDB\Collection;
use Throwable;

/**
 * Index creation that tolerates an application user without DDL rights —
 * but only when the index is already there.
 *
 * Each write path creates its indexes lazily, so a
 * production application needed index privileges forever, for an operation
 * that is setup rather than traffic. Simply swallowing the failure would have
 * been worse than the problem: the unique indexes here are what the write
 * paths rest on for conflict detection, and carrying on without one trades a
 * loud permission error for a silently lost guarantee.
 *
 * So the failure is only tolerated when the index exists anyway — which is the
 * case `MongoDbSchemaManager` produces, and listing indexes needs no more than
 * read access.
 */
trait EnsuresMongoIndexes
{
    /**
     * @param Collection $collection
     * @param array<string, int> $key
     * @param array<string, mixed> $options
     * @return void
     */
    private function ensureIndexExists(
        Collection $collection,
        array $key,
        array $options
    ): void {
        try {
            $collection->createIndex($key, $options);
        } catch (Throwable $exception) {
            if (!$this->hasIndexOn($collection, $key)) {
                throw $exception;
            }
        }
    }

    /**
     * Matched on the key rather than the name: a name says what someone called
     * an index, the key says what it does. An index carrying the right name
     * over the wrong fields would satisfy the first check and none of the
     * promises behind it.
     *
     * @param Collection $collection
     * @param array<string, int> $key
     * @return bool
     */
    private function hasIndexOn(
        Collection $collection,
        array $key
    ): bool {
        $wanted = array_map(static fn (int $direction): string => (string) $direction, $key);

        foreach ($collection->listIndexes() as $index) {
            $existing = array_map(
                static fn (mixed $direction): string => is_scalar($direction) ? (string) $direction : '',
                $index->getKey()
            );

            if ($existing === $wanted) {
                return true;
            }
        }

        return false;
    }
}
