<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Unit\Storage;

use LogicException;
use MongoDB\Collection;
use MongoDB\Database;

/**
 * Hands out RacingCollection instances, so a storage built on this database
 * reads and writes through the seam. See RacingCollection for why.
 */
final class RacingDatabase extends Database
{
    /** @var array<string, RacingCollection> */
    private array $collections = [];

    /**
     * @param array<string, mixed> $options
     */
    public function getCollection(
        string $collectionName,
        array $options = []
    ): Collection {
        $collection = new RacingCollection(
            $this->getManager(),
            $this->getDatabaseName(),
            $collectionName,
            $options
        );

        $this->collections[$collectionName] = $collection;

        return $collection;
    }

    /**
     * The collection the storage selected under this name.
     *
     * By name rather than "the last one": a storage may select several — the
     * events collection and the global position counter, for instance — and a
     * race armed on the wrong one silently never fires, which looks exactly
     * like a passing test.
     */
    public function collection(
        string $collectionName
    ): RacingCollection {
        return $this->collections[$collectionName] ?? throw new LogicException(
            sprintf('No collection named "%s" has been selected.', $collectionName)
        );
    }
}
