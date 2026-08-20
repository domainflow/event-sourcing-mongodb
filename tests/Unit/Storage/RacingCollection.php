<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Tests\Unit\Storage;

use Closure;
use MongoDB\Collection;
use MongoDB\Driver\Manager;

/**
 * A collection that lets a second writer land at one exact moment: right after
 * the fallback path has read the stored versions and before it inserts.
 *
 * That window is the whole reason compensation exists, and it is not
 * reproducible by timing. Hooking findOne() — the one read
 * assertVersionsAreFree() makes — turns the race into an ordinary test.
 */
final class RacingCollection extends Collection
{
    private ?Closure $onFirstRead = null;

    private bool $alreadyRaced = false;

    /**
     * @param array<string, mixed> $options
     */
    public function __construct(
        Manager $manager,
        string $databaseName,
        string $collectionName,
        array $options = []
    ) {
        parent::__construct($manager, $databaseName, $collectionName, $options);
    }

    public function raceOnNextRead(
        Closure $writer
    ): void {
        $this->onFirstRead = $writer;
        $this->alreadyRaced = false;
    }

    /**
     * @param array<string, mixed>|object $filter
     * @param array<string, mixed> $options
     * @return array<mixed>|object|null
     */
    public function findOne(
        array|object $filter = [],
        array $options = []
    ): array|object|null {
        $result = parent::findOne($filter, $options);

        if ($this->onFirstRead !== null && !$this->alreadyRaced) {
            $this->alreadyRaced = true;
            ($this->onFirstRead)($this);
        }

        return $result;
    }
}
