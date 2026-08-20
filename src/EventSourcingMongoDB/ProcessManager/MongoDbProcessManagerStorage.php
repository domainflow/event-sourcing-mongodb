<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\ProcessManager;

use DateTimeImmutable;
use DateTimeZone;
use DomainFlow\EventSourcing\Entity\EntityIdentifier;
use DomainFlow\EventSourcing\Exception\ProcessManagerConcurrencyException;
use DomainFlow\EventSourcing\Interface\EntityIdentifierInterface;
use DomainFlow\EventSourcing\Interface\ProcessManagerStorageInterface;
use DomainFlow\EventSourcing\ProcessManager\ProcessManagerState;
use DomainFlow\EventSourcing\ProcessManager\ProcessManagerStateEnum;
use DomainFlow\EventSourcingMongoDB\Support\AssertsMongoDocument;
use DomainFlow\EventSourcingMongoDB\Support\EnsuresMongoIndexes;
use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Driver\Exception\BulkWriteException;
use MongoDB\Driver\WriteConcern;
use RuntimeException;
use Throwable;

/**
 * MongoDB-backed ProcessManagerStorageInterface implementation: one document
 * per process, keyed on process_id, holding status/data/timeout.
 *
 * store() is conditional on the version the state was loaded at. An
 * unconditional upsert — which this used to be — loses a saga update whenever
 * two workers handle events for the same process at once, and the saga can then
 * sit in WAITING forever with nothing reported.
 *
 * The filter carries the expected version, so the upsert either matches the
 * document it read or matches nothing. `w: majority, j: true`, like the event
 * collections: a saga's state is no less worth keeping than the events that
 * drove it.
 */
final class MongoDbProcessManagerStorage implements ProcessManagerStorageInterface
{
    use AssertsMongoDocument;
    use EnsuresMongoIndexes;

    public const string DEFAULT_COLLECTION_NAME = 'process_manager_states';

    /** MongoDB's duplicate-key error. */
    private const int DUPLICATE_KEY_ERROR = 11000;

    private readonly Collection $collection;

    private bool $indexEnsured = false;

    public function __construct(
        Database $database,
        string $collectionName = self::DEFAULT_COLLECTION_NAME
    ) {
        $this->collection = $database->selectCollection($collectionName, [
            'typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array'],
            'writeConcern' => new WriteConcern(WriteConcern::MAJORITY, 0, true),
        ]);
    }

    /**
     * @param ProcessManagerState $state
     * @throws ProcessManagerConcurrencyException
     * @return void
     */
    public function store(
        ProcessManagerState $state
    ): void {
        $this->ensureIndex();

        $processId = (string) $state->getProcessId();
        $expected = $state->getVersion();
        $next = $expected + 1;

        // Version 0 means "must not exist yet", which as a filter is
        // process_id plus an absent version field. Anything else must match the
        // exact version that was read. Either way the upsert matches the
        // document this state came from, or nothing at all.
        $filter = $expected === 0
            ? ['process_id' => $processId, 'version' => ['$exists' => false]]
            : ['process_id' => $processId, 'version' => $expected];

        $document = [
            'process_id' => $processId,
            'status' => $state->getStatus()->value,
            'data' => $state->getData(),
            'timeout' => $state->getTimeout()?->format('Y-m-d H:i:s.u'),
            'version' => $next,
        ];

        try {
            $result = $this->collection->replaceOne($filter, $document, ['upsert' => $expected === 0]);
        } catch (BulkWriteException $exception) {
            // The upsert path only. A duplicate key on process_id means the
            // document already exists, so this "insert" is really a write
            // against a state someone else has already created — the same
            // conflict as a moved version, reported the same way.
            throw $this->conflictOr($exception, $state->getProcessId(), $expected, $processId);
        }

        if ($result->getModifiedCount() === 0 && $result->getUpsertedCount() === 0) {
            throw ProcessManagerConcurrencyException::versionMoved(
                $state->getProcessId(),
                $expected,
                $this->storedVersion($processId)
            );
        }

        $state->markPersisted($next);
    }

    /**
     * Only a duplicate key is a conflict; anything else is an infrastructure
     * failure and must keep its own identity, or a caller retrying on conflicts
     * retries an oversized document forever.
     */
    private function conflictOr(
        BulkWriteException $exception,
        EntityIdentifierInterface $processId,
        int $expected,
        string $processIdString
    ): Throwable {
        foreach ($exception->getWriteResult()->getWriteErrors() as $error) {
            if ($error->getCode() === self::DUPLICATE_KEY_ERROR) {
                return ProcessManagerConcurrencyException::versionMoved(
                    $processId,
                    $expected,
                    $this->storedVersion($processIdString)
                );
            }
        }

        return $exception;
    }

    /**
     * One document per process, enforced rather than assumed.
     *
     * Without it, an upsert whose filter matches nothing happily creates a
     * second document for the same process_id, and the two then take turns
     * being found. Created lazily so building the object needs no reachable
     * server, for the same reason as the event storage's index.
     */
    private function ensureIndex(): void
    {
        if ($this->indexEnsured) {
            return;
        }

        $this->ensureIndexExists($this->collection, ['process_id' => 1], ['unique' => true, 'name' => 'uq_process_id']);

        // The timeout worker's poll: overdue first, bounded by a limit.
        // Without it the contract's ordering is a collection scan and an
        // in-memory sort, on every pass, over every saga ever started.
        $this->ensureIndexExists($this->collection, ['timeout' => 1], ['name' => 'idx_timeout']);

        $this->indexEnsured = true;
    }

    private function storedVersion(
        string $processId
    ): int {
        $document = $this->collection->findOne(['process_id' => $processId], ['projection' => ['version' => 1]]);
        $version = is_array($document) ? ($document['version'] ?? null) : null;

        return is_numeric($version) ? (int) $version : 0;
    }

    public function retrieve(
        EntityIdentifierInterface $processId
    ): ?ProcessManagerState {
        $raw = $this->collection->findOne(['process_id' => (string) $processId]);

        if ($raw === null) {
            return null;
        }

        return $this->hydrate($raw, (string) $processId);
    }

    /**
     * Overdue processes, oldest first, still running.
     *
     * `timeout` is a string in a fixed, zero-padded, UTC format, so `$lte` and
     * an ascending sort are chronological — and a document that never set one
     * holds null, which no string comparison matches.
     *
     * `$nin` rather than a positive list, so a status added later counts as
     * still running instead of quietly dropping out of every timeout worker's
     * view.
     *
     * @param DateTimeImmutable $asOf
     * @param int $limit
     * @return list<ProcessManagerState>
     */
    public function findTimedOut(
        DateTimeImmutable $asOf,
        int $limit
    ): array {
        if ($limit <= 0) {
            // Not defensive padding: to MongoDB a `limit` of 0 means *no
            // limit*, so passing this through would answer "give me nothing"
            // with every overdue saga there is.
            return [];
        }

        $this->ensureIndex();

        $cursor = $this->collection->find(
            [
                'timeout' => ['$lte' => $asOf->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d H:i:s.u')],
                'status' => ['$nin' => [ProcessManagerStateEnum::COMPLETED->value, ProcessManagerStateEnum::FAILED->value]],
            ],
            ['sort' => ['timeout' => 1], 'limit' => $limit]
        );

        $states = [];

        foreach ($cursor as $document) {
            // Unlike retrieve(), there is no requested id to fall back on —
            // the document is the only thing that says which process this is.
            $states[] = $this->hydrate($document, null);
        }

        return $states;
    }

    /**
     * @param mixed $raw
     * @param string|null $requestedProcessId The id this document was looked up
     *        by, where there was one. A find by timeout has none, so the
     *        document has to identify itself.
     * @return ProcessManagerState
     */
    private function hydrate(
        mixed $raw,
        ?string $requestedProcessId
    ): ProcessManagerState {
        $processId = $requestedProcessId ?? 'unknown';

        $document = $this->assertDocument(
            $raw,
            sprintf('Failed to decode process manager document for process "%s": document is malformed.', $processId)
        );

        if ($requestedProcessId === null) {
            $storedProcessId = $document['process_id'] ?? null;

            if (!is_string($storedProcessId)) {
                // Returning an empty identifier instead would hand a timeout
                // worker a saga it cannot store back, and the version check
                // would report it as a lost race rather than as the malformed
                // document it is.
                throw new RuntimeException(
                    'Failed to decode process manager document: process_id is missing or not a string.'
                );
            }

            $processId = $storedProcessId;
        }

        $data = $this->assertDocument(
            $document['data'] ?? [],
            sprintf('Failed to decode process manager data for process "%s": data is not a document.', $processId)
        );

        $status = isset($document['status']) && is_string($document['status'])
            ? ProcessManagerStateEnum::from($document['status'])
            : ProcessManagerStateEnum::WAITING;

        $state = new ProcessManagerState(
            EntityIdentifier::fromString($processId),
            $status,
            is_numeric($document['version'] ?? null) ? (int) $document['version'] : 0
        );
        $state->setData($data);

        if (isset($document['timeout']) && is_string($document['timeout'])) {
            // Stated, not inferred. The stored string carries no offset, and
            // everything written here is UTC, so a runtime in another zone
            // would otherwise read it as local time and move it.
            $state->setTimeout(new DateTimeImmutable($document['timeout'], new DateTimeZone('UTC')));
        }

        return $state;
    }

    public function delete(
        EntityIdentifierInterface $processId
    ): void {
        $this->collection->deleteOne(['process_id' => (string) $processId]);
    }
}
