<?php

declare(strict_types=1);

namespace DomainFlow\EventSourcingMongoDB\Support;

use RuntimeException;

/**
 * Shared runtime validation that a decoded BSON value is a string-keyed
 * document, used wherever a driver return type is wider (array|object) than
 * what this adapter's typeMap actually produces.
 */
trait AssertsMongoDocument
{
    /**
     * A cursor result as documents this adapter can talk about.
     *
     * A conversion rather than an assertion, unlike `assertDocument()` below:
     * the collection's typeMap already produces arrays, so a guard that threw
     * here would be a branch nothing could reach. `assertDocument()` stays for
     * the places where a wrong shape is a real possibility worth reporting —
     * a payload or a stored subdocument someone else may have written.
     *
     * @param mixed $values
     * @return list<array<string, mixed>>
     */
    private function toDocuments(
        mixed $values
    ): array {
        $documents = [];

        foreach (is_array($values) ? $values : [] as $value) {
            $documents[] = $this->toDocument($value);
        }

        return $documents;
    }

    /**
     * @param mixed $value
     * @return array<string, mixed>
     */
    private function toDocument(
        mixed $value
    ): array {
        $document = [];

        foreach (is_array($value) ? $value : [] as $key => $item) {
            $document[(string) $key] = $item;
        }

        return $document;
    }

    /**
     * @return array<string, mixed>
     */
    private function assertDocument(
        mixed $value,
        string $exceptionMessage
    ): array {
        if (!is_array($value)) {
            throw new RuntimeException($exceptionMessage);
        }

        $document = [];
        foreach ($value as $key => $item) {
            if (!is_string($key)) {
                throw new RuntimeException($exceptionMessage);
            }

            $document[$key] = $item;
        }

        return $document;
    }
}
