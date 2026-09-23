<?php

namespace Voyager\Redis;

use Ramsey\Uuid\Uuid;
use Voyager\Contracts\IOPools\Event;

/** A list entry that was not one of ours. The raw string, untouched. */
final class RedisMessage extends Event
{
    private readonly string $uuid;

    public function __construct(
        public readonly string $key,
        public readonly string $raw,
    ) {
        $this->uuid = Uuid::uuid4()->toString();
    }

    public function name(): string
    {
        return 'redis:'.$this->key;
    }

    public function uuid(): string
    {
        return $this->uuid;
    }

    public function toData(): array
    {
        return ['key' => $this->key, 'raw' => $this->raw];
    }
}
