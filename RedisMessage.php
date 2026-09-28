<?php

namespace Voyager\Redis;

use Voyager\Contracts\Signals\NamedSignal;

/** One value popped off a list, raw bytes as Redis stored them. Dispatched as "redis:{key}". */
final readonly class RedisMessage implements NamedSignal
{
    public function __construct(
        public string $key,
        public string $raw,
    ) {}

    public function name(): string
    {
        return 'redis:'.$this->key;
    }
}
