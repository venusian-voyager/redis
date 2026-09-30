<?php

namespace Voyager\Redis\Lists;

use Voyager\Contracts\IOPools\Promise;
use Voyager\Redis\Sockets\RedisPipe;
use Voyager\Redis\Sockets\RedisSocketException;

/**
 * RPUSH onto one list without blocking, through a RedisPipe. Values go as raw bytes: the
 * connection's serializer and compression options don't apply.
 */
final class ListPush
{
    public function __construct(
        private readonly RedisPipe $pipe,
        private readonly string $key,
    ) {}

    /**
     * @return Promise the list's length once Redis has taken the values
     * @throws RedisSocketException the pipe's socket could not connect
     */
    public function push(string $value, string ...$values): Promise
    {
        return $this->pipe->send(['RPUSH', $this->pipe->prefix().$this->key, $value, ...$values], "RPUSH {$this->key}");
    }
}
